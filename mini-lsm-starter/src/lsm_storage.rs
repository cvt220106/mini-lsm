#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use farmhash::fingerprint32;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // Signal the flush thread to stop
        self.flush_notifier
            .send(())
            .map_err(|e| anyhow!(format!("Failed to send shutdown signal: {}", e)))?;

        // Wait for the flush thread to finish
        // Safely get the lock and take the JoinHandle
        let join_handle = self.flush_thread.lock().take();
        if let Some(handle) = join_handle {
            match handle.join() {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!(format!("Failed to join flush thread: {:?}", e))),
            }?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // use clone make an unchanged snapshot to get, minimum time to hold the lock
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        // search by memtable
        if let Some(value) = snapshot.memtable.get(_key) {
            return if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            };
        }

        // search by all freeze imm_memtable
        for mem_table in snapshot.imm_memtables.iter() {
            if let Some(value) = mem_table.get(_key) {
                return if value.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(value))
                };
            }
        }

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(key, table.first_key().raw_ref(), table.last_key().raw_ref()) {
                if let Some(bloom) = table.bloom.as_ref() {
                    if bloom.may_contain(fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };

        // search by l0 sst
        // make sst search as a merge iter search
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for idx in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables[idx].clone();
            if keep_table(_key, &sst) {
                let iter =
                    SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(_key))?;
                l0_iters.push(Box::new(iter));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        // search by l1-ln sst
        // use concat iter
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level) in snapshot.levels.iter() {
            let mut level_sstables = Vec::with_capacity(level.len());
            for idx in level.iter() {
                let sst = snapshot.sstables[idx].clone();
                if keep_table(_key, &sst) {
                    level_sstables.push(sst);
                }
            }
            let level_concat_iter = SstConcatIterator::create_and_seek_to_key(
                level_sstables,
                KeySlice::from_slice(_key),
            )?;
            level_iters.push(Box::new(level_concat_iter));
        }

        let iter = TwoMergeIterator::create(l0_iter, MergeIterator::create(level_iters))?;
        if iter.is_valid() && iter.key().raw_ref() == _key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let state = self.state.read();
        state.memtable.put(_key, _value)?;
        let size = state.memtable.approximate_size();
        drop(state);
        self.try_freeze(size)?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let state = self.state.read();
        state.memtable.put(_key, &[])?;
        let size = state.memtable.approximate_size();
        drop(state);
        self.try_freeze(size)?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    fn try_freeze(&self, mem_table_size: usize) -> Result<()> {
        if mem_table_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let state = self.state.read();
            if state.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let mem_table_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                mem_table_id,
                self.path_of_wal(mem_table_id),
            )?)
        } else {
            Arc::new(MemTable::create(mem_table_id))
        };

        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            let old_mem_table = std::mem::replace(&mut snapshot.memtable, memtable);
            snapshot.imm_memtables.insert(0, old_mem_table.clone());
            *state = Arc::new(snapshot);
            Ok(())
        }
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let memtable_to_flush;
        {
            let state = self.state.read();
            memtable_to_flush = state
                .imm_memtables
                .last()
                .expect("no more imm_memtable")
                .clone();
        };

        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;
        let sst_id = memtable_to_flush.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            let mem = snapshot.imm_memtables.pop().unwrap();
            // add some check logic
            assert_eq!(mem.id(), sst_id);
            // use self.compaction_controller.flush_to_l0() to know whether to flush to L0
            if self.compaction_controller.flush_to_l0() {
                // flush the l0_sstables
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // in tiered compaction, directly flush to level
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            // update lsm storage state
            *state = Arc::new(snapshot);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // use clone make an unchanged snapshot to get, minimum time to hold the lock
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        let mut mmt_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mmt_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        for iter in snapshot.imm_memtables.iter() {
            mmt_iters.push(Box::new(iter.scan(_lower, _upper)));
        }
        let mmt_iters = MergeIterator::create(mmt_iters);

        let mut sst_l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for idx in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables[idx].clone();
            if range_overlap(
                _lower,
                _upper,
                sst.first_key().raw_ref(),
                sst.last_key().raw_ref(),
            ) {
                let iter = match _lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sst,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
                };
                sst_l0_iters.push(Box::new(iter));
            }
        }
        let sst_l0_iters = MergeIterator::create(sst_l0_iters);
        let two_merge_iters = TwoMergeIterator::create(mmt_iters, sst_l0_iters)?;

        // create l1-ln sstables merge concat iter
        let mut level_iters = Vec::new();
        for (_, level) in snapshot.levels.iter() {
            let mut level_sstables = Vec::with_capacity(level.len());
            for idx in level.iter() {
                let sst = snapshot.sstables[idx].clone();
                if range_overlap(
                    _lower,
                    _upper,
                    sst.first_key().raw_ref(),
                    sst.last_key().raw_ref(),
                ) {
                    level_sstables.push(sst);
                }
            }
            let level_iter = match _lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_sstables,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_sstables,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_sstables)?,
            };

            level_iters.push(Box::new(level_iter));
        }
        let level_iters = MergeIterator::create(level_iters);

        let iters = TwoMergeIterator::create(two_merge_iters, level_iters)?;
        let iters = FusedIterator::new(LsmIterator::new(iters, map_bound(_upper))?);
        Ok(iters)
    }
}

fn key_within(key: &[u8], table_first: &[u8], table_last: &[u8]) -> bool {
    table_first <= key && key <= table_last
}

fn range_overlap(
    user_lower: Bound<&[u8]>,
    user_upper: Bound<&[u8]>,
    table_first: &[u8],
    table_last: &[u8],
) -> bool {
    // judge the table range included user range or not
    // the intersection is an empty set
    match user_lower {
        Bound::Included(key) => {
            if key > table_last {
                return false;
            }
        }
        Bound::Excluded(key) => {
            if key >= table_last {
                return false;
            }
        }
        _ => {}
    }

    match user_upper {
        Bound::Included(key) => {
            if key < table_first {
                return false;
            }
        }
        Bound::Excluded(key) => {
            if key <= table_first {
                return false;
            }
        }
        _ => {}
    }

    true
}
