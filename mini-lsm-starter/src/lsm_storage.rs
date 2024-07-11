#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
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
use crate::key::{KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, map_key_bound_plus_ts, MemTable};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

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
        self.inner.sync_dir()?;
        // Signal the flush thread to stop
        self.flush_notifier
            .send(())
            .map_err(|e| anyhow!(format!("Failed to send shutdown signal: {}", e)))?;

        // Wait for the flush thread to finish
        // Safely get the lock and take the JoinHandle
        let flush = self.flush_thread.lock().take();
        if let Some(flush) = flush {
            match flush.join() {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!(format!("Failed to join flush thread: {:?}", e))),
            }?;
        }

        // signal the compaction thread to stop
        self.compaction_notifier
            .send(())
            .map_err(|e| anyhow!(format!("Failed to send shutdown signal: {}", e)))?;

        // Wait for the compaction thread to finish
        // Safely get the lock and take the JoinHandle
        let compaction = self.compaction_thread.lock().take();
        if let Some(compaction) = compaction {
            match compaction.join() {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!(format!(
                    "Failed to join compaction thread: {:?}",
                    e
                ))),
            }?;
        }

        // if don't have wal, flush all memtable to disk
        if self.inner.options.enable_wal {
            self.inner.sync()?;
        } else {
            // 1. freeze current memtable to immtable
            // 2. flush all imm_memtable to disk
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .freeze_memtable_to_immmemtables(Arc::new(MemTable::create(
                        self.inner.next_sst_id(),
                    )))?;
            }
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }
        self.inner.sync_dir()?;

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

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
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

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
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
    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let mut state = LsmStorageState::create(&options);
        let path = path.as_ref();
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1 << 10));
        let mut init_ts = TS_DEFAULT;
        let manifest;

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

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

        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            // if not exists, create it, or else recover it
            manifest = Manifest::create(manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            // recover
            let (m, records) = Manifest::recover(manifest_path)?;
            // collect all memtable id, use id get wal log to recover memtable
            // we gather the new memtable record id, remove those flush one id
            let mut memtables = BTreeSet::new();
            for record in records.iter() {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, *sst_id);
                        } else {
                            state.levels.insert(0, (*sst_id, vec![*sst_id]));
                            next_sst_id = next_sst_id.max(*sst_id);
                        }
                        memtables.remove(sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, task, output, true);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                    ManifestRecord::NewMemtable(x) => {
                        next_sst_id = next_sst_id.max(*x);
                        memtables.insert(*x);
                    }
                }
            }
            // recover sst
            let mut sst_cnt = 0;
            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().map(|(_, files)| files).flatten())
            {
                let sst = SsTable::open(
                    *sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, *sst_id))?,
                )?;
                init_ts = init_ts.max(sst.max_ts());
                state.sstables.insert(*sst_id, Arc::new(sst));
                sst_cnt += 1;
            }
            println!("{} ssts recoverd", sst_cnt);
            next_sst_id += 1;

            // in level compaction, we need extra process, sort every level
            if let CompactionController::Leveled(_) = compaction_controller {
                for (_, level) in state.levels.iter_mut() {
                    level.sort_by(|a, b| {
                        state
                            .sstables
                            .get(a)
                            .unwrap()
                            .first_key()
                            .cmp(state.sstables.get(b).unwrap().first_key())
                    })
                }
            }

            // if have wal, recover memtable by wal
            if options.enable_wal {
                let mut memtable_cnt = 0;
                for memtable_id in memtables.iter() {
                    let memtable = MemTable::recover_from_wal(
                        *memtable_id,
                        Self::path_of_wal_static(path, *memtable_id),
                    )?;
                    let max_ts = memtable
                        .map
                        .iter()
                        .map(|x| x.key().ts())
                        .max()
                        .unwrap_or_default();
                    init_ts = init_ts.max(max_ts);
                    if !memtables.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        memtable_cnt += 1;
                    }
                }
                println!("{} memtable recovered", memtable_cnt);

                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?)
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(init_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, _key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(_key)
    }

    pub(crate) fn get_with_ts(&self, _key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        // use clone make an unchanged snapshot to get, minimum time to hold the lock
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        let mut mmt_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mmt_iters.push(Box::new(snapshot.memtable.scan(
            Bound::Included(KeySlice::from_slice(_key, TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(_key, TS_RANGE_END)),
        )));
        for iter in snapshot.imm_memtables.iter() {
            mmt_iters.push(Box::new(iter.scan(
                Bound::Included(KeySlice::from_slice(_key, TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(_key, TS_RANGE_END)),
            )));
        }
        let mmt_iters = MergeIterator::create(mmt_iters);

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(key, table.first_key().key_ref(), table.last_key().key_ref()) {
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
                let iter = SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(_key, TS_RANGE_BEGIN),
                )?;
                l0_iters.push(Box::new(iter));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);
        let two_merge_iter = TwoMergeIterator::create(mmt_iters, l0_iter)?;

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
                KeySlice::from_slice(_key, TS_RANGE_BEGIN),
            )?;
            level_iters.push(Box::new(level_concat_iter));
        }

        let iter = TwoMergeIterator::create(two_merge_iter, MergeIterator::create(level_iters))?;
        let iter = LsmIterator::new(iter, Bound::Unbounded, read_ts)?;
        if iter.is_valid() && iter.key() == _key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    pub(crate) fn write_batch_inner<T: AsRef<[u8]>>(
        &self,
        _batch: &[WriteBatchRecord<T>],
    ) -> Result<u64> {
        let _lock = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        let mut batch_data = Vec::with_capacity(_batch.len());
        for record in _batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    batch_data.push((KeySlice::from_slice(key, ts), value));
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    batch_data.push((KeySlice::from_slice(key, ts), b""))
                }
            }
        }

        let size = {
            let state = self.state.read();
            state.memtable.put_batch(&batch_data)?;
            state.memtable.approximate_size()
        };
        self.try_freeze(size)?;
        self.mvcc().update_commit_ts(ts);

        Ok(ts)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        _batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(_batch)?;
        } else {
            let new_txn = self.new_txn()?;
            for record in _batch {
                match record {
                    WriteBatchRecord::Put(key, value) => {
                        new_txn.put(key.as_ref(), value.as_ref());
                    }
                    WriteBatchRecord::Del(key) => {
                        new_txn.delete(key.as_ref());
                    }
                }
            }
            new_txn.commit()?;
        }

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, _key: &[u8], _value: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(&[WriteBatchRecord::Put(_key, _value)])?;
        } else {
            let new_txn = self.new_txn()?;
            new_txn.put(_key, _value);
            new_txn.commit()?;
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, _key: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch(&[WriteBatchRecord::Del(_key)])?;
        } else {
            let new_txn = self.new_txn()?;
            new_txn.delete(_key);
            new_txn.commit()?;
        }
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
        File::open(&self.path)?.sync_all()?;

        Ok(())
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

        // before freeze, we need sync this memtable wal log
        self.sync()?;

        self.freeze_memtable_to_immmemtables(memtable)?;

        self.manifest.as_ref().unwrap().add_record(
            _state_lock_observer,
            ManifestRecord::NewMemtable(mem_table_id),
        )?;
        self.sync_dir()?;

        Ok(())
    }

    fn freeze_memtable_to_immmemtables(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut state = self.state.write();
        let mut snapshot = state.as_ref().clone();
        let old_mem_table = std::mem::replace(&mut snapshot.memtable, memtable);
        snapshot.imm_memtables.insert(0, old_mem_table.clone());
        *state = Arc::new(snapshot);
        Ok(())
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

        // when we flush memtable to disk, consistent it, now we don't need wal log
        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        // add manifest record
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;

        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        self: &Arc<Self>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(_lower, _upper)
    }

    pub(crate) fn scan_with_ts(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        // use clone make an unchanged snapshot to get, minimum time to hold the lock
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        let mut mmt_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mmt_iters.push(Box::new(snapshot.memtable.scan(
            map_key_bound_plus_ts(_lower, TS_RANGE_BEGIN),
            map_key_bound_plus_ts(_upper, TS_RANGE_END),
        )));
        for iter in snapshot.imm_memtables.iter() {
            mmt_iters.push(Box::new(iter.scan(
                map_key_bound_plus_ts(_lower, TS_RANGE_BEGIN),
                map_key_bound_plus_ts(_upper, TS_RANGE_END),
            )));
        }
        let mmt_iters = MergeIterator::create(mmt_iters);

        let mut sst_l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for idx in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables[idx].clone();
            if range_overlap(
                _lower,
                _upper,
                sst.first_key().key_ref(),
                sst.last_key().key_ref(),
            ) {
                let iter = match _lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        sst,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sst,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;
                        while iter.is_valid() && iter.key().key_ref() == key {
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
                    sst.first_key().key_ref(),
                    sst.last_key().key_ref(),
                ) {
                    level_sstables.push(sst);
                }
            }
            let level_iter = match _lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_sstables,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_sstables,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    // in mvcc, have more same key
                    while iter.is_valid() && iter.key().key_ref() == key {
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
        let iters = FusedIterator::new(LsmIterator::new(iters, map_bound(_upper), read_ts)?);
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
