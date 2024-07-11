#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::lsm_storage::WriteBatchRecord;
use crate::mem_table::map_bound;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set and write keys and scan range set
    pub(crate) key_hashes: Option<
        Mutex<(
            HashSet<u32>,
            HashSet<u32>,
            HashSet<Bytes>,
            HashSet<(Bound<Bytes>, Bound<Bytes>)>,
        )>,
    >,
}

impl Transaction {
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("can't operate for commited txn");
        }
        // in serializable, add the key to read set
        if let Some(guard) = self.key_hashes.as_ref() {
            let mut guard = guard.lock();
            let (_, read_set, _, _) = &mut *guard;
            let key_hash = farmhash::hash32(key);
            read_set.insert(key_hash);
        }

        let value = self
            .local_storage
            .get(key)
            .map(|entry| entry.value().clone());
        if let Some(value) = value {
            return if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            };
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("can't operate for commited txn");
        }
        if let Some(guard) = self.key_hashes.as_ref() {
            let mut guard = guard.lock();
            let (_, _, _, scan_range_set) = &mut *guard;
            scan_range_set.insert((map_bound(lower), map_bound(upper)));
        }

        let mut local_iter = TxnLocalIteratorBuilder {
            map: Arc::clone(&self.local_storage),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        local_iter.next().unwrap();
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("can't operate for commited txn");
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        if let Some(guard) = self.key_hashes.as_ref() {
            let mut guard = guard.lock();
            let (write_set, _, write_keys, _) = &mut *guard;
            let key_hash = farmhash::hash32(key);
            write_set.insert(key_hash);
            write_keys.insert(Bytes::copy_from_slice(key));
        }
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("can't operate for commited txn");
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        if let Some(guard) = self.key_hashes.as_ref() {
            let mut guard = guard.lock();
            let (write_set, _, write_keys, _) = &mut *guard;
            let key_hash = farmhash::hash32(key);
            write_set.insert(key_hash);
            write_keys.insert(Bytes::copy_from_slice(key));
        }
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("can't operate for commited txn");

        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let serialized_check;
        // serializable validation for get and scan
        if let Some(guard) = self.key_hashes.as_ref() {
            let mut guard = guard.lock();
            let (write_set, read_set, _, scan_range_set) = &mut *guard;

            // get read set check and scan range set check
            if !write_set.is_empty() {
                // non-readonly txn, need to check validation
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_, write_data) in committed_txns.range((self.read_ts + 1)..) {
                    for read_key in read_set.iter() {
                        if let Some(_) = write_data.key_hashes.get(read_key) {
                            bail!("serialized check get path failed")
                        }
                    }
                    for scan_range in scan_range_set.iter() {
                        let (lower, upper) = scan_range;
                        for commited_write_key in write_data.keys.iter() {
                            if Self::check_key_with_scan_range(lower, upper, commited_write_key) {
                                bail!("serialized check scan path failed")
                            }
                        }
                    }
                }
            }

            serialized_check = true;
        } else {
            serialized_check = false;
        }

        let commit_write_batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        let ts = self.inner.write_batch_inner(&commit_write_batch)?;
        // if serializable check, write this txn write set to mvcc struct
        if serialized_check {
            let mut commited_txns = self.inner.mvcc().committed_txns.lock();
            let mut key_hashes = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _, write_keys, _) = &mut *key_hashes;

            let old_data = commited_txns.insert(
                ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    keys: std::mem::take(write_keys),
                    read_ts: self.read_ts,
                    commit_ts: ts,
                },
            );
            assert!(old_data.is_none());

            // garbage collection, if txn data ts smaller than watermark, can remove it
            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = commited_txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    fn check_key_with_scan_range(lower: &Bound<Bytes>, upper: &Bound<Bytes>, key: &Bytes) -> bool {
        match lower {
            Bound::Included(lower) => {
                if key < lower {
                    return false;
                }
            }
            Bound::Excluded(lower) => {
                if key <= lower {
                    return false;
                }
            }
            _ => (),
        }

        match upper {
            Bound::Included(upper) => {
                if key > upper {
                    return false;
                }
            }
            Bound::Excluded(upper) => {
                if key >= upper {
                    return false;
                }
            }
            _ => (),
        }

        true
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut ts = self.inner.mvcc().ts.lock();
        ts.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|v| (v.key().clone(), v.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}
impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_bytes()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_bytes()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_item_mut(|item| *item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { _txn: txn, iter };
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next()?;
        }

        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.move_to_non_delete()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
