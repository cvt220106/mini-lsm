use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Self::create_with_none(sstables);
        }
        let current_sst = sstables[0].clone();
        let current = SsTableIterator::create_and_seek_to_first(current_sst)?;

        Ok(Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Self::create_with_none(sstables);
        }
        // use binary search, quick find sstable contain this key
        let (sst_idx, current_sst) = match sstables.binary_search_by(|sst| {
            if key < sst.first_key().as_key_slice() {
                Ordering::Greater
            } else if key > sst.last_key().as_key_slice() {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            Ok(index) => (index, Some(sstables[index].clone())),
            Err(index) => {
                if index == 0 {
                    (index, Some(sstables[index].clone()))
                } else {
                    (0, None)
                }
            }
        };

        if current_sst.is_none() {
            return Self::create_with_none(sstables);
        }

        let current = SsTableIterator::create_and_seek_to_key(current_sst.unwrap(), key)?;

        Ok(Self {
            current: Some(current),
            next_sst_idx: sst_idx + 1,
            sstables,
        })
    }

    fn create_with_none(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Ok(Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        if !self.current.as_ref().unwrap().is_valid() {
            // move to next sst
            // judge the next_sst_idx validation
            if self.next_sst_idx == self.sstables.len() {
                // if it's not in range, make the current as None, return
                self.current = None;
                return Ok(());
            }
            let next_sst = self.sstables[self.next_sst_idx].clone();
            self.current = Some(SsTableIterator::create_and_seek_to_first(next_sst)?);
            self.next_sst_idx += 1;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
