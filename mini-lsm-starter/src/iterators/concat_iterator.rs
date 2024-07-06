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

        let mut iter = Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        };
        iter.move_until_valid()?;

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Self::create_with_none(sstables);
        }
        // use binary search, quick find sstable contain this key
        let idx = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);

        if idx >= sstables.len() {
            return Self::create_with_none(sstables);
        }

        let current = SsTableIterator::create_and_seek_to_key(sstables[idx].clone(), key)?;

        let mut iter = Self {
            current: Some(current),
            next_sst_idx: idx + 1,
            sstables,
        };
        iter.move_until_valid()?;

        Ok(iter)
    }

    fn create_with_none(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Ok(Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        })
    }

    fn move_until_valid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                let current = SsTableIterator::create_and_seek_to_first(self.sstables[self.next_sst_idx].clone())?;
                self.current = Some(current);
                self.next_sst_idx += 1;
            }
        }

        Ok(())
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
        self.move_until_valid()?;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
