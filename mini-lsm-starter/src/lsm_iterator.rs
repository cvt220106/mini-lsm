use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    read_ts: u64,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end: Bound<Bytes>, read_ts: u64) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound: end,
            read_ts,
            prev_key: Vec::new(),
        };
        iter.move_to_key()?;

        Ok(iter)
    }

    fn move_to_key(&mut self) -> Result<()> {
        loop {
            // move to next not same key
            while self.inner.is_valid() && self.inner.key().key_ref() == self.prev_key {
                self.next_inner()?;
            }
            // key is invalid, means this iter is ending
            if !self.inner.is_valid() {
                break;
            }

            // use read_ts, move to correct version snapshot key
            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().key_ref());
            while self.inner.is_valid()
                && self.inner.key().key_ref() == self.prev_key
                && self.read_ts < self.inner.key().ts()
            {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            // not have this version snapshot key, move to next key
            if self.inner.key().key_ref() != self.prev_key {
                continue;
            }
            // when value is not empty, break the loop, return this key
            if !self.inner.value().is_empty() {
                break;
            }
        }

        Ok(())
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.key() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.key() < key.as_ref(),
        }

        Ok(())
    }

    // because we add ts, same key but ts is not same, so we need process this situation
    // just return latest ts key
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_key()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("the iter is invalid, can't continue use it")
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("the iter is invalid, can't continue use it")
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is ending with error")
        }
        if self.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
