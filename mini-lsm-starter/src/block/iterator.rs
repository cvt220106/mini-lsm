use bytes::Buf;
use std::cmp::Ordering;
use std::mem::size_of;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

const LEN_VAR_SIZE: usize = 2;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    fn reconstruct_key(first_key: &[u8], rest_key: &[u8], key_overlap_len: usize) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend(&first_key[..key_overlap_len]);
        key.extend(rest_key);

        key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if !self.block.data.is_empty() {
            self.idx = 0;
            let data = self.block.data.as_slice();
            let key_len = (&data[0..LEN_VAR_SIZE]).get_u16() as usize;
            let first_key_ts = (&data
                [LEN_VAR_SIZE + key_len..LEN_VAR_SIZE + key_len + size_of::<u64>()])
                .get_u64();
            self.first_key =
                KeyVec::from_vec_with_ts(data[LEN_VAR_SIZE..LEN_VAR_SIZE + key_len].to_vec(), first_key_ts);
            self.key = self.first_key.clone();
            // first key-pair struct
            // | key_len(2b) | key(key_len) | (mvcc ts(8b) | value_len(2b) | value(value_len) |
            let value_begin = LEN_VAR_SIZE + self.key.raw_len();
            let value_len = (&data[value_begin..value_begin + LEN_VAR_SIZE]).get_u16() as usize;
            self.value_range = (
                value_begin + LEN_VAR_SIZE,
                value_begin + LEN_VAR_SIZE + value_len,
            );
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) -> bool {
        // first check whether reach the end of the block
        // if true, set key to empty then call is_valid return false
        if self.idx == self.block.offsets.len() - 1 {
            self.key = KeyVec::new();

            self.is_valid()
        } else {
            self.idx += 1;
            let data = self.block.data.as_slice();
            let offset = *self.block.offsets.get(self.idx).unwrap() as usize;
            // the next key struct, use the key compaction strategy
            // compare prefix with first key
            // | key_overlap_len(2b) | rest_key_len(2b) | rest_key(rest_key_len) | (mvcc ts(8b) |
            let key_overlap_len = (&data[offset..LEN_VAR_SIZE + offset]).get_u16() as usize;
            let rest_key_len =
                (&data[offset + LEN_VAR_SIZE..LEN_VAR_SIZE * 2 + offset]).get_u16() as usize;
            let rest_key =
                &data[offset + LEN_VAR_SIZE * 2..LEN_VAR_SIZE * 2 + offset + rest_key_len];
            let key = Self::reconstruct_key(&self.first_key.key_ref(), rest_key, key_overlap_len);
            // add mvcc ts
            let ts_begin = offset + LEN_VAR_SIZE * 2 + rest_key_len;
            let ts = (&data[ts_begin..ts_begin + size_of::<u64>()]).get_u64();
            self.key.set_from_slice(KeySlice::from_slice(&key, ts));

            let value_begin = ts_begin + size_of::<u64>();
            let value_len = (&data[value_begin..value_begin + LEN_VAR_SIZE]).get_u16() as usize;
            self.value_range = (
                value_begin + LEN_VAR_SIZE,
                value_begin + LEN_VAR_SIZE + value_len,
            );

            true
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.key.cmp(&key.to_key_vec()) == Ordering::Less {
            self.next();
            if self.key.is_empty() {
                break;
            }
        }
    }
}
