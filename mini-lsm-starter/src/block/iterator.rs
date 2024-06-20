use std::cmp::Ordering;
use std::sync::Arc;
use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

const LEN_VAR_SIZE: usize = 2;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
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
            self.first_key = KeyVec::from_vec(data[LEN_VAR_SIZE..LEN_VAR_SIZE + key_len].to_vec());
            self.key = self.first_key.clone();

            let value_begin = LEN_VAR_SIZE + key_len;
            let value_len = (&data[value_begin..value_begin + LEN_VAR_SIZE]).get_u16() as usize;
            self.value_range = (value_begin + LEN_VAR_SIZE, value_begin + LEN_VAR_SIZE+ value_len);
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
            let key_len = (&data[offset..LEN_VAR_SIZE + offset]).get_u16() as usize;
            self.key = KeyVec::from_vec(data[offset + LEN_VAR_SIZE..LEN_VAR_SIZE + offset + key_len].to_vec());

            let value_begin = offset + LEN_VAR_SIZE + key_len;
            let value_len = (&data[value_begin..value_begin + LEN_VAR_SIZE]).get_u16() as usize;
            self.value_range = (value_begin + LEN_VAR_SIZE, value_begin + LEN_VAR_SIZE + value_len);

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
