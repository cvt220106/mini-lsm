use crate::key::{KeyBytes, KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

const LEN_VAR_SIZE: usize = 2;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// the last key in the block
    last_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        let add_len = key_len + value_len + LEN_VAR_SIZE as u16 * 3;
        if self.is_empty() {
            // init the first key
            self.first_key = key.to_key_vec();
        }
        if add_len as usize + self.current_size() > self.block_size && !self.is_empty() {
            return false;
        }

        let offset = self.data.len() as u16;
        self.offsets.push(offset);
        // key_len
        self.data.put_u16(key_len);
        // key
        self.data.put(key.raw_ref());
        // value_len
        self.data.put_u16(value_len);
        // value
        self.data.put(value);

        // update the last key
        self.last_key = key.to_key_vec();

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn first_and_last_key(&self) -> (KeyBytes, KeyBytes) {
        (
            self.first_key.clone().into_key_bytes(),
            self.last_key.clone().into_key_bytes(),
        )
    }

    fn current_size(&self) -> usize {
        self.data.len() + self.offsets.len() * LEN_VAR_SIZE + LEN_VAR_SIZE
    }
}
