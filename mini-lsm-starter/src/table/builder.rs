use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::{KeyVec};
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

const FALSE_PR: f64 = 0.01_f64;
/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    entity_num: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            entity_num: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // when builder is full, split block
        while !self.builder.add(key, value) {
            self.process();
        }
        // add key hash info
        let h = farmhash::fingerprint32(key.key_ref());
        self.key_hashes.push(h);
        self.entity_num += 1;
    }

    fn process(&mut self) {
        // new builder
        let builder = BlockBuilder::new(self.block_size);
        let block = std::mem::replace(&mut self.builder, builder);

        // update other
        let (first_key, last_key) = block.first_and_last_key();
        let offset = self.data.len();
        let block = block.build();
        self.data.put(block.encode());
        // add block check sum
        let block_checksum = crc32fast::hash(block.encode().as_ref());
        self.data.put_u32(block_checksum);

        if self.meta.is_empty() {
            self.first_key.set_from_slice(first_key.as_key_slice());
        }
        self.last_key.set_from_slice(last_key.as_key_slice());
        self.meta.push(BlockMeta {
            offset,
            first_key,
            last_key,
        });
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // the last block in add func, we not process it!!
        // we need process the last block data in here
        self.process();
        let mut data = self.data;
        let meta = self.meta;

        // add block meta data to data storage
        let meta_offset = data.len() as u32;
        // block meta add the number of block to as a help
        data.put_u32(meta.len() as u32);
        BlockMeta::encode_block_meta(meta.as_slice(), &mut data);
        data.put_u32(meta_offset);

        // make a bloom filter
        let bits_per_key = Bloom::bloom_bits_per_key(self.entity_num, FALSE_PR);
        let bloom = Bloom::build_from_key_hashes(self.key_hashes.as_slice(), bits_per_key);
        // add bloom filter info to data storage
        let bloom_offset = data.len() as u32;
        bloom.encode(&mut data);
        data.put_u32(bloom_offset);
        let file = FileObject::create(path.as_ref(), data)?;

        Ok(SsTable {
            file,
            block_meta: meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key: self.first_key.into_key_bytes(),
            last_key: self.last_key.into_key_bytes(),
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
