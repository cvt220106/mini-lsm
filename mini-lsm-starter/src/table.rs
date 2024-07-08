pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_metas: &[BlockMeta], buf: &mut Vec<u8>) {
        // compute estimated size of buf will put
        // reduce the time consume of Vec clone and move in memory extend
        let mut estimated_size = 0;
        for block_meta in block_metas.iter() {
            // the size of offset len
            estimated_size += size_of::<u32>();
            // the size of first_key_len
            estimated_size += size_of::<u16>();
            // the size of first_key
            estimated_size += block_meta.first_key.raw_len();
            // the size of last_key_len
            estimated_size += size_of::<u16>();
            // the size of last_key
            estimated_size += block_meta.last_key.raw_len();
        }

        let offset = buf.len();
        buf.reserve(estimated_size + size_of::<u32>());
        for block_meta in block_metas.iter() {
            // | offset(4b) | first_key_len(2b) | first_key | last_key_len(2b) | last_key |
            // add the offset
            buf.put_u32(block_meta.offset as u32);
            // add the length of first_key and first_key
            buf.put_u16(block_meta.first_key.key_len() as u16);
            buf.put(block_meta.first_key.key_ref());
            buf.put_u64(block_meta.first_key.ts());
            // add the length of last_key and last_key
            buf.put_u16(block_meta.last_key.key_len() as u16);
            buf.put(block_meta.last_key.key_ref());
            buf.put_u64(block_meta.last_key.ts());
        }
        // add the check sum
        let check_sum = crc32fast::hash(&buf[offset..]);
        buf.put_u32(check_sum);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Vec<BlockMeta> {
        let num = buf.get_u32() as usize;
        let mut block_metas = Vec::new();
        let check_sum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(first_key_len), buf.get_u64());
            let last_key_len = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(last_key_len), buf.get_u64());
            block_metas.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        }
        assert_eq!(check_sum, buf.get_u32());

        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        // first decode the bloom filter
        let bloom_offset = file.read(len - 4, 4)?.as_slice().get_u32() as u64;
        let bloom = if bloom_offset < len - 4 {
            Some(Bloom::decode(
                file.read(bloom_offset, len - 4 - bloom_offset)?.as_slice(),
            )?)
        } else {
            None
        };

        let meta_offset = file.read(bloom_offset - 4, 4)?.as_slice().get_u32() as u64;
        let block_meta = BlockMeta::decode_block_meta(
            file.read(meta_offset, bloom_offset - 4 - meta_offset)?
                .as_slice(),
        );

        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta.get(block_idx).unwrap().offset;
        let len = if block_idx == self.num_of_blocks() - 1 {
            self.block_meta_offset - offset
        } else {
            self.block_meta.get(block_idx + 1).unwrap().offset - offset
        };

        let data = self.file.read(offset as u64, len as u64)?;
        let check_sum = (&data[data.len() - size_of::<u32>()..]).get_u32();
        assert_eq!(check_sum, crc32fast::hash(&data[..data.len() - size_of::<u32>()]));

        Ok(Arc::new(Block::decode(&data[..data.len() - size_of::<u32>()])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
