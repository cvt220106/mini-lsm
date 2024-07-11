use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(_path)
            .context("failed to create manifest")?;

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to read wal file")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();

        while buf.has_remaining() {
            // get the batch size
            let batch_size = buf.get_u32() as usize;
            if buf.remaining() < batch_size {
                bail!("incomplete WAL");
            }
            let mut kv_batch = Vec::new();
            let mut hasher = crc32fast::Hasher::new();
            // get the batch body
            let mut batch_buf = &buf[..batch_size];
            // compute the batch checksum
            let check_sum = crc32fast::hash(batch_buf);

            while batch_buf.has_remaining() {
                let key_len = batch_buf.get_u16();
                // can use write_u16, it's little-endian bytes,
                // but original put big-endian bytes, so transform to big-endian by hand
                hasher.write(&key_len.to_be_bytes());
                let key = batch_buf.copy_to_bytes(key_len as usize);
                hasher.write(key.as_ref());
                // read ts
                let ts = batch_buf.get_u64();
                hasher.write(&ts.to_be_bytes());
                let value_len = batch_buf.get_u16();
                hasher.write(&value_len.to_be_bytes());
                let value = batch_buf.copy_to_bytes(value_len as usize);
                hasher.write(value.as_ref());
                kv_batch.push((KeyBytes::from_bytes_with_ts(key, ts), value));
            }
            buf.advance(batch_size);
            let store_check_sum = buf.get_u32();
            assert_eq!(check_sum, store_check_sum);
            assert_eq!(check_sum, hasher.finalize());

            for (key, value) in kv_batch {
                _skiplist.insert(key, value);
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut data: Vec<u8> = Vec::new();
        for (key, value) in _data {
            let key_len = key.key_len() as u16;
            let value_len = value.len() as u16;
            // when use put, get big-endian bytes, so when decoded, also need big-endian
            data.put_u16(key_len);
            data.extend(key.key_ref());
            // write ts
            data.put_u64(key.ts());
            data.put_u16(value_len);
            data.extend(*value);
        }
        // write batch size (body data len)
        file.write_all(&(data.len() as u32).to_be_bytes())?;
        // write body data
        file.write_all(&data)?;
        // write checksum
        file.write_all(&crc32fast::hash(&data).to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;

        Ok(())
    }
}
