use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
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

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to read manifest")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_index = buf.as_slice();

        while buf_index.has_remaining() {
            let key_len = buf_index.get_u16() as usize;
            let key = buf_index.copy_to_bytes(key_len);
            let value_len = buf_index.get_u16() as usize;
            let value = buf_index.copy_to_bytes(value_len);
            _skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut data: Vec<u8> = Vec::new();
        let key_len = _key.len() as u16;
        let value_len = _value.len() as u16;
        data.put_u16(key_len);
        data.extend(_key);
        data.put_u16(value_len);
        data.extend(_value);

        let mut file = self.file.lock();
        file.get_mut().write(data.as_slice())?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;

        Ok(())
    }
}
