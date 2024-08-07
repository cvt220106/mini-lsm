use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize), // the flushed sst id
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>), // task and output sst ids
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(_path)
            .context("failed to create manifest")?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to read manifest")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_index = buf.as_slice();

        let mut records = Vec::new();
        while buf_index.has_remaining() {
            let len = buf_index.get_u64() as usize;
            let slice = &buf_index[..len];
            buf_index.advance(len);
            let checksum = buf_index.get_u32();
            assert_eq!(checksum, crc32fast::hash(slice));

            let record = serde_json::from_slice::<ManifestRecord>(slice)?;
            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        let data = serde_json::to_vec(&_record)?;
        let len = data.len() as u64;
        let check_sum = crc32fast::hash(data.as_slice());
        buf.put_u64(len);
        buf.extend(data);
        buf.put_u32(check_sum);

        file.write_all(&buf)?;
        file.sync_all()?;

        Ok(())
    }
}
