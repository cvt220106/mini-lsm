#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        // acquire this time snapshot to process
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };

        let mut new_sst = Vec::new();
        // use all l0 and l1 sstable, make a merge iterator
        let mut iter = match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut iters = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                for idx in l0_sstables.iter().chain(l1_sstables.iter()) {
                    let sst = snapshot.sstables[idx].clone();
                    iters.push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
                }
                MergeIterator::create(iters)
            }
            _ => unimplemented!(),
        };

        // use merge iterator, scan all key-value pair
        // refactor to sort compact sst
        // 1. sorted, by merger iter, the next one sequence is sorted
        // 2. same key, just choose the latest one
        // 3. delete tombstone, skip it
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut previous_key = Vec::new();

        while iter.is_valid() {
            if iter.value().is_empty()
                || (!previous_key.is_empty() && iter.key().raw_ref() == previous_key.as_slice())
            {
                iter.next()?;
                continue;
            }
            builder.add(iter.key(), iter.value());
            previous_key = iter.key().raw_ref().to_vec();
            if builder.estimated_size() >= self.options.target_sst_size {
                // attach the size limit, split the sstable file
                let new_builder = SsTableBuilder::new(self.options.block_size);
                let old_builder = std::mem::replace(&mut builder, new_builder);

                let sst_id = self.next_sst_id();
                let sst = old_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?;
                new_sst.push(Arc::new(sst));
            }
            iter.next()?;
        }

        // the last sst, need extra process to build
        let sst_id = self.next_sst_id();
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        new_sst.push(Arc::new(sst));

        Ok(new_sst)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let l0_remove = l0_sstables.clone();
        let l1_remove = l1_sstables.clone();
        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        })?;
        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();
            // remove the old sst in sstables hashmap
            for sst in l0_remove.iter().chain(l1_remove.iter()) {
                let result = snapshot.sstables.remove(sst);
                assert!(result.is_some());
            }
            // add new sst to sstables hashmap
            let mut ids = Vec::with_capacity(new_ssts.len());
            for new_sst in new_ssts {
                ids.push(new_sst.sst_id());
                snapshot.sstables.insert(new_sst.sst_id(), new_sst);
            }
            assert_eq!(l1_remove, snapshot.levels[0].1);
            // update the level struct
            snapshot.levels[0].1 = ids;
            // remove the compacted l0 sstable id in l0_sstables struct
            let mut remove_id_set: HashSet<_> = l0_remove.iter().copied().collect();
            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .filter(|x| !remove_id_set.remove(x))
                .copied()
                .collect();
            // check whether all ids are removed
            assert!(remove_id_set.is_empty());
            *state = Arc::new(snapshot);
        }
        // remove disk storage file
        for sst in l0_remove.iter().chain(l1_remove.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if {
            let state = self.state.read();
            state.imm_memtables.len() + 1 >= self.options.num_memtable_limit
        } {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
