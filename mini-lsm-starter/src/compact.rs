#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

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
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
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
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // use all l0 and l1 sstable
                // make a two merge iterator with merge l0 sst iter and l1 concat sst iter
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for idx in l0_sstables.iter() {
                    let sst = snapshot.sstables.get(idx).unwrap().clone();
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
                }
                let l0_iters = MergeIterator::create(l0_iters);

                let mut l1_iters = Vec::with_capacity(l1_sstables.len());
                for idx in l1_sstables.iter() {
                    let sst = snapshot.sstables.get(idx).unwrap().clone();
                    l1_iters.push(sst);
                }
                let l1_iters = SstConcatIterator::create_and_seek_to_first(l1_iters)?;

                self.use_iter_build_new_ssts(TwoMergeIterator::create(l0_iters, l1_iters)?, true)
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => match upper_level {
                Some(_) => {
                    let mut upper_sst = Vec::with_capacity(upper_level_sst_ids.len());
                    for idx in upper_level_sst_ids.iter() {
                        let sst = snapshot.sstables.get(idx).unwrap().clone();
                        upper_sst.push(sst);
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_sst)?;

                    let mut lower_sst = Vec::with_capacity(lower_level_sst_ids.len());
                    for idx in lower_level_sst_ids.iter() {
                        let sst = snapshot.sstables.get(idx).unwrap().clone();
                        lower_sst.push(sst);
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_sst)?;

                    self.use_iter_build_new_ssts(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        *is_lower_level_bottom_level,
                    )
                }
                None => {
                    let mut upper_sst = Vec::with_capacity(upper_level_sst_ids.len());
                    for idx in upper_level_sst_ids.iter() {
                        let sst = snapshot.sstables.get(idx).unwrap().clone();
                        upper_sst.push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
                    }
                    let upper_iter = MergeIterator::create(upper_sst);

                    let mut lower_sst = Vec::with_capacity(lower_level_sst_ids.len());
                    for idx in lower_level_sst_ids.iter() {
                        let sst = snapshot.sstables.get(idx).unwrap().clone();
                        lower_sst.push(sst);
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_sst)?;

                    self.use_iter_build_new_ssts(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        *is_lower_level_bottom_level,
                    )
                }
            },
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut tiers_iter = Vec::with_capacity(tiers.len());
                for (_, files) in tiers.iter() {
                    let mut ssts = Vec::with_capacity(files.len());
                    for idx in files {
                        let sst = snapshot.sstables.get(idx).unwrap().clone();
                        ssts.push(sst);
                    }
                    let tier_iter = SstConcatIterator::create_and_seek_to_first(ssts)?;
                    tiers_iter.push(Box::new(tier_iter));
                }
                self.use_iter_build_new_ssts(
                    MergeIterator::create(tiers_iter),
                    *bottom_tier_included,
                )
            }
        }
    }

    fn use_iter_build_new_ssts(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        // use merge iterator, scan all key-value pair
        // refactor to sort compact sst
        // 1. sorted, by two merger iter, the next one sequence is sorted
        // 2. same key, just choose the latest one
        // 3. delete tombstone, skip it
        let mut new_sst = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);

        while iter.is_valid() {
            if compact_to_bottom_level {
                // represent different empty value process idea
                if !iter.value().is_empty() {
                    builder.add(iter.key(), iter.value());
                }
            } else {
                builder.add(iter.key(), iter.value());
            }
            iter.next()?;
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
        }

        // the last sst, need extra process to build
        if !builder.is_empty() {
            let sst_id = self.next_sst_id();
            let sst = builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?;
            new_sst.push(Arc::new(sst));
        }

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
            let _state_lock = self.state_lock.lock();
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
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };
        // generate the compaction task
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        // if no compaction schedule, return Ok
        if task.is_none() {
            return Ok(());
        }
        let task = task.unwrap();
        println!("----before compaction task-----");
        self.dump_structure();
        println!("running compaction task: {:?}", task);
        let new_ssts = self.compact(&task)?;
        let output: Vec<usize> = new_ssts.iter().map(|x| x.sst_id()).collect();
        let ssts_added = new_ssts.len();
        let sst_to_remove = {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            // add to manifest
            let mut new_sst_ids = Vec::with_capacity(new_ssts.len());
            // add new ssts
            for sst in new_ssts {
                new_sst_ids.push(sst.sst_id());
                let result = snapshot.sstables.insert(sst.sst_id(), sst);
                assert!(result.is_none());
            }

            let (mut snapshot, sst_id_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            // remove old ssts
            let mut sst_to_remove = Vec::with_capacity(sst_id_to_remove.len());
            for sst in sst_id_to_remove.iter() {
                let result = snapshot.sstables.remove(sst);
                assert!(result.is_some());
                sst_to_remove.push(result.unwrap());
            }

            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);

            self.sync_dir()?;
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&_state_lock, ManifestRecord::Compaction(task, new_sst_ids))?;

            sst_to_remove
        };
        println!(
            "compaction finished: {} files remvoed, {} files added, output={:?}",
            sst_to_remove.len(),
            ssts_added,
            output
        );
        println!("----after compaction task----");
        self.dump_structure();
        // remove disk storage file
        for sst in sst_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        self.sync_dir()?;

        Ok(())
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
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
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
