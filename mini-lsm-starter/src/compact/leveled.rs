use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        // find the bound with _sst_ids
        let lower = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();

        let upper = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();

        let mut overlap_ssts = Vec::new();
        for idx in _snapshot.levels[_in_level - 1].1.iter() {
            let sst = _snapshot.sstables[idx].clone();
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(first_key > &upper || last_key < &lower) {
                overlap_ssts.push(*idx);
            }
        }

        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // pre compaction: compute the real level size and target size
        let mut target_leveled_size: Vec<_> = (0..self.options.max_levels).map(|_| 0).collect();
        let mut real_leveled_size = Vec::with_capacity(self.options.max_levels);
        for i in 0..self.options.max_levels {
            real_leveled_size.push(
                _snapshot.levels[i]
                    .1
                    .iter()
                    .map(|table| _snapshot.sstables[table].table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        target_leveled_size[self.options.max_levels - 1] =
            real_leveled_size[self.options.max_levels - 1].max(base_level_size_bytes);
        let mut base_level = self.options.max_levels;
        for i in (0..self.options.max_levels - 1).rev() {
            let next_level_size = target_leveled_size[i + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                // only bottom level size exceeds the base level size, other level will set target size
                target_leveled_size[i] = this_level_size;
            }
            if target_leveled_size[i] > 0 {
                // because idx from 0 to max_level - 1
                // actually level from 1 to max_level
                base_level = i + 1;
            }
        }

        // 1. check the L0 level, flush it to base level we found
        // flush all l0 sst to the base level
        let l0_num = _snapshot.l0_sstables.len();
        if l0_num >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        // 2.compute ratio, decide level priorities
        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for i in 0..self.options.max_levels {
            let ratio = real_leveled_size[i] as f64 / target_leveled_size[i] as f64;
            if ratio > 1.0 {
                priorities.push((ratio, i + 1));
            }
        }
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let priority = priorities.first();
        if let Some((_, level)) = priority {
            // compaction level to level + 1
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_leveled_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_leveled_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );

            // select the oldest one sst, compact it with next level overlapping sst
            let level = *level;
            let oldest_id = _snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            println!(
                "compaction triggered by priority: {level} out of {:?}, select {oldest_id} for compaction",
                priorities
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![oldest_id],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(_snapshot, &[oldest_id], level + 1),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut sst_id_remove = Vec::new();
        let mut upper_remove_id_set: HashSet<_> =
            _task.upper_level_sst_ids.iter().copied().collect();
        let mut lowe_remove_id_set: HashSet<_> =
            _task.lower_level_sst_ids.iter().copied().collect();
        if let Some(upper_level) = _task.upper_level {
            let new_upper_level_sst_ids: Vec<_> = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_remove_id_set.remove(x) {
                        None
                    } else {
                        Some(*x)
                    }
                })
                .collect();

            assert!(upper_remove_id_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_sst_ids;
        } else {
            // l0 sstables to level
            let new_l0_sst_ids: Vec<_> = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_remove_id_set.remove(x) {
                        None
                    } else {
                        Some(*x)
                    }
                })
                .collect();

            assert!(upper_remove_id_set.is_empty());
            snapshot.l0_sstables = new_l0_sst_ids;
        }

        sst_id_remove.extend(&_task.upper_level_sst_ids);
        sst_id_remove.extend(&_task.lower_level_sst_ids);

        let mut new_lower_level_sst_ids: Vec<_> = snapshot.levels[_task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if lowe_remove_id_set.remove(x) {
                    None
                } else {
                    Some(*x)
                }
            })
            .collect();
        assert!(lowe_remove_id_set.is_empty());
        new_lower_level_sst_ids.extend(_output);
        if !_in_recovery {
            new_lower_level_sst_ids.sort_by(|a, b| {
                _snapshot
                    .sstables
                    .get(a)
                    .unwrap()
                    .first_key()
                    .cmp(_snapshot.sstables.get(b).unwrap().first_key())
            });
        }

        // update state
        snapshot.levels[_task.lower_level - 1].1 = new_lower_level_sst_ids;

        (snapshot, sst_id_remove)
    }
}
