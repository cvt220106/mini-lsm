use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    // upper and lower means level height, from bottom to up
    // so l1 - l2, l1 is upper level, l2 is lower level
    // is lower level bottom level means the lower level is max level ln
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // collect every level len
        // for l0 compare with the num limit
        // for else level, compare with the ratio
        let mut level_size = Vec::with_capacity(self.options.max_levels + 1);
        level_size.push(_snapshot.l0_sstables.len());
        for (_, level) in _snapshot.levels.iter() {
            level_size.push(level.len());
        }

        // traversal to compare, find the compaction task
        // when i == 0, means l0, so to ln level, we just compare ln-1 with ln, do not extra process ln
        // l0 first need reach the num limit, then compute the ratio, if all completed, start compaction
        for i in 0..self.options.max_levels {
            if i == 0
                && _snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger
            {
                continue;
            }

            let low_level = i + 1;
            let size_ratio = level_size[low_level] as f64 / level_size[i] as f64;
            if size_ratio < self.options.size_ratio_percent as f64 / 100f64 {
                // upper is bigger than lower, start compaction
                println!(
                    "compaction trigger at level{} and level{} with size ratio: {}%",
                    i,
                    low_level,
                    size_ratio * 100f64
                );
                // levels[0] means l1, so we need make idx minus 1
                return Some(SimpleLeveledCompactionTask {
                    upper_level: if i == 0 { None } else { Some(i) },
                    upper_level_sst_ids: if i == 0 {
                        _snapshot.l0_sstables.clone()
                    } else {
                        _snapshot.levels[i - 1].1.clone()
                    },
                    lower_level: low_level,
                    lower_level_sst_ids: _snapshot.levels[low_level - 1].1.clone(),
                    is_lower_level_bottom_level: low_level == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // _output is the new compaction sst_id vec, need modify to new state
        // return Vec<usize> indicate the old sst_id vec, which sst will be removed in state
        let mut snapshot = _snapshot.clone();
        let mut sst_id_remove = Vec::new();

        // check the upper level
        if let Some(level) = _task.upper_level {
            assert_eq!(
                _task.upper_level_sst_ids,
                snapshot.levels[level - 1].1,
                "upper level mismatch"
            );
            sst_id_remove.extend(&snapshot.levels[level - 1].1);
            snapshot.levels[level - 1].1.clear();
        } else {
            // l0 sstables
            // assert_eq!(
            //     _task.upper_level_sst_ids, snapshot.l0_sstables,
            //     "l0 level mismatch"
            // );
            let mut remove_id_set: HashSet<_> = _task.upper_level_sst_ids.iter().copied().collect();
            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|x| !remove_id_set.remove(x))
                .collect();
            assert!(remove_id_set.is_empty());
            sst_id_remove.extend(&_task.upper_level_sst_ids);
        }

        // check the lower level
        assert_eq!(
            _task.lower_level_sst_ids,
            snapshot.levels[_task.lower_level - 1].1,
            "low level mismatch"
        );
        sst_id_remove.extend(&snapshot.levels[_task.lower_level - 1].1);

        // update state
        snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();

        (snapshot, sst_id_remove)
    }
}
