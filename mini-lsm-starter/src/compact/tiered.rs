use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "L0 level in tiered compaction need be empty!"
        );

        // only tier num larger than num_tier option, can be compact
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // 1.check the space amplification ratio
        // compute by all levels except last level size / last level size
        let mut size = 0;
        for i in 0.._snapshot.levels.len() - 1 {
            size += _snapshot.levels[i].1.len();
        }
        let space_amp_ratio =
            size as f64 / _snapshot.levels[_snapshot.levels.len() - 1].1.len() as f64 * 100_f64;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction trigger by space amplification ratio: {}",
                space_amp_ratio
            );
            // in this case, compaction all 1 - n-1 level to n level
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // 2.check the size ratio
        // in every i level, compute 1 to i -1 level size sum divide by i level size, compare with size ratio trigger
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        size = 0;
        for i in 0.._snapshot.levels.len() - 1 {
            size += _snapshot.levels[i].1.len();
            let size_ratio = size as f64 / _snapshot.levels[i + 1].1.len() as f64;
            // we will compact all i levels, if i bigger than min_merge_width at the same time
            // attention: because i begin with 0, so when i = 1, is means 2 level, so we need add 2 to compare width
            if size_ratio >= size_ratio_trigger && i + 2 >= self.options.min_merge_width {
                println!("compaction trigger by size ratio: {}", size_ratio * 100_f64);
                return {
                    Some(TieredCompactionTask {
                        tiers: _snapshot.levels.iter().take(i + 2).cloned().collect(),
                        bottom_tier_included: i + 2 >= _snapshot.levels.len(),
                    })
                };
            }
        }

        // 3.direct compact top-most level, make the tire num to num_tier
        // because when compaction, still have new memtable flush to top tier
        // so we compact this snapshot state 1 - n-num_tier+2 level as 1 level
        // after that, we will have new snapshot flush top tier, new compaction tier and old n tier
        let num_tiers_to_take = _snapshot.levels.len() - self.options.num_tiers + 2;
        println!("compaction trigger by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: _snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect(),
            bottom_tier_included: num_tiers_to_take >= _snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut snapshot = _snapshot.clone();
        let mut tier_id_remove = Vec::new();
        let mut tier_to_remove = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        // check the snapshot levels, compare with tier task generated
        // if levels in tier_to_remove, remove it, and store id
        // if not, add it to new levels,
        // and we need find the correct location for out tier(compaction one)
        let mut levels = Vec::new();
        let mut output_added = false;
        for (tier_id, files) in snapshot.levels.iter() {
            if let Some(remove_files) = tier_to_remove.remove(tier_id) {
                tier_id_remove.extend(remove_files.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()));
            }
            // find the output correct location
            if tier_to_remove.is_empty() && !output_added {
                output_added = true;
                levels.push((_output[0], _output.to_vec()));
            }
        }
        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }
        // update state to apply compaction result
        snapshot.levels = levels;

        (snapshot, tier_id_remove)
    }
}
