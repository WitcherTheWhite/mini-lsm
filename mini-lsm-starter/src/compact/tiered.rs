use std::collections::HashSet;

use serde::{Deserialize, Serialize};

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
    pub max_merge_width: Option<usize>,
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
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let engine_size = snapshot.levels.iter().fold(0, |acc, (_, v)| acc + v.len());
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        if ((engine_size - last_level_size) as f64 / last_level_size as f64) * 100.0
            >= self.options.max_size_amplification_percent as f64
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        let mut prev_tiers_size = 0;
        for (level, (_, tier)) in snapshot.levels.iter().enumerate() {
            if level == 0 {
                prev_tiers_size += tier.len();
                continue;
            }
            if (tier.len() as f64 / prev_tiers_size as f64) * 100.0
                > (100 + self.options.size_ratio) as f64
                && level >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[0..level].to_vec(),
                    bottom_tier_included: false,
                });
            }
            prev_tiers_size += tier.len();
        }

        Some(TieredCompactionTask {
            tiers: snapshot.levels.clone(),
            bottom_tier_included: true,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let del = task.tiers.iter().cloned().flat_map(|(_, v)| v).collect();

        let mut state = snapshot.clone();
        let mut levels = Vec::new();
        let mut del_iters: HashSet<usize> = task.tiers.iter().map(|(id, _)| *id).collect();

        for (id, iter) in state.levels {
            if del_iters.contains(&id) {
                del_iters.remove(&id);
                if del_iters.is_empty() {
                    levels.push((output[0], output.to_vec()));
                }
            } else {
                levels.push((id, iter));
            }
        }
        state.levels = levels;

        (state, del)
    }
}
