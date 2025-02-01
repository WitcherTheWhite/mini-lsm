#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
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
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            Arc::clone(&state)
        };
        match task {
            CompactionTask::Leveled(_task) => unimplemented!(),
            CompactionTask::Tiered(task) => self.compact_tiers(&task.tiers, snapshot),
            CompactionTask::Simple(task) => self.compact_two_level(
                &task.upper_level_sst_ids,
                &task.lower_level_sst_ids,
                snapshot,
            ),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_two_level(l0_sstables, l1_sstables, snapshot),
        }
    }

    fn compact_tiers(
        &self,
        tiers: &Vec<(usize, Vec<usize>)>,
        snapshot: Arc<LsmStorageState>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut ssts_to_compact =
            Vec::with_capacity(tiers.iter().fold(0, |acc, (_, v)| acc + v.len()));
        for (_, tier) in tiers {
            let mut ssts = Vec::with_capacity(tier.len());
            for i in tier {
                let table = snapshot.sstables[i].clone();
                ssts.push(table);
            }
            ssts_to_compact.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?))
        }

        let mut new_ssts = Vec::new();
        let mut iter = MergeIterator::create(ssts_to_compact);
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            if iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            sst_builder.add(iter.key(), iter.value());
            if sst_builder.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let sst_file = sst_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?;
                new_ssts.push(Arc::new(sst_file));
                sst_builder = SsTableBuilder::new(self.options.block_size);
            }
            iter.next()?;
        }

        let sst_id = self.next_sst_id();
        let sst_file = sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        new_ssts.push(Arc::new(sst_file));

        Ok(new_ssts)
    }

    fn compact_two_level(
        &self,
        upper_level: &Vec<usize>,
        lower_level: &Vec<usize>,
        snapshot: Arc<LsmStorageState>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut ssts_to_compact = Vec::with_capacity(upper_level.len() + lower_level.len());
        for i in upper_level {
            let table = snapshot.sstables[i].clone();
            ssts_to_compact.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?));
        }
        for i in lower_level {
            let table = snapshot.sstables[i].clone();
            ssts_to_compact.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?));
        }

        let mut new_ssts = Vec::new();
        let mut iter = MergeIterator::create(ssts_to_compact);
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            if iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            sst_builder.add(iter.key(), iter.value());
            if sst_builder.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let sst_file = sst_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?;
                new_ssts.push(Arc::new(sst_file));
                sst_builder = SsTableBuilder::new(self.options.block_size);
            }
            iter.next()?;
        }

        let sst_id = self.next_sst_id();
        let sst_file = sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;
        new_ssts.push(Arc::new(sst_file));

        Ok(new_ssts)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;

        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();
            new_state
                .l0_sstables
                .truncate(new_state.l0_sstables.len() - l0_sstables.len());
            for sst in &new_ssts {
                new_state.sstables.insert(sst.sst_id(), sst.clone());
            }
            new_state.levels[0] = (1, new_ssts.iter().map(|x| x.sst_id()).collect());
            *state = Arc::new(new_state);
        };

        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.as_ref().clone()
        };

        if let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        {
            let new_ssts = self.compact(&task)?;
            let output: Vec<usize> = new_ssts.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
            let del_ssts = {
                let _state_lock = self.state_lock.lock();
                let mut state = self.state.write();
                let mut new_state = state.as_ref().clone();
                for sst in &new_ssts {
                    new_state.sstables.insert(sst.sst_id(), sst.clone());
                }
                let (new_state, del_ssts) = self
                    .compaction_controller
                    .apply_compaction_result(&new_state, &task, &output, true);
                *state = Arc::new(new_state);
                del_ssts
            };

            for sst in del_ssts {
                std::fs::remove_file(self.path_of_sst(sst))?;
            }
        }

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
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
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
