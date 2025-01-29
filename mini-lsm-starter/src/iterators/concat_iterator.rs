#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut iter = Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        };
        if iter.sstables.is_empty() {
            return Ok(iter);
        }

        let sst_iter =
            SsTableIterator::create_and_seek_to_first(iter.sstables.first().unwrap().clone())?;
        iter.current = Some(sst_iter);
        iter.next_sst_idx = 1;

        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut iter = Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        };
        if iter.sstables.is_empty() {
            return Ok(iter);
        };

        let index = if key < iter.sstables.first().unwrap().first_key().as_key_slice() {
            0
        } else {
            iter.sstables
                .partition_point(|x| x.first_key().as_key_slice() <= key)
                - 1
        };

        let sst_iter = SsTableIterator::create_and_seek_to_key(iter.sstables[index].clone(), key)?;
        iter.current = Some(sst_iter);
        iter.next_sst_idx = index + 1;

        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        match &self.current {
            Some(iter) => iter.key(),
            None => KeySlice::default(),
        }
    }

    fn value(&self) -> &[u8] {
        match &self.current {
            Some(iter) => iter.value(),
            None => &[],
        }
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|x| x.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        if !self.is_valid() && self.next_sst_idx < self.sstables.len() {
            let table = self.sstables[self.next_sst_idx].clone();
            let sst_iter = SsTableIterator::create_and_seek_to_first(table)?;
            self.current = Some(sst_iter);
            self.next_sst_idx += 1;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
