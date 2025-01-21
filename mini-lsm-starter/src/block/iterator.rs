use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.data.is_empty() {
            return;
        }

        self.idx = 0;
        let entry_start = 0;
        let key_size = (&self.block.data[entry_start..entry_start + 2]).get_u16() as usize;
        let key_start = entry_start + 2;
        let key = &self.block.data[key_start..key_start + key_size];
        let value_size =
            (&self.block.data[key_start + key_size..key_start + key_size + 2]).get_u16() as usize;
        let value_start = key_start + key_size + 2;
        let value_end = value_start + value_size;

        self.key = KeyVec::from_vec(key.to_vec());
        self.value_range = (value_start, value_end);
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.key.clear();
            return;
        }

        self.idx += 1;
        let entry_start = self.block.offsets[self.idx] as usize;
        let key_size = (&self.block.data[entry_start..entry_start + 2]).get_u16() as usize;
        let key_start = entry_start + 2;
        let key = &self.block.data[key_start..key_start + key_size];
        let value_size =
            (&self.block.data[key_start + key_size..key_start + key_size + 2]).get_u16() as usize;
        let value_start = key_start + key_size + 2;
        let value_end = value_start + value_size;

        self.key = KeyVec::from_vec(key.to_vec());
        self.value_range = (value_start, value_end);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        loop {
            if !self.is_valid() || self.key.as_key_slice() >= key {
                break;
            }
            self.next();
        }
    }
}
