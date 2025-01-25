use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

pub(crate) const KEY_PREFIX_LEN_SIZE: usize = 2;
pub(crate) const REST_KEY_LEN_SIZE: usize = 2;
pub(crate) const VALUE_LEN_SIZE: usize = 2;
pub(crate) const OFFSET_SIZE: usize = 2;
pub(crate) const EXTRA_SIZE: usize = 2;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.data.is_empty() {
            self.first_key.set_from_slice(key);
            self.offsets.push(0);
            self.data.put_u16(0);
            self.data.put_u16(key.len() as u16);
            self.data.put_slice(key.raw_ref());
            self.data.put_u16(value.len() as u16);
            self.data.put_slice(value);
            return true;
        }

        let current_size = self.data.len() + 2 * self.offsets.len() + EXTRA_SIZE;
        let key = key.raw_ref();
        let prefix_len = longest_common_prefix_len(self.first_key.raw_ref(), key);
        let rest_key_len = key.len() - prefix_len;

        let add_size = KEY_PREFIX_LEN_SIZE
            + REST_KEY_LEN_SIZE
            + rest_key_len
            + VALUE_LEN_SIZE
            + value.len()
            + OFFSET_SIZE;
        if current_size + add_size > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(prefix_len as u16);
        self.data.put_u16(rest_key_len as u16);
        self.data.put_slice(&key[prefix_len..]);
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

fn longest_common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}
