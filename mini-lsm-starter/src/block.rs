// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub const KEY_LEN_SIZE: usize = 2;
pub const VAL_LEN_SIZE: usize = 2;
pub const EXTRA_SIZE: usize = 2;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let total_size = self.data.len() + self.offsets.len() * 2 + EXTRA_SIZE;
        let mut buf = Vec::with_capacity(total_size);
        buf.put_slice(self.data.as_slice());
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let offset_len = (&data[(data.len() - EXTRA_SIZE)..]).get_u16() as usize;
        let offsets_start = data.len() - EXTRA_SIZE - offset_len * 2;
        let offset_raw = &data[offsets_start..data.len() - EXTRA_SIZE];
        let offsets = offset_raw
            .chunks(size_of::<u16>())
            .map(|mut v| v.get_u16())
            .collect();
        let data = data[..offsets_start].to_vec();
        Self { data, offsets }
    }
}
