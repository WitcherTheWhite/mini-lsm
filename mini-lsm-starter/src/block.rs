mod builder;
mod iterator;

pub use builder::BlockBuilder;
use builder::{EXTRA_SIZE, OFFSET_SIZE};
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.data);
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);

        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let key_num = (&data[data.len() - EXTRA_SIZE..]).get_u16() as usize;
        let data_end = data.len() - EXTRA_SIZE - OFFSET_SIZE * key_num;
        let offset_raw = &data[data_end..data.len() - EXTRA_SIZE];
        let offsets = offset_raw
            .chunks(OFFSET_SIZE)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[..data_end].to_vec();

        Self { data, offsets }
    }
}
