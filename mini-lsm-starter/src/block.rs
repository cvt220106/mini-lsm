mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

const LEN_VAR_SIZE: usize = 2;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let num_of_elements = self.offsets.len() as u16;
        for offset in self.offsets.iter() {
            buf.put_u16(*offset);
        }
        buf.put_u16(num_of_elements);

        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // get the number of elements
        let num_of_elements = (&data[data.len() - LEN_VAR_SIZE..]).get_u16() as usize;
        let data_end = data.len() - LEN_VAR_SIZE - num_of_elements * LEN_VAR_SIZE;
        // retrieve offset vec
        let offset_raw = &data[data_end..data.len() - LEN_VAR_SIZE];
        let offsets: Vec<u16> = offset_raw
            .chunks(LEN_VAR_SIZE)
            .map(|mut x| x.get_u16())
            .collect();
        // retrieve data vec
        let data = data[..data_end].to_vec();

        Self { data, offsets }
    }
}
