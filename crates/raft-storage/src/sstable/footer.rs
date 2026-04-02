use raft_common::error::{Error, Result};

/// Fixed-size footer at the end of an SSTable file.
///
/// Layout (40 bytes):
/// - index_offset: u64 LE — byte offset of the index block
/// - index_size: u64 LE — byte size of the index block
/// - bloom_offset: u64 LE — byte offset of the bloom filter block
/// - bloom_size: u64 LE — byte size of the bloom filter block
/// - bloom_num_hashes: u32 LE
/// - magic: u32 LE — 0x5354_424C ("STBL")
pub const FOOTER_SIZE: usize = 40;
pub const MAGIC: u32 = 0x5354_424C;

#[derive(Debug, Clone)]
pub struct Footer {
    pub index_offset: u64,
    pub index_size: u64,
    pub bloom_offset: u64,
    pub bloom_size: u64,
    pub bloom_num_hashes: u32,
}

impl Footer {
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..8].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[8..16].copy_from_slice(&self.index_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.bloom_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.bloom_size.to_le_bytes());
        buf[32..36].copy_from_slice(&self.bloom_num_hashes.to_le_bytes());
        buf[36..40].copy_from_slice(&MAGIC.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8; FOOTER_SIZE]) -> Result<Self> {
        let magic = u32::from_le_bytes([buf[36], buf[37], buf[38], buf[39]]);
        if magic != MAGIC {
            return Err(Error::Corruption(format!(
                "SSTable footer magic mismatch: {:#010x} != {:#010x}",
                magic, MAGIC
            )));
        }

        Ok(Self {
            index_offset: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            index_size: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            bloom_offset: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            bloom_size: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            bloom_num_hashes: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let footer = Footer {
            index_offset: 1024,
            index_size: 256,
            bloom_offset: 1280,
            bloom_size: 128,
            bloom_num_hashes: 7,
        };

        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();

        assert_eq!(decoded.index_offset, 1024);
        assert_eq!(decoded.index_size, 256);
        assert_eq!(decoded.bloom_offset, 1280);
        assert_eq!(decoded.bloom_size, 128);
        assert_eq!(decoded.bloom_num_hashes, 7);
    }

    #[test]
    fn bad_magic() {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[36..40].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        assert!(Footer::decode(&buf).is_err());
    }
}
