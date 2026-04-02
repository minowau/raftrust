use crc32fast::Hasher;
use raft_common::error::{Error, Result};
use std::io::{Read, Write};

/// Header size: 4 bytes CRC32 + 4 bytes length.
pub const HEADER_SIZE: usize = 8;

/// A single record in the write-ahead log.
///
/// On-disk format: `[crc32: 4 bytes LE][len: 4 bytes LE][data: len bytes]`
/// CRC32 covers both `len` (as 4 LE bytes) and `data`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub data: Vec<u8>,
}

impl WalRecord {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Compute CRC32 over the length bytes and data bytes.
    fn compute_crc(data: &[u8]) -> u32 {
        let len = data.len() as u32;
        let mut hasher = Hasher::new();
        hasher.update(&len.to_le_bytes());
        hasher.update(data);
        hasher.finalize()
    }

    /// Encode this record into its on-disk format and write to `w`.
    pub fn encode<W: Write>(&self, w: &mut W) -> Result<usize> {
        let crc = Self::compute_crc(&self.data);
        let len = self.data.len() as u32;

        w.write_all(&crc.to_le_bytes())?;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&self.data)?;

        Ok(HEADER_SIZE + self.data.len())
    }

    /// Decode one record from `r`. Returns `None` on clean EOF (no partial header).
    /// Returns `Err(Corruption)` if CRC mismatch or partial record.
    pub fn decode<R: Read>(r: &mut R) -> Result<Option<Self>> {
        // Read header
        let mut header = [0u8; HEADER_SIZE];
        match r.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;

        // Sanity check: reject absurdly large records (> 64 MB)
        if len > 64 * 1024 * 1024 {
            return Err(Error::Corruption(format!(
                "WAL record length {} exceeds maximum",
                len
            )));
        }

        // Read data
        let mut data = vec![0u8; len];
        match r.read_exact(&mut data) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::Corruption(
                    "WAL record truncated: incomplete data".to_string(),
                ));
            }
            Err(e) => return Err(e.into()),
        }

        // Verify CRC
        let computed_crc = Self::compute_crc(&data);
        if stored_crc != computed_crc {
            return Err(Error::Corruption(format!(
                "WAL CRC mismatch: stored={:#010x}, computed={:#010x}",
                stored_crc, computed_crc
            )));
        }

        Ok(Some(Self { data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn roundtrip() {
        let record = WalRecord::new(b"hello world".to_vec());
        let mut buf = Vec::new();
        record.encode(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn empty_data() {
        let record = WalRecord::new(vec![]);
        let mut buf = Vec::new();
        record.encode(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn eof_returns_none() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        assert!(WalRecord::decode(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn partial_header_returns_none() {
        let mut cursor = Cursor::new(vec![0u8; 3]);
        assert!(WalRecord::decode(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn crc_corruption_detected() {
        let record = WalRecord::new(b"data".to_vec());
        let mut buf = Vec::new();
        record.encode(&mut buf).unwrap();

        // Flip a bit in the data section
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let mut cursor = Cursor::new(&buf);
        let result = WalRecord::decode(&mut cursor);
        assert!(matches!(result, Err(Error::Corruption(_))));
    }

    #[test]
    fn truncated_data_detected() {
        let record = WalRecord::new(b"hello".to_vec());
        let mut buf = Vec::new();
        record.encode(&mut buf).unwrap();

        // Truncate the data portion
        buf.truncate(HEADER_SIZE + 2);

        let mut cursor = Cursor::new(&buf);
        let result = WalRecord::decode(&mut cursor);
        assert!(matches!(result, Err(Error::Corruption(_))));
    }

    #[test]
    fn multiple_records() {
        let records: Vec<WalRecord> = (0..100)
            .map(|i| WalRecord::new(format!("record-{}", i).into_bytes()))
            .collect();

        let mut buf = Vec::new();
        for r in &records {
            r.encode(&mut buf).unwrap();
        }

        let mut cursor = Cursor::new(&buf);
        for expected in &records {
            let decoded = WalRecord::decode(&mut cursor).unwrap().unwrap();
            assert_eq!(expected, &decoded);
        }

        // Should return None at EOF
        assert!(WalRecord::decode(&mut cursor).unwrap().is_none());
    }
}
