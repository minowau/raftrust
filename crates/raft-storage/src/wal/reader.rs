use raft_common::error::{Error, Result};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use tracing::warn;

use super::record::WalRecord;

/// Sequential WAL reader for crash recovery / replay.
///
/// Reads all valid records from a WAL file. If a corruption is detected
/// (CRC mismatch or truncated record), reading stops at that point —
/// all records before the corruption are returned. This handles crash
/// recovery where the tail of the file may be partially written.
pub struct WalReader {
    reader: BufReader<File>,
    path: PathBuf,
}

impl WalReader {
    /// Open a WAL file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            path: path.to_path_buf(),
        })
    }

    /// Read all valid records. Stops at first corruption (crash recovery).
    pub fn read_all(mut self) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();

        loop {
            match WalRecord::decode(&mut self.reader) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break, // Clean EOF
                Err(Error::Corruption(msg)) => {
                    warn!(
                        path = %self.path.display(),
                        records_recovered = records.len(),
                        "WAL corruption detected, stopping replay: {}",
                        msg
                    );
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// Read all records, returning an error on any corruption instead of
    /// stopping gracefully. Useful for tests that expect a clean WAL.
    pub fn read_all_strict(mut self) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();

        loop {
            match WalRecord::decode(&mut self.reader) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use std::io::Write;

    #[test]
    fn recover_from_truncated_tail() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write 5 valid records
        {
            let mut writer = WalWriter::open(&wal_path).unwrap();
            for i in 0..5 {
                writer.append(format!("record-{}", i).as_bytes()).unwrap();
            }
            writer.sync().unwrap();
        }

        // Corrupt the tail: append garbage bytes
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x05, 0x01, 0x02])
                .unwrap();
        }

        // Recovery should return 5 valid records, ignoring the corrupt tail
        let records = WalReader::open(&wal_path).unwrap().read_all().unwrap();
        assert_eq!(records.len(), 5);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.data, format!("record-{}", i).as_bytes());
        }
    }

    #[test]
    fn strict_mode_fails_on_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::open(&wal_path).unwrap();
            writer.append(b"valid").unwrap();
            writer.sync().unwrap();
        }

        // Append corrupt data
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            file.write_all(&[0xFF; 20]).unwrap();
        }

        let result = WalReader::open(&wal_path).unwrap().read_all_strict();
        assert!(result.is_err());
    }

    #[test]
    fn empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        std::fs::File::create(&wal_path).unwrap();

        let records = WalReader::open(&wal_path).unwrap().read_all().unwrap();
        assert!(records.is_empty());
    }
}
