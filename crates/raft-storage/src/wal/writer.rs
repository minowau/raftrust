use raft_common::error::Result;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use super::record::WalRecord;

/// Append-only WAL writer with CRC32 integrity on every record.
///
/// Each write appends a `WalRecord` to the file and optionally fsyncs.
/// The caller controls sync policy (every write vs. batched).
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    bytes_written: u64,
}

impl WalWriter {
    /// Open or create a WAL file at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let bytes_written = file.metadata()?.len();

        Ok(Self {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
            bytes_written,
        })
    }

    /// Append a record to the WAL. Does NOT fsync — call `sync()` explicitly.
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        let record = WalRecord::new(data.to_vec());
        let written = record.encode(&mut self.writer)?;
        self.bytes_written += written as u64;
        Ok(())
    }

    /// Flush buffer and fsync to disk, ensuring durability.
    pub fn sync(&mut self) -> Result<()> {
        use std::io::Write;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Total bytes written to this WAL file.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Path to this WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::reader::WalReader;

    #[test]
    fn write_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write records
        {
            let mut writer = WalWriter::open(&wal_path).unwrap();
            writer.append(b"first").unwrap();
            writer.append(b"second").unwrap();
            writer.append(b"third").unwrap();
            writer.sync().unwrap();
        }

        // Read back
        let records = WalReader::open(&wal_path).unwrap().read_all().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].data, b"first");
        assert_eq!(records[1].data, b"second");
        assert_eq!(records[2].data, b"third");
    }

    #[test]
    fn append_to_existing() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write first batch
        {
            let mut writer = WalWriter::open(&wal_path).unwrap();
            writer.append(b"one").unwrap();
            writer.sync().unwrap();
        }

        // Reopen and append more
        {
            let mut writer = WalWriter::open(&wal_path).unwrap();
            writer.append(b"two").unwrap();
            writer.sync().unwrap();
        }

        let records = WalReader::open(&wal_path).unwrap().read_all().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].data, b"one");
        assert_eq!(records[1].data, b"two");
    }
}
