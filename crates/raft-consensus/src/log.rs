use raft_common::error::{Error, Result};
use raft_common::types::{LogIndex, Term};
use raft_storage::wal::reader::WalReader;
use raft_storage::wal::writer::WalWriter;
use std::path::Path;

use crate::message::{EntryType, LogEntry};

/// Raft log backed by a WAL for durability.
///
/// Log entries are 1-indexed. Index 0 is a sentinel (term 0).
/// The WAL stores serialized log entries; the in-memory Vec mirrors them.
pub struct RaftLog {
    entries: Vec<LogEntry>,
    wal: WalWriter,
    /// Index and term of the last snapshot (entries before this are discarded).
    snapshot_index: LogIndex,
    snapshot_term: Term,
}

impl RaftLog {
    /// Open or create a Raft log at the given WAL path.
    pub fn open(wal_path: &Path) -> Result<Self> {
        let mut entries = Vec::new();

        // Replay WAL
        if wal_path.exists() {
            let records = WalReader::open(wal_path)?.read_all()?;
            for record in records {
                let entry = Self::decode_entry(&record.data)?;
                entries.push(entry);
            }
        }

        let wal = WalWriter::open(wal_path)?;

        Ok(Self {
            entries,
            wal,
            snapshot_index: 0,
            snapshot_term: 0,
        })
    }

    /// Append entries to the log. Writes to WAL before returning.
    pub fn append(&mut self, entries: Vec<LogEntry>) -> Result<()> {
        for entry in &entries {
            let data = Self::encode_entry(entry);
            self.wal.append(&data)?;
        }
        self.wal.sync()?;
        self.entries.extend(entries);
        Ok(())
    }

    /// Get a log entry by index. Returns None if out of range.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 || index <= self.snapshot_index {
            return None;
        }
        let vec_idx = (index - self.snapshot_index - 1) as usize;
        self.entries.get(vec_idx)
    }

    /// The index of the last entry in the log.
    pub fn last_index(&self) -> LogIndex {
        if self.entries.is_empty() {
            self.snapshot_index
        } else {
            self.entries.last().unwrap().index
        }
    }

    /// The term of the last entry in the log.
    pub fn last_term(&self) -> Term {
        if self.entries.is_empty() {
            self.snapshot_term
        } else {
            self.entries.last().unwrap().term
        }
    }

    /// Get the term at a given index. Returns 0 for index 0.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Check if the log matches at (index, term).
    pub fn match_term(&self, index: LogIndex, term: Term) -> bool {
        self.term_at(index) == Some(term)
    }

    /// Get entries from start_index to the end.
    pub fn entries_from(&self, start_index: LogIndex) -> &[LogEntry] {
        if start_index <= self.snapshot_index {
            return &self.entries;
        }
        let vec_idx = (start_index - self.snapshot_index - 1) as usize;
        if vec_idx >= self.entries.len() {
            return &[];
        }
        &self.entries[vec_idx..]
    }

    /// Truncate the log after the given index (exclusive).
    /// Used when a leader's AppendEntries reveals our log diverges.
    pub fn truncate_after(&mut self, index: LogIndex) {
        if index <= self.snapshot_index {
            self.entries.clear();
            return;
        }
        let keep = (index - self.snapshot_index) as usize;
        self.entries.truncate(keep);
        // Note: WAL truncation is not done here — on restart, we'll replay
        // and the leader will fix any divergence. A full implementation
        // would rewrite the WAL.
    }

    /// Number of entries in the log (excluding snapshot).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Set snapshot metadata and discard entries up to snapshot_index.
    pub fn compact(&mut self, snapshot_index: LogIndex, snapshot_term: Term) {
        if snapshot_index <= self.snapshot_index {
            return;
        }
        let discard = (snapshot_index - self.snapshot_index) as usize;
        if discard >= self.entries.len() {
            self.entries.clear();
        } else {
            self.entries.drain(..discard);
        }
        self.snapshot_index = snapshot_index;
        self.snapshot_term = snapshot_term;
    }

    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    fn encode_entry(entry: &LogEntry) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&entry.index.to_le_bytes());
        buf.extend_from_slice(&entry.term.to_le_bytes());
        let type_byte = match entry.entry_type {
            EntryType::Normal => 0u8,
            EntryType::ConfigChange => 1,
            EntryType::Noop => 2,
        };
        buf.push(type_byte);
        buf.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.data);
        buf
    }

    fn decode_entry(data: &[u8]) -> Result<LogEntry> {
        if data.len() < 21 {
            return Err(Error::Corruption("log entry too short".to_string()));
        }
        let index = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let term = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let entry_type = match data[16] {
            0 => EntryType::Normal,
            1 => EntryType::ConfigChange,
            2 => EntryType::Noop,
            t => return Err(Error::Corruption(format!("unknown entry type: {}", t))),
        };
        let data_len = u32::from_le_bytes(data[17..21].try_into().unwrap()) as usize;
        let entry_data = data[21..21 + data_len].to_vec();

        Ok(LogEntry {
            index,
            term,
            data: entry_data,
            entry_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(index: LogIndex, term: Term) -> LogEntry {
        LogEntry {
            index,
            term,
            data: format!("entry-{}", index).into_bytes(),
            entry_type: EntryType::Normal,
        }
    }

    #[test]
    fn basic_append_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = RaftLog::open(&dir.path().join("raft.wal")).unwrap();

        log.append(vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.get(1).unwrap().term, 1);
        assert_eq!(log.get(3).unwrap().term, 2);
        assert!(log.get(0).is_none());
        assert!(log.get(4).is_none());
    }

    #[test]
    fn term_at() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = RaftLog::open(&dir.path().join("raft.wal")).unwrap();

        log.append(vec![make_entry(1, 1), make_entry(2, 3)])
            .unwrap();

        assert_eq!(log.term_at(0), Some(0)); // sentinel
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(3));
        assert_eq!(log.term_at(3), None);
    }

    #[test]
    fn match_term_check() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = RaftLog::open(&dir.path().join("raft.wal")).unwrap();

        log.append(vec![make_entry(1, 1), make_entry(2, 2)])
            .unwrap();

        assert!(log.match_term(0, 0)); // sentinel always matches
        assert!(log.match_term(1, 1));
        assert!(!log.match_term(1, 2)); // wrong term
        assert!(log.match_term(2, 2));
    }

    #[test]
    fn entries_from() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = RaftLog::open(&dir.path().join("raft.wal")).unwrap();

        log.append(vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 2),
            make_entry(4, 2),
        ])
        .unwrap();

        let from_3 = log.entries_from(3);
        assert_eq!(from_3.len(), 2);
        assert_eq!(from_3[0].index, 3);
        assert_eq!(from_3[1].index, 4);
    }

    #[test]
    fn truncate_after() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = RaftLog::open(&dir.path().join("raft.wal")).unwrap();

        log.append(vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 2)])
            .unwrap();

        log.truncate_after(1);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn crash_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("raft.wal");

        // Write entries
        {
            let mut log = RaftLog::open(&wal_path).unwrap();
            log.append(vec![make_entry(1, 1), make_entry(2, 2)])
                .unwrap();
        }

        // Reopen — should recover from WAL
        {
            let log = RaftLog::open(&wal_path).unwrap();
            assert_eq!(log.last_index(), 2);
            assert_eq!(log.get(1).unwrap().term, 1);
            assert_eq!(log.get(2).unwrap().term, 2);
        }
    }

    #[test]
    fn compact_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = RaftLog::open(&dir.path().join("raft.wal")).unwrap();

        log.append(vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 2),
            make_entry(4, 2),
        ])
        .unwrap();

        // Compact up to index 2
        log.compact(2, 1);
        assert_eq!(log.snapshot_index(), 2);
        assert_eq!(log.snapshot_term(), 1);
        assert_eq!(log.len(), 2); // entries 3 and 4 remain
        assert!(log.get(1).is_none()); // compacted
        assert!(log.get(2).is_none()); // compacted (at snapshot boundary)
        assert_eq!(log.get(3).unwrap().term, 2);
        assert_eq!(log.last_index(), 4);
    }

    #[test]
    fn empty_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = RaftLog::open(&dir.path().join("raft.wal")).unwrap();

        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert!(log.is_empty());
    }
}
