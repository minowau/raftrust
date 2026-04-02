use crc32fast::Hasher;
use raft_common::error::{Error, Result};
use raft_common::types::LogIndex;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::info;

/// Metadata stored alongside a snapshot file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_index: LogIndex,
    pub last_included_term: u64,
    pub checksum: u32,
    pub size: u64,
}

/// Manages snapshot creation, storage, and restoration.
///
/// Snapshots are stored as files in the snapshot directory. Each snapshot
/// consists of a data file (the serialized state machine) and a metadata
/// file (JSON with index, term, and checksum).
pub struct SnapshotManager {
    snapshot_dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(snapshot_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(snapshot_dir)?;
        Ok(Self {
            snapshot_dir: snapshot_dir.to_path_buf(),
        })
    }

    /// Create a snapshot from raw state machine data.
    pub fn create_snapshot(
        &self,
        last_index: LogIndex,
        last_term: u64,
        data: &[u8],
    ) -> Result<SnapshotMetadata> {
        let checksum = Self::compute_checksum(data);
        let metadata = SnapshotMetadata {
            last_included_index: last_index,
            last_included_term: last_term,
            checksum,
            size: data.len() as u64,
        };

        let data_path = self.snapshot_data_path(last_index);
        std::fs::write(&data_path, data)?;

        let meta_path = self.snapshot_meta_path(last_index);
        let meta_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Storage(format!("serialize snapshot metadata: {}", e)))?;
        std::fs::write(&meta_path, meta_json)?;

        info!(
            index = last_index,
            term = last_term,
            size = data.len(),
            "Created snapshot"
        );

        self.cleanup_old_snapshots(last_index)?;

        Ok(metadata)
    }

    /// Load the latest snapshot. Returns (metadata, data) or None if none exists.
    pub fn load_latest(&self) -> Result<Option<(SnapshotMetadata, Vec<u8>)>> {
        let latest = self.find_latest_snapshot()?;
        match latest {
            Some(index) => {
                let meta = self.load_metadata(index)?;
                let data = self.load_data(index)?;

                let computed = Self::compute_checksum(&data);
                if computed != meta.checksum {
                    return Err(Error::Corruption(format!(
                        "snapshot checksum mismatch: stored={:#010x}, computed={:#010x}",
                        meta.checksum, computed
                    )));
                }

                Ok(Some((meta, data)))
            }
            None => Ok(None),
        }
    }

    /// Receive a snapshot from a leader (InstallSnapshot RPC).
    pub fn receive_snapshot(&self, metadata: &SnapshotMetadata, data: &[u8]) -> Result<()> {
        let computed = Self::compute_checksum(data);
        if computed != metadata.checksum {
            return Err(Error::Corruption(format!(
                "received snapshot checksum mismatch: expected={:#010x}, computed={:#010x}",
                metadata.checksum, computed
            )));
        }

        let data_path = self.snapshot_data_path(metadata.last_included_index);
        std::fs::write(&data_path, data)?;

        let meta_path = self.snapshot_meta_path(metadata.last_included_index);
        let meta_json = serde_json::to_string_pretty(metadata)
            .map_err(|e| Error::Storage(format!("serialize snapshot metadata: {}", e)))?;
        std::fs::write(&meta_path, meta_json)?;

        self.cleanup_old_snapshots(metadata.last_included_index)?;

        Ok(())
    }

    /// Read snapshot data as bytes (for streaming to a follower).
    pub fn read_snapshot_data(&self, index: LogIndex) -> Result<Vec<u8>> {
        self.load_data(index)
    }

    fn snapshot_data_path(&self, index: LogIndex) -> PathBuf {
        self.snapshot_dir
            .join(format!("snapshot-{:020}.data", index))
    }

    fn snapshot_meta_path(&self, index: LogIndex) -> PathBuf {
        self.snapshot_dir
            .join(format!("snapshot-{:020}.meta", index))
    }

    fn load_metadata(&self, index: LogIndex) -> Result<SnapshotMetadata> {
        let path = self.snapshot_meta_path(index);
        let data = std::fs::read_to_string(&path)?;
        serde_json::from_str(&data)
            .map_err(|e| Error::Corruption(format!("parse snapshot metadata: {}", e)))
    }

    fn load_data(&self, index: LogIndex) -> Result<Vec<u8>> {
        let path = self.snapshot_data_path(index);
        Ok(std::fs::read(&path)?)
    }

    fn find_latest_snapshot(&self) -> Result<Option<LogIndex>> {
        let mut latest: Option<LogIndex> = None;
        for entry in std::fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("snapshot-") && name.ends_with(".meta") {
                let index_str = name
                    .strip_prefix("snapshot-")
                    .and_then(|s| s.strip_suffix(".meta"))
                    .unwrap_or("0");
                if let Ok(index) = index_str.parse::<LogIndex>() {
                    latest = Some(latest.map_or(index, |l: LogIndex| l.max(index)));
                }
            }
        }
        Ok(latest)
    }

    fn cleanup_old_snapshots(&self, keep_index: LogIndex) -> Result<()> {
        for entry in std::fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("snapshot-") {
                let index_part = name_str
                    .strip_prefix("snapshot-")
                    .and_then(|s| s.split('.').next())
                    .unwrap_or("0");
                if let Ok(index) = index_part.parse::<LogIndex>() {
                    if index < keep_index {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }

    fn compute_checksum(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_load_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SnapshotManager::new(&dir.path().join("snapshots")).unwrap();

        let data = b"state machine data at index 100";
        let meta = mgr.create_snapshot(100, 5, data).unwrap();

        assert_eq!(meta.last_included_index, 100);
        assert_eq!(meta.last_included_term, 5);
        assert_eq!(meta.size, data.len() as u64);

        let (loaded_meta, loaded_data) = mgr.load_latest().unwrap().unwrap();
        assert_eq!(loaded_meta.last_included_index, 100);
        assert_eq!(loaded_data, data);
    }

    #[test]
    fn latest_snapshot_wins() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SnapshotManager::new(&dir.path().join("snapshots")).unwrap();

        mgr.create_snapshot(50, 3, b"old").unwrap();
        mgr.create_snapshot(100, 5, b"new").unwrap();

        let (meta, data) = mgr.load_latest().unwrap().unwrap();
        assert_eq!(meta.last_included_index, 100);
        assert_eq!(data, b"new");
    }

    #[test]
    fn corrupt_snapshot_detected() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let mgr = SnapshotManager::new(&snap_dir).unwrap();

        mgr.create_snapshot(100, 5, b"valid data").unwrap();

        // Corrupt the data file
        let data_path = snap_dir.join("snapshot-00000000000000000100.data");
        std::fs::write(&data_path, b"corrupted!").unwrap();

        let result = mgr.load_latest();
        assert!(result.is_err());
    }

    #[test]
    fn receive_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SnapshotManager::new(&dir.path().join("snapshots")).unwrap();

        let data = b"received state";
        let checksum = {
            let mut h = Hasher::new();
            h.update(data);
            h.finalize()
        };
        let meta = SnapshotMetadata {
            last_included_index: 200,
            last_included_term: 10,
            checksum,
            size: data.len() as u64,
        };

        mgr.receive_snapshot(&meta, data).unwrap();

        let (loaded_meta, loaded_data) = mgr.load_latest().unwrap().unwrap();
        assert_eq!(loaded_meta.last_included_index, 200);
        assert_eq!(loaded_data, data);
    }

    #[test]
    fn receive_bad_checksum_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SnapshotManager::new(&dir.path().join("snapshots")).unwrap();

        let meta = SnapshotMetadata {
            last_included_index: 200,
            last_included_term: 10,
            checksum: 0xDEADBEEF,
            size: 5,
        };

        let result = mgr.receive_snapshot(&meta, b"data!");
        assert!(result.is_err());
    }

    #[test]
    fn no_snapshot_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SnapshotManager::new(&dir.path().join("snapshots")).unwrap();
        assert!(mgr.load_latest().unwrap().is_none());
    }

    #[test]
    fn old_snapshots_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        let snap_dir = dir.path().join("snapshots");
        let mgr = SnapshotManager::new(&snap_dir).unwrap();

        mgr.create_snapshot(50, 3, b"old").unwrap();
        let old_data = snap_dir.join("snapshot-00000000000000000050.data");
        assert!(old_data.exists());

        mgr.create_snapshot(100, 5, b"new").unwrap();
        assert!(!old_data.exists());
    }
}
