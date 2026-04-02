use raft_consensus_core::node::RaftNode;
use raft_mvcc::mvcc::MvccStore;
use serde::{Deserialize, Serialize};
use tracing::info;

/// Metadata included with a backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub node_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub revision: u64,
    pub timestamp: u64,
}

/// Create a point-in-time backup of the KV store.
///
/// The backup consists of:
/// 1. A snapshot of the Raft state (from the snapshot manager)
/// 2. Metadata about the backup point
///
/// Returns (metadata, snapshot_data) or None if no snapshot exists.
pub fn create_backup(node: &RaftNode, store: &MvccStore) -> Option<(BackupMetadata, Vec<u8>)> {
    // First, trigger a fresh snapshot
    let revision = store.revision();

    // Try to get the latest snapshot
    let (snap_meta, snap_data) = node.get_snapshot_for_follower()?;

    let metadata = BackupMetadata {
        node_id: node.id(),
        term: node.term(),
        commit_index: node.commit_index(),
        applied_index: node.last_applied(),
        revision,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    info!(
        node = node.id(),
        revision = revision,
        snapshot_index = snap_meta.last_included_index,
        "Created backup"
    );

    // Combine metadata + snapshot data into a single backup blob
    let meta_json = serde_json::to_vec(&metadata).ok()?;
    let meta_len = meta_json.len() as u32;

    let mut backup = Vec::with_capacity(4 + meta_json.len() + snap_data.len());
    backup.extend_from_slice(&meta_len.to_le_bytes());
    backup.extend_from_slice(&meta_json);
    backup.extend_from_slice(&snap_data);

    Some((metadata, backup))
}

/// Parse a backup blob into metadata and snapshot data.
pub fn parse_backup(data: &[u8]) -> Option<(BackupMetadata, Vec<u8>)> {
    if data.len() < 4 {
        return None;
    }

    let meta_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

    if data.len() < 4 + meta_len {
        return None;
    }

    let metadata: BackupMetadata = serde_json::from_slice(&data[4..4 + meta_len]).ok()?;
    let snapshot_data = data[4 + meta_len..].to_vec();

    Some((metadata, snapshot_data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_metadata_roundtrip() {
        let meta = BackupMetadata {
            node_id: 1,
            term: 5,
            commit_index: 100,
            applied_index: 99,
            revision: 50,
            timestamp: 1700000000,
        };

        let json = serde_json::to_vec(&meta).unwrap();
        let decoded: BackupMetadata = serde_json::from_slice(&json).unwrap();

        assert_eq!(decoded.node_id, 1);
        assert_eq!(decoded.term, 5);
        assert_eq!(decoded.commit_index, 100);
        assert_eq!(decoded.revision, 50);
    }

    #[test]
    fn parse_backup_blob() {
        let meta = BackupMetadata {
            node_id: 1,
            term: 3,
            commit_index: 50,
            applied_index: 50,
            revision: 25,
            timestamp: 1700000000,
        };

        let meta_json = serde_json::to_vec(&meta).unwrap();
        let meta_len = meta_json.len() as u32;
        let snapshot_data = b"fake-snapshot-data";

        let mut blob = Vec::new();
        blob.extend_from_slice(&meta_len.to_le_bytes());
        blob.extend_from_slice(&meta_json);
        blob.extend_from_slice(snapshot_data);

        let (parsed_meta, parsed_snap) = parse_backup(&blob).unwrap();
        assert_eq!(parsed_meta.node_id, 1);
        assert_eq!(parsed_meta.term, 3);
        assert_eq!(parsed_snap, snapshot_data);
    }

    #[test]
    fn parse_backup_too_short() {
        assert!(parse_backup(&[0, 0]).is_none());
        assert!(parse_backup(&[100, 0, 0, 0]).is_none()); // meta_len too large
    }
}
