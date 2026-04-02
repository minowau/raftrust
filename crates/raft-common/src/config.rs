use serde::{Deserialize, Serialize};

/// Configuration for the storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory for all data files (WAL, SSTables, snapshots).
    pub data_dir: String,

    /// Maximum size of a MemTable before flushing to SSTable (bytes).
    pub memtable_size_limit: usize,

    /// Maximum number of SSTables per level before triggering compaction.
    pub level_size_ratio: usize,

    /// Target size for SSTable data blocks (bytes).
    pub block_size: usize,

    /// Bloom filter target false positive rate.
    pub bloom_false_positive_rate: f64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
            memtable_size_limit: 4 * 1024 * 1024, // 4 MB
            level_size_ratio: 10,
            block_size: 4096,
            bloom_false_positive_rate: 0.01,
        }
    }
}

/// Configuration for the Raft consensus module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// This node's ID.
    pub node_id: u64,

    /// Minimum election timeout in milliseconds.
    pub election_timeout_min_ms: u64,

    /// Maximum election timeout in milliseconds.
    pub election_timeout_max_ms: u64,

    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,

    /// Maximum number of log entries before triggering a snapshot.
    pub snapshot_threshold: u64,

    /// Peer addresses: node_id -> "host:port".
    pub peers: std::collections::HashMap<u64, String>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_threshold: 10_000,
            peers: std::collections::HashMap::new(),
        }
    }
}
