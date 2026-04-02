use raft_common::error::Result;
use raft_consensus::message::{EntryType, LogEntry};
use raft_mvcc::mvcc::MvccStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};

/// A command that can be proposed to Raft and applied to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvCommand {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: i64,
        ttl_seconds: i64,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl KvCommand {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(|e| {
            raft_common::error::Error::Corruption(format!("failed to decode KvCommand: {}", e))
        })
    }
}

/// Applies committed Raft log entries to the MvccStore.
pub struct ApplyLoop {
    store: Arc<MvccStore>,
}

impl ApplyLoop {
    pub fn new(store: Arc<MvccStore>) -> Self {
        Self { store }
    }

    /// Apply a batch of committed entries. Returns the number applied.
    pub fn apply(&self, entries: &[LogEntry]) -> Result<usize> {
        let mut applied = 0;

        for entry in entries {
            match entry.entry_type {
                EntryType::Normal => {
                    if entry.data.is_empty() {
                        continue;
                    }
                    match KvCommand::decode(&entry.data) {
                        Ok(cmd) => {
                            self.apply_command(&cmd)?;
                            applied += 1;
                        }
                        Err(e) => {
                            warn!(index = entry.index, error = %e, "Failed to decode command");
                        }
                    }
                }
                EntryType::Noop => {
                    debug!(index = entry.index, "Applied no-op entry");
                }
                EntryType::ConfigChange => {
                    debug!(
                        index = entry.index,
                        "Config change entry (not yet implemented)"
                    );
                }
            }
        }

        Ok(applied)
    }

    fn apply_command(&self, cmd: &KvCommand) -> Result<()> {
        match cmd {
            KvCommand::Put {
                key,
                value,
                lease_id,
                ttl_seconds,
            } => {
                self.store
                    .put_with_options(key, value, *lease_id, *ttl_seconds)?;
            }
            KvCommand::Delete { key } => {
                self.store.delete(key)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_storage::lsm::{LsmConfig, LsmTree};

    fn test_store(dir: &std::path::Path) -> Arc<MvccStore> {
        let engine = Arc::new(
            LsmTree::open(
                dir,
                LsmConfig {
                    memtable_size_limit: 64 * 1024,
                    block_size: 256,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        Arc::new(MvccStore::new(engine))
    }

    #[test]
    fn apply_put_command() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());
        let apply = ApplyLoop::new(store.clone());

        let cmd = KvCommand::Put {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            lease_id: 0,
            ttl_seconds: 0,
        };

        let entries = vec![LogEntry {
            index: 1,
            term: 1,
            data: cmd.encode(),
            entry_type: EntryType::Normal,
        }];

        let applied = apply.apply(&entries).unwrap();
        assert_eq!(applied, 1);

        let kv = store.get(b"hello").unwrap().unwrap();
        assert_eq!(kv.value, b"world");
    }

    #[test]
    fn apply_delete_command() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());
        let apply = ApplyLoop::new(store.clone());

        // Put then delete
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                data: KvCommand::Put {
                    key: b"key".to_vec(),
                    value: b"val".to_vec(),
                    lease_id: 0,
                    ttl_seconds: 0,
                }
                .encode(),
                entry_type: EntryType::Normal,
            },
            LogEntry {
                index: 2,
                term: 1,
                data: KvCommand::Delete {
                    key: b"key".to_vec(),
                }
                .encode(),
                entry_type: EntryType::Normal,
            },
        ];

        apply.apply(&entries).unwrap();
        assert!(store.get(b"key").unwrap().is_none());
    }

    #[test]
    fn skip_noop_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());
        let apply = ApplyLoop::new(store.clone());

        let entries = vec![LogEntry {
            index: 1,
            term: 1,
            data: vec![],
            entry_type: EntryType::Noop,
        }];

        let applied = apply.apply(&entries).unwrap();
        assert_eq!(applied, 0);
    }

    #[test]
    fn kv_command_roundtrip() {
        let cmd = KvCommand::Put {
            key: b"test".to_vec(),
            value: b"data".to_vec(),
            lease_id: 42,
            ttl_seconds: 300,
        };

        let encoded = cmd.encode();
        let decoded = KvCommand::decode(&encoded).unwrap();

        match decoded {
            KvCommand::Put {
                key,
                value,
                lease_id,
                ttl_seconds,
            } => {
                assert_eq!(key, b"test");
                assert_eq!(value, b"data");
                assert_eq!(lease_id, 42);
                assert_eq!(ttl_seconds, 300);
            }
            _ => panic!("expected Put"),
        }
    }
}
