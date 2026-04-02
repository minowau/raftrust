use raft_common::error::Result;
use raft_consensus::message::{EntryType, LogEntry};
use raft_consensus::node::RaftNode;
use raft_mvcc::mvcc::MvccStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::lease::LeaseManager;
use crate::watch::{WatchEvent, WatchEventType, WatchHub};

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
    LeaseGrant {
        lease_id: i64,
        ttl: i64,
    },
    LeaseRevoke {
        lease_id: i64,
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
/// Publishes watch events and manages lease key attachments.
pub struct ApplyLoop {
    store: Arc<MvccStore>,
    node: Option<Arc<RaftNode>>,
    watch_hub: Option<Arc<WatchHub>>,
    lease_mgr: Option<Arc<LeaseManager>>,
}

impl ApplyLoop {
    pub fn new(store: Arc<MvccStore>) -> Self {
        Self {
            store,
            node: None,
            watch_hub: None,
            lease_mgr: None,
        }
    }

    /// Create with a RaftNode reference for applying config changes.
    pub fn with_node(store: Arc<MvccStore>, node: Arc<RaftNode>) -> Self {
        Self {
            store,
            node: Some(node),
            watch_hub: None,
            lease_mgr: None,
        }
    }

    /// Create fully wired with all Phase 6/7 components.
    pub fn full(
        store: Arc<MvccStore>,
        node: Arc<RaftNode>,
        watch_hub: Arc<WatchHub>,
        lease_mgr: Arc<LeaseManager>,
    ) -> Self {
        Self {
            store,
            node: Some(node),
            watch_hub: Some(watch_hub),
            lease_mgr: Some(lease_mgr),
        }
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
                    if let Some(ref node) = self.node {
                        if let Some(new_config) = node.apply_config_change(&entry.data) {
                            info!(
                                index = entry.index,
                                members = ?new_config.member_ids(),
                                "Applied config change"
                            );
                        }
                    } else {
                        debug!(
                            index = entry.index,
                            "Config change entry (no node reference to apply)"
                        );
                    }
                    applied += 1;
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
                // Get previous value for watch event
                let prev = self.store.get(key)?;

                let revision = self
                    .store
                    .put_with_options(key, value, *lease_id, *ttl_seconds)?;

                // Attach key to lease if specified
                if *lease_id > 0 {
                    if let Some(ref mgr) = self.lease_mgr {
                        let _ = mgr.attach_key(*lease_id, key.clone());
                    }
                }

                // Publish watch event
                if let Some(ref hub) = self.watch_hub {
                    hub.publish(WatchEvent {
                        event_type: WatchEventType::Put,
                        key: key.clone(),
                        value: value.clone(),
                        mod_revision: revision,
                        create_revision: revision, // simplified; real create_revision comes from MVCC
                        prev_value: prev.map(|kv| kv.value),
                    });
                }
            }
            KvCommand::Delete { key } => {
                // Get previous value for watch event
                let prev = self.store.get(key)?;

                let (revision, _existed) = self.store.delete(key)?;

                // Detach key from any lease
                if let Some(ref prev_kv) = prev {
                    if prev_kv.lease_id > 0 {
                        if let Some(ref mgr) = self.lease_mgr {
                            mgr.detach_key(prev_kv.lease_id, key);
                        }
                    }
                }

                // Publish watch event
                if let Some(ref hub) = self.watch_hub {
                    hub.publish(WatchEvent {
                        event_type: WatchEventType::Delete,
                        key: key.clone(),
                        value: vec![],
                        mod_revision: revision,
                        create_revision: 0,
                        prev_value: prev.map(|kv| kv.value),
                    });
                }
            }
            KvCommand::LeaseGrant { lease_id, ttl } => {
                if let Some(ref mgr) = self.lease_mgr {
                    let _ = mgr.grant(*lease_id, *ttl);
                }
            }
            KvCommand::LeaseRevoke { lease_id } => {
                if let Some(ref mgr) = self.lease_mgr {
                    if let Ok(keys) = mgr.revoke(*lease_id) {
                        // Delete all keys attached to this lease
                        for key in keys {
                            let _ = self.store.delete(&key);
                            if let Some(ref hub) = self.watch_hub {
                                hub.publish(WatchEvent {
                                    event_type: WatchEventType::Delete,
                                    key: key.clone(),
                                    value: vec![],
                                    mod_revision: self.store.revision(),
                                    create_revision: 0,
                                    prev_value: None,
                                });
                            }
                        }
                    }
                }
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

    #[test]
    fn apply_publishes_watch_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());
        let hub = Arc::new(WatchHub::new(16));

        let apply = ApplyLoop {
            store: store.clone(),
            node: None,
            watch_hub: Some(hub.clone()),
            lease_mgr: None,
        };

        // Create a watcher before the put
        let (_id, mut rx) = hub.create_watcher(b"key".to_vec(), vec![], 0);

        let entries = vec![LogEntry {
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
        }];

        apply.apply(&entries).unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, WatchEventType::Put);
        assert_eq!(event.key, b"key");
        assert_eq!(event.value, b"val");
    }

    #[test]
    fn apply_delete_publishes_watch_event_with_prev() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());
        let hub = Arc::new(WatchHub::new(16));

        let apply = ApplyLoop {
            store: store.clone(),
            node: None,
            watch_hub: Some(hub.clone()),
            lease_mgr: None,
        };

        // Put first
        store.put(b"key", b"old_val").unwrap();

        // Watch before delete
        let (_id, mut rx) = hub.create_watcher(b"key".to_vec(), vec![], 0);

        let entries = vec![LogEntry {
            index: 1,
            term: 1,
            data: KvCommand::Delete {
                key: b"key".to_vec(),
            }
            .encode(),
            entry_type: EntryType::Normal,
        }];

        apply.apply(&entries).unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.event_type, WatchEventType::Delete);
        assert_eq!(event.prev_value, Some(b"old_val".to_vec()));
    }

    #[test]
    fn apply_with_lease_attaches_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());
        let lease_mgr = Arc::new(LeaseManager::new());

        // Grant lease first
        lease_mgr.grant(42, 60).unwrap();

        let apply = ApplyLoop {
            store: store.clone(),
            node: None,
            watch_hub: None,
            lease_mgr: Some(lease_mgr.clone()),
        };

        let entries = vec![LogEntry {
            index: 1,
            term: 1,
            data: KvCommand::Put {
                key: b"key".to_vec(),
                value: b"val".to_vec(),
                lease_id: 42,
                ttl_seconds: 0,
            }
            .encode(),
            entry_type: EntryType::Normal,
        }];

        apply.apply(&entries).unwrap();

        let info = lease_mgr.get(42).unwrap();
        assert!(info.keys.contains(&b"key".to_vec()));
    }

    #[test]
    fn apply_lease_revoke_deletes_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());
        let lease_mgr = Arc::new(LeaseManager::new());

        // Grant lease and put a key
        lease_mgr.grant(42, 60).unwrap();
        store.put_with_options(b"locked", b"data", 42, 0).unwrap();
        lease_mgr.attach_key(42, b"locked".to_vec()).unwrap();

        let apply = ApplyLoop {
            store: store.clone(),
            node: None,
            watch_hub: None,
            lease_mgr: Some(lease_mgr.clone()),
        };

        let entries = vec![LogEntry {
            index: 1,
            term: 1,
            data: KvCommand::LeaseRevoke { lease_id: 42 }.encode(),
            entry_type: EntryType::Normal,
        }];

        apply.apply(&entries).unwrap();

        // Key should be deleted
        assert!(store.get(b"locked").unwrap().is_none());
        assert!(!lease_mgr.is_alive(42));
    }

    #[test]
    fn lease_command_roundtrip() {
        let cmd = KvCommand::LeaseGrant {
            lease_id: 99,
            ttl: 30,
        };
        let encoded = cmd.encode();
        let decoded = KvCommand::decode(&encoded).unwrap();
        match decoded {
            KvCommand::LeaseGrant { lease_id, ttl } => {
                assert_eq!(lease_id, 99);
                assert_eq!(ttl, 30);
            }
            _ => panic!("expected LeaseGrant"),
        }
    }
}
