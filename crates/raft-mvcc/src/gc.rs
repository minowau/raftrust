use raft_common::error::Result;
use raft_storage::engine::StorageEngine;
use std::sync::Arc;

use crate::version::decode_key;

/// Garbage collector for old MVCC versions.
///
/// Removes versions older than a given retention revision, keeping only
/// the latest version of each key. This reclaims storage space from
/// historical versions that are no longer needed for snapshot reads.
pub struct GarbageCollector {
    engine: Arc<dyn StorageEngine>,
}

impl GarbageCollector {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Self { engine }
    }

    /// Remove all versions with revision < `min_revision`, except the latest
    /// version of each key.
    ///
    /// Returns the number of versions removed.
    pub fn collect(&self, min_revision: u64) -> Result<usize> {
        let start = vec![0u8];
        let end = vec![0xFF; 32];

        let all_entries = self.engine.scan(&start, &end)?;

        let mut to_delete = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut seen_latest = false;

        for (internal_key, _raw_value) in &all_entries {
            let (user_key, rev) = decode_key(internal_key);

            if last_user_key.as_deref() != Some(user_key) {
                // New user key — reset
                last_user_key = Some(user_key.to_vec());
                seen_latest = false;
            }

            if !seen_latest {
                // Keep the latest version regardless of revision
                seen_latest = true;
                continue;
            }

            // This is an older version — GC if below threshold
            if rev < min_revision {
                to_delete.push(internal_key.clone());
            }
        }

        let count = to_delete.len();
        for key in to_delete {
            self.engine.delete(&key)?;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::MvccStore;
    use raft_storage::lsm::{LsmConfig, LsmTree};

    #[test]
    fn gc_removes_old_versions() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            LsmTree::open(
                dir.path(),
                LsmConfig {
                    memtable_size_limit: 64 * 1024,
                    block_size: 256,
                    ..Default::default()
                },
            )
            .unwrap(),
        );

        let store = MvccStore::new(engine.clone());
        store.put(b"key", b"v1").unwrap(); // rev 1
        store.put(b"key", b"v2").unwrap(); // rev 2
        store.put(b"key", b"v3").unwrap(); // rev 3

        // Before GC: snapshot read at rev 1 should work
        let v1 = store.get_at_revision(b"key", 1).unwrap();
        assert!(v1.is_some());

        // GC versions older than rev 3
        let gc = GarbageCollector::new(engine.clone());
        let removed = gc.collect(3).unwrap();
        assert_eq!(removed, 2); // v1 and v2 removed

        // Latest version still accessible
        let latest = store.get(b"key").unwrap().unwrap();
        assert_eq!(latest.value, b"v3");

        // Old versions are gone — snapshot read at rev 1 returns None
        let v1 = store.get_at_revision(b"key", 1).unwrap();
        assert!(v1.is_none());
    }

    #[test]
    fn gc_keeps_latest_even_if_old() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            LsmTree::open(
                dir.path(),
                LsmConfig {
                    memtable_size_limit: 64 * 1024,
                    block_size: 256,
                    ..Default::default()
                },
            )
            .unwrap(),
        );

        let store = MvccStore::new(engine.clone());
        store.put(b"key", b"only-version").unwrap(); // rev 1

        // GC with threshold above the only version
        let gc = GarbageCollector::new(engine);
        let removed = gc.collect(100).unwrap();
        assert_eq!(removed, 0); // latest version is always kept

        assert_eq!(
            store.get(b"key").unwrap().unwrap().value,
            b"only-version"
        );
    }
}
