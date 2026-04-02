use parking_lot::RwLock;
use raft_common::error::Result;
use raft_storage::engine::StorageEngine;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::version::{
    decode_key, encode_key, encode_key_prefix_end, encode_key_prefix_start, VersionedValue,
};

/// Key-value pair returned by MVCC operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub create_revision: u64,
    pub mod_revision: u64,
    pub lease_id: i64,
}

/// MVCC store wrapping a StorageEngine with versioned reads and writes.
///
/// Every write creates a new version at the current revision. Reads can
/// target a specific revision for snapshot isolation, or read the latest.
pub struct MvccStore {
    engine: Arc<dyn StorageEngine>,
    current_revision: AtomicU64,
    /// Lock for serializing writes (ensures revision monotonicity).
    write_lock: RwLock<()>,
}

impl MvccStore {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Self {
            engine,
            current_revision: AtomicU64::new(0),
            write_lock: RwLock::new(()),
        }
    }

    /// Restore the revision counter (e.g., after WAL replay).
    pub fn set_revision(&self, revision: u64) {
        self.current_revision.store(revision, Ordering::SeqCst);
    }

    /// Current revision number.
    pub fn revision(&self) -> u64 {
        self.current_revision.load(Ordering::SeqCst)
    }

    /// Allocate the next revision number.
    fn next_revision(&self) -> u64 {
        self.current_revision.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Get the latest version of a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<KeyValue>> {
        self.get_at_revision(key, u64::MAX)
    }

    /// Get the version of a key at or before the given revision.
    pub fn get_at_revision(&self, key: &[u8], revision: u64) -> Result<Option<KeyValue>> {
        let start = encode_key_prefix_start(key);
        let end = encode_key_prefix_end(key);

        let entries = self.engine.scan(&start, &end)?;

        for (internal_key, raw_value) in entries {
            let (_, rev) = decode_key(&internal_key);
            if rev > revision {
                continue; // Skip versions newer than requested
            }
            let vv = VersionedValue::decode(&raw_value);
            return match vv.value {
                Some(value) => Ok(Some(KeyValue {
                    key: key.to_vec(),
                    value,
                    create_revision: vv.create_revision,
                    mod_revision: vv.mod_revision,
                    lease_id: vv.lease_id,
                })),
                None => Ok(None), // Delete marker
            };
        }

        Ok(None)
    }

    /// Put a key-value pair. Returns the new revision.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        self.put_with_options(key, value, 0, 0)
    }

    /// Put with lease and TTL options.
    pub fn put_with_options(
        &self,
        key: &[u8],
        value: &[u8],
        lease_id: i64,
        ttl_seconds: i64,
    ) -> Result<u64> {
        let _guard = self.write_lock.write();
        let revision = self.next_revision();

        // Check if this key existed before to set create_revision
        let create_revision = match self.get_latest_version_info(key)? {
            Some((_, vv)) if vv.value.is_some() => vv.create_revision,
            _ => revision, // New key
        };

        let vv = VersionedValue {
            value: Some(value.to_vec()),
            create_revision,
            mod_revision: revision,
            lease_id,
            ttl_seconds,
        };

        let internal_key = encode_key(key, revision);
        self.engine.put(&internal_key, &vv.encode())?;

        Ok(revision)
    }

    /// Delete a key. Returns the revision and whether the key existed.
    pub fn delete(&self, key: &[u8]) -> Result<(u64, bool)> {
        let _guard = self.write_lock.write();

        // Check if key exists
        let existed = match self.get_latest_version_info(key)? {
            Some((_, vv)) => vv.value.is_some(),
            None => false,
        };

        let revision = self.next_revision();

        let vv = VersionedValue {
            value: None, // delete marker
            create_revision: 0,
            mod_revision: revision,
            lease_id: 0,
            ttl_seconds: 0,
        };

        let internal_key = encode_key(key, revision);
        self.engine.put(&internal_key, &vv.encode())?;

        Ok((revision, existed))
    }

    /// Scan keys in range [start_key, end_key) at the latest revision.
    pub fn scan(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<KeyValue>> {
        self.scan_at_revision(start_key, end_key, u64::MAX)
    }

    /// Scan keys in range at a specific revision.
    pub fn scan_at_revision(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        revision: u64,
    ) -> Result<Vec<KeyValue>> {
        let internal_start = encode_key_prefix_start(start_key);
        let internal_end = encode_key_prefix_end(end_key);

        let entries = self.engine.scan(&internal_start, &internal_end)?;

        let mut results = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        for (internal_key, raw_value) in entries {
            let (user_key, rev) = decode_key(&internal_key);

            // Skip if we already found a version for this user key
            if let Some(ref last) = last_user_key {
                if last.as_slice() == user_key {
                    continue;
                }
            }

            if rev > revision {
                continue;
            }

            last_user_key = Some(user_key.to_vec());

            let vv = VersionedValue::decode(&raw_value);
            if let Some(value) = vv.value {
                // Only include if user_key is in [start_key, end_key)
                if user_key >= start_key && user_key < end_key {
                    results.push(KeyValue {
                        key: user_key.to_vec(),
                        value,
                        create_revision: vv.create_revision,
                        mod_revision: vv.mod_revision,
                        lease_id: vv.lease_id,
                    });
                }
            }
        }

        Ok(results)
    }

    /// Get the latest VersionedValue for a key (internal use).
    fn get_latest_version_info(&self, key: &[u8]) -> Result<Option<(u64, VersionedValue)>> {
        let start = encode_key_prefix_start(key);
        let end = encode_key_prefix_end(key);

        let entries = self.engine.scan(&start, &end)?;
        if let Some((internal_key, raw_value)) = entries.into_iter().next() {
            let (_, rev) = decode_key(&internal_key);
            let vv = VersionedValue::decode(&raw_value);
            Ok(Some((rev, vv)))
        } else {
            Ok(None)
        }
    }

    /// Exposed for transactions: write a versioned value at a specific revision.
    pub(crate) fn put_versioned(
        &self,
        key: &[u8],
        vv: &VersionedValue,
        revision: u64,
    ) -> Result<()> {
        let internal_key = encode_key(key, revision);
        self.engine.put(&internal_key, &vv.encode())
    }

    /// Exposed for transactions: allocate a batch of revisions.
    pub(crate) fn allocate_revision(&self) -> u64 {
        self.next_revision()
    }

    /// Reference to the underlying engine.
    pub fn engine(&self) -> &Arc<dyn StorageEngine> {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_storage::lsm::{LsmConfig, LsmTree};

    fn test_store(dir: &std::path::Path) -> MvccStore {
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
        MvccStore::new(engine)
    }

    #[test]
    fn basic_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        let rev = store.put(b"key1", b"val1").unwrap();
        assert_eq!(rev, 1);

        let kv = store.get(b"key1").unwrap().unwrap();
        assert_eq!(kv.value, b"val1");
        assert_eq!(kv.create_revision, 1);
        assert_eq!(kv.mod_revision, 1);
    }

    #[test]
    fn overwrite_preserves_create_revision() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"key", b"v1").unwrap();
        let rev2 = store.put(b"key", b"v2").unwrap();

        let kv = store.get(b"key").unwrap().unwrap();
        assert_eq!(kv.value, b"v2");
        assert_eq!(kv.create_revision, 1); // preserved from first put
        assert_eq!(kv.mod_revision, rev2);
    }

    #[test]
    fn delete_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"key", b"val").unwrap();
        let (_, existed) = store.delete(b"key").unwrap();
        assert!(existed);

        assert!(store.get(b"key").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        let (_, existed) = store.delete(b"ghost").unwrap();
        assert!(!existed);
    }

    #[test]
    fn snapshot_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"key", b"v1").unwrap(); // rev 1
        store.put(b"key", b"v2").unwrap(); // rev 2
        store.put(b"key", b"v3").unwrap(); // rev 3

        // Read at revision 2 — should see v2
        let kv = store.get_at_revision(b"key", 2).unwrap().unwrap();
        assert_eq!(kv.value, b"v2");

        // Read at revision 1 — should see v1
        let kv = store.get_at_revision(b"key", 1).unwrap().unwrap();
        assert_eq!(kv.value, b"v1");

        // Read latest
        let kv = store.get(b"key").unwrap().unwrap();
        assert_eq!(kv.value, b"v3");
    }

    #[test]
    fn scan_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"2").unwrap();
        store.put(b"c", b"3").unwrap();
        store.put(b"d", b"4").unwrap();

        let results = store.scan(b"b", b"d").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, b"b");
        assert_eq!(results[1].key, b"c");
    }

    #[test]
    fn scan_after_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"a", b"1").unwrap();
        store.put(b"b", b"2").unwrap();
        store.put(b"c", b"3").unwrap();
        store.delete(b"b").unwrap();

        let results = store.scan(b"a", b"d").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, b"a");
        assert_eq!(results[1].key, b"c");
    }

    #[test]
    fn revision_increases() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        let r1 = store.put(b"a", b"1").unwrap();
        let r2 = store.put(b"b", b"2").unwrap();
        let (r3, _) = store.delete(b"a").unwrap();

        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
        assert_eq!(r3, 3);
        assert_eq!(store.revision(), 3);
    }
}
