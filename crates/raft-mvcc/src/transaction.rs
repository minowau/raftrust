use raft_common::error::{Error, Result};
use std::sync::Arc;

use crate::mvcc::MvccStore;
use crate::version::VersionedValue;

/// Optimistic concurrency control transaction.
///
/// Collects a read-set and write-set during execution. At commit time,
/// validates that no key in the read-set was modified by another transaction
/// since it was read. If validation passes, all writes are applied atomically
/// at a single revision. If it fails, the transaction aborts with a conflict error.
pub struct Transaction {
    store: Arc<MvccStore>,
    /// Revision at which this transaction started (snapshot boundary).
    start_revision: u64,
    /// Keys read during this transaction, with the revision they were read at.
    read_set: Vec<(Vec<u8>, u64)>,
    /// Buffered writes: key -> Some(value) for put, None for delete.
    write_set: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl Transaction {
    pub fn begin(store: Arc<MvccStore>) -> Self {
        let start_revision = store.revision();
        Self {
            store,
            start_revision,
            read_set: Vec::new(),
            write_set: Vec::new(),
        }
    }

    /// Read a key within this transaction's snapshot.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check local write buffer first
        for (k, v) in self.write_set.iter().rev() {
            if k == key {
                return Ok(v.clone());
            }
        }

        // Read from store at snapshot revision
        let result = self.store.get_at_revision(key, self.start_revision)?;
        let mod_revision = result.as_ref().map_or(0, |kv| kv.mod_revision);
        self.read_set.push((key.to_vec(), mod_revision));

        Ok(result.map(|kv| kv.value))
    }

    /// Buffer a put operation.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.write_set.push((key, Some(value)));
    }

    /// Buffer a delete operation.
    pub fn delete(&mut self, key: Vec<u8>) {
        self.write_set.push((key, None));
    }

    /// Validate and commit the transaction atomically.
    /// Returns the commit revision on success.
    pub fn commit(self) -> Result<u64> {
        if self.write_set.is_empty() {
            return Ok(self.start_revision); // Read-only transaction, nothing to commit
        }

        // Validation phase: check that no key in the read-set was modified
        // since we read it. We need to check the current latest version.
        for (key, read_revision) in &self.read_set {
            let current = self.store.get(key)?;
            let current_mod_revision = current.as_ref().map_or(0, |kv| kv.mod_revision);

            if current_mod_revision != *read_revision {
                return Err(Error::TransactionConflict);
            }
        }

        // Commit phase: apply all writes at a single revision
        let commit_revision = self.store.allocate_revision();

        for (key, value) in &self.write_set {
            let create_revision = if value.is_some() {
                // Check if key previously existed
                match self.store.get(key)? {
                    Some(kv) => kv.create_revision,
                    None => commit_revision,
                }
            } else {
                0
            };

            let vv = VersionedValue {
                value: value.clone(),
                create_revision,
                mod_revision: commit_revision,
                lease_id: 0,
                ttl_seconds: 0,
            };

            self.store.put_versioned(key, &vv, commit_revision)?;
        }

        Ok(commit_revision)
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
    fn basic_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        let mut txn = Transaction::begin(store.clone());
        txn.put(b"key1".to_vec(), b"val1".to_vec());
        txn.put(b"key2".to_vec(), b"val2".to_vec());
        let rev = txn.commit().unwrap();
        assert!(rev > 0);

        assert_eq!(store.get(b"key1").unwrap().unwrap().value, b"val1");
        assert_eq!(store.get(b"key2").unwrap().unwrap().value, b"val2");
    }

    #[test]
    fn read_own_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        let mut txn = Transaction::begin(store.clone());
        txn.put(b"key".to_vec(), b"val".to_vec());

        // Should see our buffered write
        let result = txn.get(b"key").unwrap();
        assert_eq!(result, Some(b"val".to_vec()));

        txn.commit().unwrap();
    }

    #[test]
    fn conflict_detection() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        // Write initial value
        store.put(b"key", b"v1").unwrap();

        // Start transaction, read key
        let mut txn = Transaction::begin(store.clone());
        txn.get(b"key").unwrap();
        txn.put(b"key".to_vec(), b"txn-value".to_vec());

        // Concurrent write modifies the same key
        store.put(b"key", b"v2").unwrap();

        // Transaction should fail due to conflict
        let result = txn.commit();
        assert!(matches!(result, Err(Error::TransactionConflict)));
    }

    #[test]
    fn no_conflict_on_different_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"key-a", b"v1").unwrap();
        store.put(b"key-b", b"v1").unwrap();

        // Transaction reads key-a, writes key-a
        let mut txn = Transaction::begin(store.clone());
        txn.get(b"key-a").unwrap();
        txn.put(b"key-a".to_vec(), b"txn-value".to_vec());

        // Concurrent write modifies key-b (not in read-set)
        store.put(b"key-b", b"v2").unwrap();

        // Should succeed — no conflict
        txn.commit().unwrap();
    }

    #[test]
    fn read_only_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"key", b"val").unwrap();

        let mut txn = Transaction::begin(store.clone());
        let val = txn.get(b"key").unwrap();
        assert_eq!(val, Some(b"val".to_vec()));

        // Read-only commit returns start revision
        let rev = txn.commit().unwrap();
        assert_eq!(rev, 1); // start_revision was 1 after the put
    }

    #[test]
    fn transaction_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        store.put(b"key", b"val").unwrap();

        let mut txn = Transaction::begin(store.clone());
        txn.delete(b"key".to_vec());
        txn.commit().unwrap();

        assert!(store.get(b"key").unwrap().is_none());
    }

    #[test]
    fn multi_key_atomicity() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        // All writes in a transaction get the same revision
        let mut txn = Transaction::begin(store.clone());
        txn.put(b"a".to_vec(), b"1".to_vec());
        txn.put(b"b".to_vec(), b"2".to_vec());
        txn.put(b"c".to_vec(), b"3".to_vec());
        let rev = txn.commit().unwrap();

        let a = store.get(b"a").unwrap().unwrap();
        let b = store.get(b"b").unwrap().unwrap();
        let c = store.get(b"c").unwrap().unwrap();

        // All should have the same mod_revision
        assert_eq!(a.mod_revision, rev);
        assert_eq!(b.mod_revision, rev);
        assert_eq!(c.mod_revision, rev);
    }
}
