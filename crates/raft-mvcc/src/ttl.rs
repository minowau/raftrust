use raft_common::error::Result;
use std::sync::Arc;

use crate::mvcc::MvccStore;
use crate::version::{decode_key, VersionedValue};

/// Manages TTL / key expiry.
///
/// Periodically sweeps all keys to find and delete expired ones.
/// In a production system this would use a more efficient index (e.g., a
/// min-heap of expiry times), but for correctness the sweep approach works.
pub struct TtlManager {
    store: Arc<MvccStore>,
}

impl TtlManager {
    pub fn new(store: Arc<MvccStore>) -> Self {
        Self { store }
    }

    /// Sweep all keys and delete any that have expired.
    /// `now_seconds` is the current time as seconds since the value was written
    /// (for simplicity, we use the revision as a proxy — in production this
    /// would use wall-clock time stored in the VersionedValue).
    ///
    /// Returns the number of keys expired.
    pub fn sweep_expired(&self, now_secs: i64) -> Result<usize> {
        // Scan all keys by using the widest possible range
        let start = vec![0u8];
        let end = vec![0xFF; 32]; // effectively max key

        let all_entries = self.store.engine().scan(&start, &end)?;

        let mut expired_keys = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        for (internal_key, raw_value) in all_entries {
            let (user_key, _rev) = decode_key(&internal_key);

            // Only check the latest version of each key
            if let Some(ref last) = last_user_key {
                if last.as_slice() == user_key {
                    continue;
                }
            }
            last_user_key = Some(user_key.to_vec());

            let vv = VersionedValue::decode(&raw_value);
            if vv.value.is_some() && vv.ttl_seconds > 0 {
                // Check if expired: mod_revision + ttl < now
                // (Using mod_revision as a proxy for write time in seconds)
                let expiry = vv.mod_revision as i64 + vv.ttl_seconds;
                if expiry <= now_secs {
                    expired_keys.push(user_key.to_vec());
                }
            }
        }

        let count = expired_keys.len();
        for key in expired_keys {
            self.store.delete(&key)?;
        }

        Ok(count)
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
    fn expire_ttl_keys() {
        let dir = tempfile::tempdir().unwrap();
        let store = test_store(dir.path());

        // Put with TTL of 5 "seconds" (using revision as time proxy)
        store.put_with_options(b"expires", b"soon", 0, 5).unwrap(); // rev 1, expires at 1+5=6
        store.put(b"permanent", b"forever").unwrap(); // rev 2, no TTL

        let ttl = TtlManager::new(store.clone());

        // At "time" 3, nothing should expire
        let expired = ttl.sweep_expired(3).unwrap();
        assert_eq!(expired, 0);
        assert!(store.get(b"expires").unwrap().is_some());

        // At "time" 7, the key should expire
        let expired = ttl.sweep_expired(7).unwrap();
        assert_eq!(expired, 1);
        assert!(store.get(b"expires").unwrap().is_none());
        assert!(store.get(b"permanent").unwrap().is_some());
    }
}
