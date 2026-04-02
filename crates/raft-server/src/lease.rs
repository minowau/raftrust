use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use parking_lot::RwLock;
use tracing::{debug, info};

/// A lease with a TTL that must be periodically renewed.
///
/// Leases are used for:
/// - Key expiry: keys attached to a lease are deleted when it expires
/// - Distributed locks: a client holds a lease; if it dies, the lease expires
///   and the lock is released
#[derive(Debug, Clone)]
pub struct Lease {
    pub id: i64,
    /// The granted TTL in seconds.
    pub granted_ttl: i64,
    /// When this lease was last renewed (or created).
    pub last_renewed: Instant,
    /// Keys attached to this lease.
    pub keys: HashSet<Vec<u8>>,
}

impl Lease {
    pub fn new(id: i64, ttl: i64) -> Self {
        Self {
            id,
            granted_ttl: ttl,
            last_renewed: Instant::now(),
            keys: HashSet::new(),
        }
    }

    /// Remaining TTL in seconds (0 if expired).
    pub fn remaining_ttl(&self) -> i64 {
        let elapsed = self.last_renewed.elapsed().as_secs() as i64;
        (self.granted_ttl - elapsed).max(0)
    }

    /// Whether this lease has expired.
    pub fn is_expired(&self) -> bool {
        self.remaining_ttl() == 0
    }

    /// Renew the lease (reset the TTL timer).
    pub fn renew(&mut self) {
        self.last_renewed = Instant::now();
    }

    /// Attach a key to this lease.
    pub fn attach_key(&mut self, key: Vec<u8>) {
        self.keys.insert(key);
    }

    /// Detach a key from this lease.
    pub fn detach_key(&mut self, key: &[u8]) {
        self.keys.remove(key);
    }
}

/// Manages all active leases.
///
/// The lease manager is responsible for:
/// 1. Granting new leases with a TTL
/// 2. Processing keepalive requests to renew leases
/// 3. Revoking leases (deleting attached keys)
/// 4. Detecting expired leases and cleaning up
pub struct LeaseManager {
    leases: RwLock<HashMap<i64, Lease>>,
    next_id: AtomicI64,
}

impl LeaseManager {
    pub fn new() -> Self {
        Self {
            leases: RwLock::new(HashMap::new()),
            next_id: AtomicI64::new(1),
        }
    }

    /// Grant a new lease. If `id` is 0, auto-assign an ID.
    /// Returns the lease ID and granted TTL.
    pub fn grant(&self, id: i64, ttl: i64) -> Result<(i64, i64), LeaseError> {
        if ttl <= 0 {
            return Err(LeaseError::InvalidTtl);
        }

        let lease_id = if id == 0 {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        } else {
            // Check for duplicate
            if self.leases.read().contains_key(&id) {
                return Err(LeaseError::AlreadyExists(id));
            }
            // Advance next_id past this ID
            let _ = self.next_id.fetch_max(id + 1, Ordering::SeqCst);
            id
        };

        let lease = Lease::new(lease_id, ttl);
        self.leases.write().insert(lease_id, lease);

        info!(lease_id = lease_id, ttl = ttl, "Granted lease");
        Ok((lease_id, ttl))
    }

    /// Renew a lease (keepalive). Returns the new TTL.
    pub fn keepalive(&self, id: i64) -> Result<i64, LeaseError> {
        let mut leases = self.leases.write();
        let lease = leases.get_mut(&id).ok_or(LeaseError::NotFound(id))?;

        if lease.is_expired() {
            return Err(LeaseError::Expired(id));
        }

        lease.renew();
        debug!(lease_id = id, ttl = lease.granted_ttl, "Lease renewed");
        Ok(lease.granted_ttl)
    }

    /// Revoke a lease. Returns the keys that were attached to it (to be deleted).
    pub fn revoke(&self, id: i64) -> Result<Vec<Vec<u8>>, LeaseError> {
        let mut leases = self.leases.write();
        let lease = leases.remove(&id).ok_or(LeaseError::NotFound(id))?;

        let keys: Vec<Vec<u8>> = lease.keys.into_iter().collect();
        info!(lease_id = id, keys_count = keys.len(), "Revoked lease");
        Ok(keys)
    }

    /// Get lease info (TTL, keys).
    pub fn get(&self, id: i64) -> Result<LeaseInfo, LeaseError> {
        let leases = self.leases.read();
        let lease = leases.get(&id).ok_or(LeaseError::NotFound(id))?;

        Ok(LeaseInfo {
            id: lease.id,
            ttl: lease.remaining_ttl(),
            granted_ttl: lease.granted_ttl,
            keys: lease.keys.iter().cloned().collect(),
        })
    }

    /// Attach a key to a lease.
    pub fn attach_key(&self, lease_id: i64, key: Vec<u8>) -> Result<(), LeaseError> {
        let mut leases = self.leases.write();
        let lease = leases
            .get_mut(&lease_id)
            .ok_or(LeaseError::NotFound(lease_id))?;
        lease.attach_key(key);
        Ok(())
    }

    /// Detach a key from a lease.
    pub fn detach_key(&self, lease_id: i64, key: &[u8]) {
        let mut leases = self.leases.write();
        if let Some(lease) = leases.get_mut(&lease_id) {
            lease.detach_key(key);
        }
    }

    /// Find and collect all expired leases.
    /// Returns a list of (lease_id, attached_keys) for each expired lease.
    pub fn collect_expired(&self) -> Vec<(i64, Vec<Vec<u8>>)> {
        let mut leases = self.leases.write();
        let mut expired = Vec::new();

        let expired_ids: Vec<i64> = leases
            .iter()
            .filter(|(_, l)| l.is_expired())
            .map(|(&id, _)| id)
            .collect();

        for id in expired_ids {
            if let Some(lease) = leases.remove(&id) {
                let keys: Vec<Vec<u8>> = lease.keys.into_iter().collect();
                info!(lease_id = id, keys_count = keys.len(), "Lease expired");
                expired.push((id, keys));
            }
        }

        expired
    }

    /// Number of active leases.
    pub fn lease_count(&self) -> usize {
        self.leases.read().len()
    }

    /// Check if a lease exists and is not expired.
    pub fn is_alive(&self, id: i64) -> bool {
        self.leases.read().get(&id).is_some_and(|l| !l.is_expired())
    }
}

impl Default for LeaseManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a lease.
#[derive(Debug, Clone)]
pub struct LeaseInfo {
    pub id: i64,
    pub ttl: i64,
    pub granted_ttl: i64,
    pub keys: Vec<Vec<u8>>,
}

/// Errors from lease operations.
#[derive(Debug, PartialEq, Eq)]
pub enum LeaseError {
    NotFound(i64),
    AlreadyExists(i64),
    Expired(i64),
    InvalidTtl,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_and_get() {
        let mgr = LeaseManager::new();
        let (id, ttl) = mgr.grant(0, 60).unwrap();
        assert!(id > 0);
        assert_eq!(ttl, 60);

        let info = mgr.get(id).unwrap();
        assert_eq!(info.granted_ttl, 60);
        assert!(info.ttl > 0);
        assert!(info.keys.is_empty());
    }

    #[test]
    fn grant_with_specific_id() {
        let mgr = LeaseManager::new();
        let (id, _) = mgr.grant(42, 60).unwrap();
        assert_eq!(id, 42);
        assert!(mgr.is_alive(42));
    }

    #[test]
    fn reject_duplicate_id() {
        let mgr = LeaseManager::new();
        mgr.grant(42, 60).unwrap();
        let result = mgr.grant(42, 30);
        assert_eq!(result, Err(LeaseError::AlreadyExists(42)));
    }

    #[test]
    fn reject_invalid_ttl() {
        let mgr = LeaseManager::new();
        assert_eq!(mgr.grant(0, 0), Err(LeaseError::InvalidTtl));
        assert_eq!(mgr.grant(0, -5), Err(LeaseError::InvalidTtl));
    }

    #[test]
    fn keepalive_renews() {
        let mgr = LeaseManager::new();
        let (id, _) = mgr.grant(0, 60).unwrap();

        let ttl = mgr.keepalive(id).unwrap();
        assert_eq!(ttl, 60);
        assert!(mgr.is_alive(id));
    }

    #[test]
    fn keepalive_not_found() {
        let mgr = LeaseManager::new();
        assert_eq!(mgr.keepalive(99), Err(LeaseError::NotFound(99)));
    }

    #[test]
    fn revoke_returns_keys() {
        let mgr = LeaseManager::new();
        let (id, _) = mgr.grant(0, 60).unwrap();

        mgr.attach_key(id, b"key1".to_vec()).unwrap();
        mgr.attach_key(id, b"key2".to_vec()).unwrap();

        let keys = mgr.revoke(id).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(!mgr.is_alive(id));
    }

    #[test]
    fn revoke_not_found() {
        let mgr = LeaseManager::new();
        assert_eq!(mgr.revoke(99), Err(LeaseError::NotFound(99)));
    }

    #[test]
    fn attach_and_detach_keys() {
        let mgr = LeaseManager::new();
        let (id, _) = mgr.grant(0, 60).unwrap();

        mgr.attach_key(id, b"key1".to_vec()).unwrap();
        mgr.attach_key(id, b"key2".to_vec()).unwrap();

        let info = mgr.get(id).unwrap();
        assert_eq!(info.keys.len(), 2);

        mgr.detach_key(id, b"key1");
        let info = mgr.get(id).unwrap();
        assert_eq!(info.keys.len(), 1);
    }

    #[test]
    fn lease_expiry() {
        let mgr = LeaseManager::new();
        // Grant a 1-second lease
        let (id, _) = mgr.grant(0, 1).unwrap();
        assert!(mgr.is_alive(id));

        // Sleep past TTL
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(!mgr.is_alive(id));

        // Collect expired
        let expired = mgr.collect_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, id);
        assert_eq!(mgr.lease_count(), 0);
    }

    #[test]
    fn keepalive_prevents_expiry() {
        let mgr = LeaseManager::new();
        let (id, _) = mgr.grant(0, 2).unwrap();

        // Wait 1 second, then renew
        std::thread::sleep(std::time::Duration::from_millis(1000));
        assert!(mgr.is_alive(id));
        mgr.keepalive(id).unwrap();

        // Wait another 1 second — should still be alive (renewed)
        std::thread::sleep(std::time::Duration::from_millis(1000));
        assert!(mgr.is_alive(id));
    }

    #[test]
    fn expired_lease_keys_collected() {
        let mgr = LeaseManager::new();
        let (id, _) = mgr.grant(0, 1).unwrap();
        mgr.attach_key(id, b"locked-resource".to_vec()).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1100));

        let expired = mgr.collect_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].1, vec![b"locked-resource".to_vec()]);
    }

    #[test]
    fn multiple_leases() {
        let mgr = LeaseManager::new();
        let (id1, _) = mgr.grant(0, 60).unwrap();
        let (id2, _) = mgr.grant(0, 60).unwrap();
        assert_ne!(id1, id2);
        assert_eq!(mgr.lease_count(), 2);

        mgr.revoke(id1).unwrap();
        assert_eq!(mgr.lease_count(), 1);
        assert!(!mgr.is_alive(id1));
        assert!(mgr.is_alive(id2));
    }

    #[test]
    fn keepalive_expired_lease_fails() {
        let mgr = LeaseManager::new();
        let (id, _) = mgr.grant(0, 1).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1100));

        let result = mgr.keepalive(id);
        assert_eq!(result, Err(LeaseError::Expired(id)));
    }
}
