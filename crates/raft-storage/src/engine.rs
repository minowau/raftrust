use raft_common::error::Result;

/// Core storage engine trait. All higher layers (MVCC, Raft log) build on this.
pub trait StorageEngine: Send + Sync + 'static {
    /// Get the value for a key. Returns None if the key does not exist.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Set a key to a value. Overwrites any existing value.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key. No-op if the key does not exist.
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Scan all keys in the range [start, end). Returns sorted key-value pairs.
    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Force flush the active MemTable to an SSTable on disk.
    fn flush(&self) -> Result<()>;
}
