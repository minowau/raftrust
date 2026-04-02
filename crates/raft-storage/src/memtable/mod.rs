use std::collections::BTreeMap;

/// In-memory sorted key-value store backed by a BTreeMap.
/// Supports tombstone markers (None values) for deletes.
pub struct MemTable {
    data: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    size_bytes: usize,
    max_size: usize,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
            max_size,
        }
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
}
