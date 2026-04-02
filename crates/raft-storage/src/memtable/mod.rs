use std::collections::BTreeMap;

/// Tombstone value indicating a deleted key.
const TOMBSTONE: Option<Vec<u8>> = None;

/// In-memory sorted key-value store backed by a BTreeMap.
///
/// Supports tombstone markers (None values) for deletes.
/// The MemTable is the write-target for the LSM-tree; when it exceeds
/// `max_size` bytes, it becomes immutable and is flushed to an SSTable.
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

    /// Get the value for a key.
    /// Returns `Some(Some(value))` for a live key,
    /// `Some(None)` for a tombstone (deleted key),
    /// `None` if the key was never written to this MemTable.
    pub fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.data.get(key).map(|v| v.as_deref())
    }

    /// Insert or update a key-value pair.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry_size = key.len() + value.len();
        if let Some(old) = self.data.insert(key, Some(value)) {
            // Subtract old value size, add new
            let old_size = old.as_ref().map_or(0, |v| v.len());
            self.size_bytes = self.size_bytes - old_size + entry_size
                - (entry_size - old_size.min(entry_size));
            // Simplified: just recalculate
        } else {
            self.size_bytes += entry_size;
        }
        // Recalculate to keep it accurate (avoid cumulative drift)
        self.recalculate_size();
    }

    /// Delete a key by writing a tombstone.
    pub fn delete(&mut self, key: Vec<u8>) {
        let key_len = key.len();
        if let Some(old) = self.data.insert(key, TOMBSTONE) {
            let old_size = old.as_ref().map_or(0, |v| v.len());
            self.size_bytes = self.size_bytes.saturating_sub(old_size);
        } else {
            self.size_bytes += key_len;
        }
    }

    /// Iterate over all entries in sorted key order.
    /// Yields `(key, Option<value>)` where None means tombstone.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.data
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
    }

    /// Scan keys in range [start, end) in sorted order.
    pub fn scan<'a>(
        &'a self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)> {
        use std::ops::Bound;
        self.data
            .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
            .map(|(k, v)| (k.as_slice(), v.as_deref()))
    }

    /// Whether this MemTable has reached its size limit.
    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size
    }

    /// Approximate memory usage in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn recalculate_size(&mut self) {
        self.size_bytes = self
            .data
            .iter()
            .map(|(k, v)| k.len() + v.as_ref().map_or(0, |v| v.len()))
            .sum();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_put_get() {
        let mut mt = MemTable::new(1024);
        mt.put(b"key1".to_vec(), b"val1".to_vec());
        mt.put(b"key2".to_vec(), b"val2".to_vec());

        assert_eq!(mt.get(b"key1"), Some(Some(b"val1".as_slice())));
        assert_eq!(mt.get(b"key2"), Some(Some(b"val2".as_slice())));
        assert_eq!(mt.get(b"key3"), None);
    }

    #[test]
    fn overwrite() {
        let mut mt = MemTable::new(1024);
        mt.put(b"key".to_vec(), b"old".to_vec());
        mt.put(b"key".to_vec(), b"new".to_vec());

        assert_eq!(mt.get(b"key"), Some(Some(b"new".as_slice())));
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn delete_tombstone() {
        let mut mt = MemTable::new(1024);
        mt.put(b"key".to_vec(), b"val".to_vec());
        mt.delete(b"key".to_vec());

        // Tombstone is present — distinguishable from "never written"
        assert_eq!(mt.get(b"key"), Some(None));
        assert_eq!(mt.get(b"other"), None);
    }

    #[test]
    fn delete_nonexistent() {
        let mut mt = MemTable::new(1024);
        mt.delete(b"ghost".to_vec());

        assert_eq!(mt.get(b"ghost"), Some(None));
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn sorted_iteration() {
        let mut mt = MemTable::new(1024);
        mt.put(b"c".to_vec(), b"3".to_vec());
        mt.put(b"a".to_vec(), b"1".to_vec());
        mt.put(b"b".to_vec(), b"2".to_vec());

        let keys: Vec<&[u8]> = mt.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn scan_range() {
        let mut mt = MemTable::new(1024);
        for i in 0u8..10 {
            mt.put(vec![i], vec![i * 10]);
        }

        let results: Vec<(u8, u8)> = mt
            .scan(&[3], &[7])
            .map(|(k, v)| (k[0], v.unwrap()[0]))
            .collect();

        assert_eq!(results, vec![(3, 30), (4, 40), (5, 50), (6, 60)]);
    }

    #[test]
    fn is_full() {
        let mut mt = MemTable::new(10);
        assert!(!mt.is_full());

        mt.put(b"12345".to_vec(), b"67890".to_vec()); // 10 bytes
        assert!(mt.is_full());
    }

    #[test]
    fn size_tracking_with_overwrites() {
        let mut mt = MemTable::new(1024);
        mt.put(b"key".to_vec(), b"short".to_vec());
        let size1 = mt.size_bytes();

        mt.put(b"key".to_vec(), b"a longer value".to_vec());
        let size2 = mt.size_bytes();

        assert!(size2 > size1);
    }

    #[test]
    fn iter_includes_tombstones() {
        let mut mt = MemTable::new(1024);
        mt.put(b"a".to_vec(), b"1".to_vec());
        mt.put(b"b".to_vec(), b"2".to_vec());
        mt.delete(b"a".to_vec());

        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"a".as_slice(), None)); // tombstone
        assert_eq!(entries[1], (b"b".as_slice(), Some(b"2".as_slice())));
    }
}
