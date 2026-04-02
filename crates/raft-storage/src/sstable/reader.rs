use raft_common::error::{Error, Result};
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;

use super::block::{BlockEntry, BlockReader};
use super::footer::{Footer, FOOTER_SIZE};

/// Index entry parsed from the index block.
#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    size: u64,
}

/// Reads key-value pairs from an SSTable file.
pub struct SSTableReader {
    data: Vec<u8>,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    path: PathBuf,
}

impl SSTableReader {
    /// Open and read an SSTable file.
    pub fn open(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        if data.len() < FOOTER_SIZE {
            return Err(Error::Corruption(format!(
                "SSTable too small: {} bytes",
                data.len()
            )));
        }

        // Parse footer
        let footer_start = data.len() - FOOTER_SIZE;
        let footer_bytes: &[u8; FOOTER_SIZE] = data[footer_start..].try_into().unwrap();
        let footer = Footer::decode(footer_bytes)?;

        // Parse index
        let index_start = footer.index_offset as usize;
        let index_end = index_start + footer.index_size as usize;
        if index_end > data.len() {
            return Err(Error::Corruption("index extends past file".to_string()));
        }
        let index_data = &data[index_start..index_end];
        let index = Self::parse_index(index_data)?;

        // Parse bloom filter
        let bloom_start = footer.bloom_offset as usize;
        let bloom_end = bloom_start + footer.bloom_size as usize;
        if bloom_end > data.len() {
            return Err(Error::Corruption(
                "bloom filter extends past file".to_string(),
            ));
        }
        let bloom_data = &data[bloom_start..bloom_end];
        let bloom = Self::parse_bloom(bloom_data, footer.bloom_num_hashes)?;

        Ok(Self {
            data,
            index,
            bloom,
            path: path.to_path_buf(),
        })
    }

    /// Get a value by key. Returns `Some(Some(value))` for live key,
    /// `Some(None)` for tombstone, `None` if not found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        // Check bloom filter first
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Find the block that could contain this key via index
        let block_idx = self.find_block(key);
        if block_idx >= self.index.len() {
            return Ok(None);
        }

        // Read and search the block
        let entry = &self.index[block_idx];
        let block_data = &self.data[entry.offset as usize..(entry.offset + entry.size) as usize];
        let entries = BlockReader::read_entries(block_data)?;

        match BlockReader::search(&entries, key) {
            Some(idx) => Ok(Some(entries[idx].value.clone())),
            None => Ok(None),
        }
    }

    /// Iterate over all entries in sorted key order.
    pub fn iter(&self) -> Result<Vec<BlockEntry>> {
        let mut all_entries = Vec::new();
        for idx_entry in &self.index {
            let block_data =
                &self.data[idx_entry.offset as usize..(idx_entry.offset + idx_entry.size) as usize];
            let entries = BlockReader::read_entries(block_data)?;
            all_entries.extend(entries);
        }
        Ok(all_entries)
    }

    /// Scan entries in range [start, end).
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<BlockEntry>> {
        let mut results = Vec::new();
        for entry in self.iter()? {
            if entry.key.as_slice() >= start && entry.key.as_slice() < end {
                results.push(entry);
            } else if entry.key.as_slice() >= end {
                break;
            }
        }
        Ok(results)
    }

    /// The smallest key in this SSTable.
    pub fn min_key(&self) -> Option<&[u8]> {
        self.index.first().map(|e| e.first_key.as_slice())
    }

    /// Path to this SSTable file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Find the block index that could contain the key.
    /// Uses binary search on the index entries' first_key.
    fn find_block(&self, key: &[u8]) -> usize {
        // Find the last block whose first_key <= key
        match self
            .index
            .binary_search_by(|e| e.first_key.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => 0, // key is before all blocks — still check first block
            Err(i) => i - 1,
        }
    }

    fn parse_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
        if data.len() < 4 {
            return Err(Error::Corruption("index block too small".to_string()));
        }

        let num_entries =
            u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut entries = Vec::with_capacity(num_entries);
        let mut offset = 4;

        for _ in 0..num_entries {
            if offset + 4 > data.len() {
                return Err(Error::Corruption("index entry truncated".to_string()));
            }
            let key_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len + 16 > data.len() {
                return Err(Error::Corruption("index entry truncated".to_string()));
            }
            let first_key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            let block_offset =
                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let block_size =
                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;

            entries.push(IndexEntry {
                first_key,
                offset: block_offset,
                size: block_size,
            });
        }

        Ok(entries)
    }

    fn parse_bloom(data: &[u8], num_hashes: u32) -> Result<BloomFilter> {
        if data.len() < 8 {
            return Err(Error::Corruption("bloom block too small".to_string()));
        }
        let num_bits = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let bloom_bytes = data[8..].to_vec();
        Ok(BloomFilter::from_raw(bloom_bytes, num_hashes, num_bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::writer::SSTableWriter;

    fn create_test_sstable(dir: &Path, entries: &[(&[u8], Option<&[u8]>)]) -> PathBuf {
        let path = dir.join("test.sst");
        let mut writer = SSTableWriter::new(&path, 256, entries.len());
        for (key, value) in entries {
            writer.add(key, *value);
        }
        writer.finish().unwrap();
        path
    }

    #[test]
    fn write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_test_sstable(
            dir.path(),
            &[
                (b"apple", Some(b"red")),
                (b"banana", Some(b"yellow")),
                (b"cherry", Some(b"dark red")),
            ],
        );

        let reader = SSTableReader::open(&path).unwrap();
        assert_eq!(
            reader.get(b"apple").unwrap(),
            Some(Some(b"red".to_vec()))
        );
        assert_eq!(
            reader.get(b"banana").unwrap(),
            Some(Some(b"yellow".to_vec()))
        );
        assert_eq!(reader.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_test_sstable(
            dir.path(),
            &[(b"alive", Some(b"yes")), (b"dead", None)],
        );

        let reader = SSTableReader::open(&path).unwrap();
        assert_eq!(
            reader.get(b"alive").unwrap(),
            Some(Some(b"yes".to_vec()))
        );
        assert_eq!(reader.get(b"dead").unwrap(), Some(None)); // tombstone
    }

    #[test]
    fn many_entries_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.sst");

        let n = 1000;
        let mut writer = SSTableWriter::new(&path, 256, n);
        for i in 0..n {
            let key = format!("key-{:06}", i);
            let val = format!("value-{:06}", i);
            writer.add(key.as_bytes(), Some(val.as_bytes()));
        }
        writer.finish().unwrap();

        let reader = SSTableReader::open(&path).unwrap();

        // Spot check some keys
        for i in [0, 100, 500, 999] {
            let key = format!("key-{:06}", i);
            let expected_val = format!("value-{:06}", i);
            assert_eq!(
                reader.get(key.as_bytes()).unwrap(),
                Some(Some(expected_val.into_bytes()))
            );
        }

        // Check a missing key
        assert_eq!(reader.get(b"key-999999").unwrap(), None);
    }

    #[test]
    fn iter_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_test_sstable(
            dir.path(),
            &[
                (b"a", Some(b"1")),
                (b"b", Some(b"2")),
                (b"c", Some(b"3")),
            ],
        );

        let reader = SSTableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"a");
        assert_eq!(entries[2].key, b"c");
    }

    #[test]
    fn scan_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scan.sst");
        let mut writer = SSTableWriter::new(&path, 4096, 10);
        for i in 0u8..10 {
            writer.add(&[i], Some(&[i * 10]));
        }
        writer.finish().unwrap();

        let reader = SSTableReader::open(&path).unwrap();
        let results = reader.scan(&[3], &[7]).unwrap();
        assert_eq!(results.len(), 4); // keys 3,4,5,6
        assert_eq!(results[0].key, vec![3]);
        assert_eq!(results[3].key, vec![6]);
    }
}
