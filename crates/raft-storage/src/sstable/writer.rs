use raft_common::error::Result;
use std::path::{Path, PathBuf};

use crate::bloom::BloomFilter;

use super::block::BlockBuilder;
use super::footer::Footer;

/// Index entry: maps a block's first key to its offset and size in the file.
#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    size: u64,
}

/// Writes sorted key-value pairs into an SSTable file.
///
/// File layout:
/// ```text
/// [data block 0][data block 1]...[data block N]
/// [index block]
/// [bloom filter bytes]
/// [footer: 40 bytes]
/// ```
pub struct SSTableWriter {
    path: PathBuf,
    block_size: usize,
    current_block: BlockBuilder,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    data: Vec<u8>,
    entry_count: usize,
}

impl SSTableWriter {
    pub fn new(path: &Path, block_size: usize, expected_entries: usize) -> Self {
        Self {
            path: path.to_path_buf(),
            block_size,
            current_block: BlockBuilder::new(),
            index: Vec::new(),
            bloom: BloomFilter::new(expected_entries.max(1), 0.01),
            data: Vec::new(),
            entry_count: 0,
        }
    }

    /// Add a key-value pair. Must be called in sorted key order.
    /// `value` of None means tombstone.
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) {
        self.bloom.insert(key);
        self.current_block.add(key, value);
        self.entry_count += 1;

        if self.current_block.estimated_size() >= self.block_size {
            self.flush_block();
        }
    }

    /// Finalize and write the SSTable to disk.
    pub fn finish(mut self) -> Result<SSTableInfo> {
        // Flush any remaining entries
        if !self.current_block.is_empty() {
            self.flush_block();
        }

        // Build index block: [num_entries: 4][entries...]
        // Each index entry: [key_len: 4][key][offset: 8][size: 8]
        let index_offset = self.data.len() as u64;
        let mut index_data = Vec::new();
        let num_index_entries = self.index.len() as u32;
        index_data.extend_from_slice(&num_index_entries.to_le_bytes());
        for entry in &self.index {
            let key_len = entry.first_key.len() as u32;
            index_data.extend_from_slice(&key_len.to_le_bytes());
            index_data.extend_from_slice(&entry.first_key);
            index_data.extend_from_slice(&entry.offset.to_le_bytes());
            index_data.extend_from_slice(&entry.size.to_le_bytes());
        }
        let index_size = index_data.len() as u64;
        self.data.extend_from_slice(&index_data);

        // Write bloom filter
        let bloom_offset = self.data.len() as u64;
        let bloom_bytes = self.bloom.as_bytes();
        let bloom_num_bits = self.bloom.num_bits() as u64;
        // Store: [num_bits: 8][bloom_bytes]
        self.data.extend_from_slice(&bloom_num_bits.to_le_bytes());
        self.data.extend_from_slice(bloom_bytes);
        let bloom_size = 8 + bloom_bytes.len() as u64;

        // Write footer
        let footer = Footer {
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            bloom_num_hashes: self.bloom.num_hashes(),
        };
        self.data.extend_from_slice(&footer.encode());

        // Write entire file atomically
        std::fs::write(&self.path, &self.data)?;

        Ok(SSTableInfo {
            path: self.path,
            entry_count: self.entry_count,
            file_size: self.data.len() as u64,
        })
    }

    fn flush_block(&mut self) {
        let first_key = self
            .current_block
            .first_key()
            .map(|k| k.to_vec())
            .unwrap_or_default();

        let block = std::mem::take(&mut self.current_block);
        let block_data = block.build();
        let offset = self.data.len() as u64;
        let size = block_data.len() as u64;

        self.data.extend_from_slice(&block_data);
        self.index.push(IndexEntry {
            first_key,
            offset,
            size,
        });
    }
}

#[derive(Debug)]
pub struct SSTableInfo {
    pub path: PathBuf,
    pub entry_count: usize,
    pub file_size: u64,
}
