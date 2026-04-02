use raft_common::error::{Error, Result};

/// A data block within an SSTable.
///
/// On-disk format per entry: `[key_len: 4 LE][val_len: 4 LE][key][value]`
/// Entries are stored in sorted key order. A value with `val_len == u32::MAX`
/// represents a tombstone (deleted key).
///
/// Block trailer: `[num_entries: 4 LE]`

pub const TOMBSTONE_MARKER: u32 = u32::MAX;

#[derive(Debug, Clone)]
pub struct BlockEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // None = tombstone
}

/// Builds a block by accumulating sorted entries.
pub struct BlockBuilder {
    entries: Vec<BlockEntry>,
    size: usize,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            size: 4, // trailer: num_entries
        }
    }

    /// Add an entry. Caller must add entries in sorted key order.
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) {
        let entry_size = 4 + 4 + key.len() + value.map_or(0, |v| v.len());
        self.size += entry_size;
        self.entries.push(BlockEntry {
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
        });
    }

    pub fn estimated_size(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The first key in this block (for index).
    pub fn first_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|e| e.key.as_slice())
    }

    /// Encode the block to bytes.
    pub fn build(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.size);
        for entry in &self.entries {
            let key_len = entry.key.len() as u32;
            let val_len = match &entry.value {
                Some(v) => v.len() as u32,
                None => TOMBSTONE_MARKER,
            };
            buf.extend_from_slice(&key_len.to_le_bytes());
            buf.extend_from_slice(&val_len.to_le_bytes());
            buf.extend_from_slice(&entry.key);
            if let Some(v) = &entry.value {
                buf.extend_from_slice(v);
            }
        }
        let num_entries = self.entries.len() as u32;
        buf.extend_from_slice(&num_entries.to_le_bytes());
        buf
    }
}

/// Reads entries from a serialized block.
pub struct BlockReader;

impl BlockReader {
    /// Parse all entries from block bytes.
    pub fn read_entries(data: &[u8]) -> Result<Vec<BlockEntry>> {
        if data.len() < 4 {
            return Err(Error::Corruption("block too small".to_string()));
        }

        let num_entries = u32::from_le_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]) as usize;

        let mut entries = Vec::with_capacity(num_entries);
        let mut offset = 0;
        let payload_end = data.len() - 4;

        for _ in 0..num_entries {
            if offset + 8 > payload_end {
                return Err(Error::Corruption("block entry truncated".to_string()));
            }
            let key_len =
                u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
                    as usize;
            let val_len_raw = u32::from_le_bytes([
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;

            if offset + key_len > payload_end {
                return Err(Error::Corruption("block key truncated".to_string()));
            }
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            let value = if val_len_raw == TOMBSTONE_MARKER {
                None
            } else {
                let val_len = val_len_raw as usize;
                if offset + val_len > payload_end {
                    return Err(Error::Corruption("block value truncated".to_string()));
                }
                let v = data[offset..offset + val_len].to_vec();
                offset += val_len;
                Some(v)
            };

            entries.push(BlockEntry { key, value });
        }

        Ok(entries)
    }

    /// Binary search for a key within block entries.
    pub fn search(entries: &[BlockEntry], key: &[u8]) -> Option<usize> {
        entries
            .binary_search_by(|e| e.key.as_slice().cmp(key))
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut builder = BlockBuilder::new();
        builder.add(b"apple", Some(b"red"));
        builder.add(b"banana", Some(b"yellow"));
        builder.add(b"cherry", None); // tombstone
        builder.add(b"date", Some(b"brown"));

        let data = builder.build();
        let entries = BlockReader::read_entries(&data).unwrap();

        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].key, b"apple");
        assert_eq!(entries[0].value.as_deref(), Some(b"red".as_slice()));
        assert_eq!(entries[2].key, b"cherry");
        assert!(entries[2].value.is_none());
    }

    #[test]
    fn binary_search() {
        let mut builder = BlockBuilder::new();
        for i in 0..100 {
            builder.add(format!("key-{:04}", i).as_bytes(), Some(b"val"));
        }

        let data = builder.build();
        let entries = BlockReader::read_entries(&data).unwrap();

        assert_eq!(BlockReader::search(&entries, b"key-0050"), Some(50));
        assert!(BlockReader::search(&entries, b"nonexistent").is_none());
    }

    #[test]
    fn empty_block() {
        let builder = BlockBuilder::new();
        let data = builder.build();
        let entries = BlockReader::read_entries(&data).unwrap();
        assert!(entries.is_empty());
    }
}
