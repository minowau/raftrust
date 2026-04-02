use raft_common::error::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Tracks which SSTables exist at each level of the LSM-tree.
/// Persisted as JSON for crash recovery. The manifest is the
/// source of truth for the LSM-tree's structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// SSTables organized by level. Level 0 may have overlapping key ranges.
    /// Levels 1+ have non-overlapping, sorted key ranges.
    pub levels: Vec<Vec<SSTableEntry>>,
    /// Monotonically increasing counter for generating unique SSTable filenames.
    pub next_sst_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableEntry {
    pub id: u64,
    pub level: usize,
    pub file_name: String,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub entry_count: usize,
    pub file_size: u64,
}

impl Manifest {
    pub fn new(num_levels: usize) -> Self {
        Self {
            levels: (0..num_levels).map(|_| Vec::new()).collect(),
            next_sst_id: 0,
        }
    }

    /// Generate the next SSTable filename and increment the ID counter.
    pub fn next_sst_filename(&mut self) -> (u64, String) {
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        (id, format!("{:08}.sst", id))
    }

    /// Add an SSTable entry to a level.
    pub fn add_sstable(&mut self, entry: SSTableEntry) {
        let level = entry.level;
        if level >= self.levels.len() {
            self.levels.resize_with(level + 1, Vec::new);
        }
        self.levels[level].push(entry);
    }

    /// Remove SSTable entries by their IDs.
    pub fn remove_sstables(&mut self, ids: &[u64]) {
        for level in &mut self.levels {
            level.retain(|e| !ids.contains(&e.id));
        }
    }

    /// Get all SSTable entries at a given level.
    pub fn level(&self, level: usize) -> &[SSTableEntry] {
        self.levels.get(level).map_or(&[], |l| l.as_slice())
    }

    /// Number of SSTables at a given level.
    pub fn level_count(&self, level: usize) -> usize {
        self.levels.get(level).map_or(0, |l| l.len())
    }

    /// Total number of levels.
    pub fn num_levels(&self) -> usize {
        self.levels.len()
    }

    /// Load manifest from a JSON file. Returns a new empty manifest if file doesn't exist.
    pub fn load(path: &Path, num_levels: usize) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new(num_levels));
        }
        let data = std::fs::read_to_string(path)?;
        let manifest: Manifest = serde_json::from_str(&data)
            .map_err(|e| raft_common::error::Error::Corruption(format!("manifest parse error: {}", e)))?;
        Ok(manifest)
    }

    /// Save manifest to a JSON file. Uses write-rename for atomicity.
    pub fn save(&self, path: &Path) -> Result<()> {
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| raft_common::error::Error::Storage(format!("manifest serialize error: {}", e)))?;
        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_manifest() {
        let m = Manifest::new(7);
        assert_eq!(m.num_levels(), 7);
        assert_eq!(m.next_sst_id, 0);
        for i in 0..7 {
            assert_eq!(m.level_count(i), 0);
        }
    }

    #[test]
    fn add_and_remove() {
        let mut m = Manifest::new(3);
        let (id, name) = m.next_sst_filename();
        m.add_sstable(SSTableEntry {
            id,
            level: 0,
            file_name: name,
            min_key: b"a".to_vec(),
            max_key: b"z".to_vec(),
            entry_count: 100,
            file_size: 4096,
        });
        assert_eq!(m.level_count(0), 1);

        m.remove_sstables(&[id]);
        assert_eq!(m.level_count(0), 0);
    }

    #[test]
    fn save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST");

        let mut m = Manifest::new(3);
        let (id, name) = m.next_sst_filename();
        m.add_sstable(SSTableEntry {
            id,
            level: 1,
            file_name: name,
            min_key: b"hello".to_vec(),
            max_key: b"world".to_vec(),
            entry_count: 50,
            file_size: 2048,
        });
        m.save(&path).unwrap();

        let loaded = Manifest::load(&path, 3).unwrap();
        assert_eq!(loaded.level_count(1), 1);
        assert_eq!(loaded.level(1)[0].min_key, b"hello");
        assert_eq!(loaded.next_sst_id, 1);
    }
}
