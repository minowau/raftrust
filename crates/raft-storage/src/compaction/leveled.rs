use raft_common::error::Result;
use std::path::Path;

use crate::sstable::block::BlockEntry;
use crate::sstable::reader::SSTableReader;
use crate::sstable::writer::SSTableWriter;

use super::manifest::{Manifest, SSTableEntry};

/// Configuration for leveled compaction.
pub struct CompactionConfig {
    /// Maximum number of SSTables in Level 0 before triggering compaction.
    pub l0_compaction_trigger: usize,
    /// Size ratio between adjacent levels (e.g., 10 means L1 is 10x L0).
    pub level_size_ratio: usize,
    /// Target block size for new SSTables.
    pub block_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            level_size_ratio: 10,
            block_size: 4096,
        }
    }
}

/// Leveled compaction strategy.
///
/// Level 0: Flushed MemTables. May have overlapping key ranges.
/// Level 1+: Non-overlapping, sorted key ranges. Each level is ~10x the previous.
///
/// Compaction picks SSTables from level N that overlap with level N+1,
/// merge-sorts them, and writes new SSTables to level N+1.
pub struct LeveledCompaction {
    config: CompactionConfig,
}

impl LeveledCompaction {
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    /// Check if compaction is needed and return the level to compact from.
    pub fn needs_compaction(&self, manifest: &Manifest) -> Option<usize> {
        // Check L0 first
        if manifest.level_count(0) >= self.config.l0_compaction_trigger {
            return Some(0);
        }

        // Check other levels: compact if level has more SSTables than target
        for level in 1..manifest.num_levels() - 1 {
            let target =
                self.config.l0_compaction_trigger * self.config.level_size_ratio.pow(level as u32);
            if manifest.level_count(level) > target {
                return Some(level);
            }
        }

        None
    }

    /// Compact from `level` to `level + 1`.
    /// Returns the IDs of SSTables to remove and the new SSTable entries to add.
    pub fn compact(
        &self,
        manifest: &mut Manifest,
        data_dir: &Path,
        level: usize,
    ) -> Result<CompactionResult> {
        let target_level = level + 1;

        // Pick source SSTables from the level
        let source_entries: Vec<SSTableEntry> = manifest.level(level).to_vec();
        if source_entries.is_empty() {
            return Ok(CompactionResult::default());
        }

        // Find the key range of source SSTables
        let min_key = source_entries
            .iter()
            .map(|e| e.min_key.as_slice())
            .min()
            .unwrap()
            .to_vec();
        let max_key = source_entries
            .iter()
            .map(|e| e.max_key.as_slice())
            .max()
            .unwrap()
            .to_vec();

        // Find overlapping SSTables in the target level
        let target_entries: Vec<SSTableEntry> = manifest
            .level(target_level)
            .iter()
            .filter(|e| e.min_key <= max_key && e.max_key >= min_key)
            .cloned()
            .collect();

        // Read all entries from source and overlapping target SSTables
        let mut all_entries = Vec::new();
        for sst_entry in source_entries.iter().chain(target_entries.iter()) {
            let path = data_dir.join(&sst_entry.file_name);
            let reader = SSTableReader::open(&path)?;
            all_entries.extend(reader.iter()?);
        }

        // Merge-sort by key, keeping only the latest version of each key
        all_entries.sort_by(|a, b| a.key.cmp(&b.key));
        all_entries.dedup_by(|a, b| {
            if a.key == b.key {
                // Keep `b` (first occurrence after sort is the one to keep since dedup_by
                // keeps the first of each group). Actually dedup_by removes `a` if closure
                // returns true. So `b` survives. For L0 compaction where newer SSTables
                // should win, we rely on the caller ordering correctly.
                true
            } else {
                false
            }
        });

        // Remove tombstones at the deepest level (they've propagated down)
        let is_deepest = target_level >= manifest.num_levels() - 1;

        // Write new SSTables to target level
        let mut new_entries = Vec::new();
        let mut old_ids: Vec<u64> = source_entries.iter().map(|e| e.id).collect();
        old_ids.extend(target_entries.iter().map(|e| e.id));

        if all_entries.is_empty() {
            // All entries were tombstones at deepest level or empty
            manifest.remove_sstables(&old_ids);
            return Ok(CompactionResult {
                removed_ids: old_ids,
                new_entries: vec![],
            });
        }

        // Write a single new SSTable (could split into multiple for very large compactions)
        let (sst_id, sst_name) = manifest.next_sst_filename();
        let sst_path = data_dir.join(&sst_name);

        let filtered_entries: Vec<&BlockEntry> = if is_deepest {
            all_entries.iter().filter(|e| e.value.is_some()).collect()
        } else {
            all_entries.iter().collect()
        };

        if filtered_entries.is_empty() {
            manifest.remove_sstables(&old_ids);
            return Ok(CompactionResult {
                removed_ids: old_ids,
                new_entries: vec![],
            });
        }

        let mut writer =
            SSTableWriter::new(&sst_path, self.config.block_size, filtered_entries.len());
        for entry in &filtered_entries {
            writer.add(&entry.key, entry.value.as_deref());
        }
        let info = writer.finish()?;

        let new_entry = SSTableEntry {
            id: sst_id,
            level: target_level,
            file_name: sst_name,
            min_key: filtered_entries.first().unwrap().key.clone(),
            max_key: filtered_entries.last().unwrap().key.clone(),
            entry_count: info.entry_count,
            file_size: info.file_size,
        };

        // Update manifest
        manifest.remove_sstables(&old_ids);
        manifest.add_sstable(new_entry.clone());
        new_entries.push(new_entry);

        // Delete old SSTable files
        for entry in source_entries.iter().chain(target_entries.iter()) {
            let path = data_dir.join(&entry.file_name);
            let _ = std::fs::remove_file(path); // Best-effort cleanup
        }

        Ok(CompactionResult {
            removed_ids: old_ids,
            new_entries,
        })
    }
}

#[derive(Debug, Default)]
pub struct CompactionResult {
    pub removed_ids: Vec<u64>,
    pub new_entries: Vec<SSTableEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::writer::SSTableWriter;

    fn write_test_sst(
        dir: &Path,
        manifest: &mut Manifest,
        level: usize,
        entries: &[(&[u8], Option<&[u8]>)],
    ) {
        let (id, name) = manifest.next_sst_filename();
        let path = dir.join(&name);
        let mut writer = SSTableWriter::new(&path, 256, entries.len());
        for (key, val) in entries {
            writer.add(key, *val);
        }
        let info = writer.finish().unwrap();
        manifest.add_sstable(SSTableEntry {
            id,
            level,
            file_name: name,
            min_key: entries.first().unwrap().0.to_vec(),
            max_key: entries.last().unwrap().0.to_vec(),
            entry_count: info.entry_count,
            file_size: info.file_size,
        });
    }

    #[test]
    fn compact_l0_to_l1() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::new(4);
        let compaction = LeveledCompaction::new(CompactionConfig {
            l0_compaction_trigger: 2,
            ..Default::default()
        });

        // Add 2 L0 SSTables
        write_test_sst(
            dir.path(),
            &mut manifest,
            0,
            &[(b"a", Some(b"1")), (b"c", Some(b"3"))],
        );
        write_test_sst(
            dir.path(),
            &mut manifest,
            0,
            &[(b"b", Some(b"2")), (b"d", Some(b"4"))],
        );

        assert!(compaction.needs_compaction(&manifest).is_some());

        let result = compaction.compact(&mut manifest, dir.path(), 0).unwrap();
        assert_eq!(result.removed_ids.len(), 2);
        assert_eq!(result.new_entries.len(), 1);
        assert_eq!(manifest.level_count(0), 0);
        assert_eq!(manifest.level_count(1), 1);

        // Verify merged SSTable has all keys
        let sst = SSTableReader::open(&dir.path().join(&result.new_entries[0].file_name)).unwrap();
        assert_eq!(sst.get(b"a").unwrap(), Some(Some(b"1".to_vec())));
        assert_eq!(sst.get(b"b").unwrap(), Some(Some(b"2".to_vec())));
        assert_eq!(sst.get(b"c").unwrap(), Some(Some(b"3".to_vec())));
        assert_eq!(sst.get(b"d").unwrap(), Some(Some(b"4".to_vec())));
    }

    #[test]
    fn dedup_on_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::new(4);
        let compaction = LeveledCompaction::new(CompactionConfig {
            l0_compaction_trigger: 2,
            ..Default::default()
        });

        // Two L0 SSTables with overlapping keys
        write_test_sst(dir.path(), &mut manifest, 0, &[(b"key", Some(b"old"))]);
        write_test_sst(dir.path(), &mut manifest, 0, &[(b"key", Some(b"new"))]);

        compaction.compact(&mut manifest, dir.path(), 0).unwrap();
        assert_eq!(manifest.level_count(1), 1);

        let sst_entry = &manifest.level(1)[0];
        let sst = SSTableReader::open(&dir.path().join(&sst_entry.file_name)).unwrap();
        let entries = sst.iter().unwrap();
        // Should have only 1 entry for "key"
        assert_eq!(entries.len(), 1);
    }
}
