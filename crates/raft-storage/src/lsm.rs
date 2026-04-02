use parking_lot::RwLock;
use raft_common::error::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

use crate::compaction::leveled::{CompactionConfig, LeveledCompaction};
use crate::compaction::manifest::{Manifest, SSTableEntry};
use crate::engine::StorageEngine;
use crate::memtable::MemTable;
use crate::sstable::reader::SSTableReader;
use crate::sstable::writer::SSTableWriter;
use crate::wal::reader::WalReader;
use crate::wal::writer::WalWriter;

const NUM_LEVELS: usize = 7;
const MANIFEST_FILE: &str = "MANIFEST";
const WAL_FILE: &str = "wal.log";

/// Operation types serialized to WAL.
#[derive(Debug)]
enum WalOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WalOp {
    fn encode(&self) -> Vec<u8> {
        match self {
            WalOp::Put { key, value } => {
                let mut buf = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
                buf.push(0x01); // put tag
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
                buf
            }
            WalOp::Delete { key } => {
                let mut buf = Vec::with_capacity(1 + 4 + key.len());
                buf.push(0x02); // delete tag
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf
            }
        }
    }

    fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(raft_common::error::Error::Corruption(
                "empty WAL op".to_string(),
            ));
        }
        match data[0] {
            0x01 => {
                let key_len =
                    u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let key = data[5..5 + key_len].to_vec();
                let val_start = 5 + key_len;
                let val_len = u32::from_le_bytes([
                    data[val_start],
                    data[val_start + 1],
                    data[val_start + 2],
                    data[val_start + 3],
                ]) as usize;
                let value = data[val_start + 4..val_start + 4 + val_len].to_vec();
                Ok(WalOp::Put { key, value })
            }
            0x02 => {
                let key_len =
                    u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
                let key = data[5..5 + key_len].to_vec();
                Ok(WalOp::Delete { key })
            }
            tag => Err(raft_common::error::Error::Corruption(format!(
                "unknown WAL op tag: {}",
                tag
            ))),
        }
    }
}

/// Top-level LSM-tree orchestrator.
///
/// Read path: MemTable -> immutable MemTables -> L0 SSTables -> L1 -> ... -> LN
/// Write path: WAL -> MemTable (flush to SSTable when full)
pub struct LsmTree {
    data_dir: PathBuf,
    active_memtable: RwLock<MemTable>,
    immutable_memtables: RwLock<Vec<Arc<MemTable>>>,
    manifest: RwLock<Manifest>,
    wal: RwLock<WalWriter>,
    sstable_cache: RwLock<Vec<(SSTableEntry, SSTableReader)>>,
    compaction: LeveledCompaction,
    config: LsmConfig,
}

#[derive(Debug, Clone)]
pub struct LsmConfig {
    pub memtable_size_limit: usize,
    pub block_size: usize,
    pub l0_compaction_trigger: usize,
    pub level_size_ratio: usize,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_size_limit: 4 * 1024 * 1024,
            block_size: 4096,
            l0_compaction_trigger: 4,
            level_size_ratio: 10,
        }
    }
}

impl LsmTree {
    /// Open or create an LSM-tree at the given directory.
    pub fn open(data_dir: &Path, config: LsmConfig) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;

        // Load manifest
        let manifest_path = data_dir.join(MANIFEST_FILE);
        let manifest = Manifest::load(&manifest_path, NUM_LEVELS)?;

        // Replay WAL into a fresh MemTable
        let wal_path = data_dir.join(WAL_FILE);
        let mut memtable = MemTable::new(config.memtable_size_limit);

        if wal_path.exists() {
            let records = WalReader::open(&wal_path)?.read_all()?;
            info!(records = records.len(), "Replaying WAL entries");
            for record in &records {
                match WalOp::decode(&record.data)? {
                    WalOp::Put { key, value } => memtable.put(key, value),
                    WalOp::Delete { key } => memtable.delete(key),
                }
            }
        }

        // Open WAL for writing (truncate old WAL — we've replayed it)
        // We create a fresh WAL since the memtable now holds the replayed state
        let wal = WalWriter::open(&wal_path)?;

        // Load SSTable readers
        let mut sstable_cache = Vec::new();
        for level in &manifest.levels {
            for entry in level {
                let path = data_dir.join(&entry.file_name);
                if path.exists() {
                    let reader = SSTableReader::open(&path)?;
                    sstable_cache.push((entry.clone(), reader));
                }
            }
        }

        let compaction = LeveledCompaction::new(CompactionConfig {
            l0_compaction_trigger: config.l0_compaction_trigger,
            level_size_ratio: config.level_size_ratio,
            block_size: config.block_size,
        });

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            active_memtable: RwLock::new(memtable),
            immutable_memtables: RwLock::new(Vec::new()),
            manifest: RwLock::new(manifest),
            wal: RwLock::new(wal),
            sstable_cache: RwLock::new(sstable_cache),
            compaction,
            config,
        })
    }

    /// Flush the active MemTable to an SSTable. Called when MemTable is full.
    fn flush_memtable(&self) -> Result<()> {
        // Rotate: active -> immutable, create new active
        let frozen;
        {
            let mut active = self.active_memtable.write();
            if active.is_empty() {
                return Ok(());
            }
            frozen = Arc::new(std::mem::replace(
                &mut *active,
                MemTable::new(self.config.memtable_size_limit),
            ));
        }
        self.immutable_memtables.write().push(frozen.clone());

        // Write SSTable from frozen MemTable
        let (sst_id, sst_name) = self.manifest.write().next_sst_filename();
        let sst_path = self.data_dir.join(&sst_name);

        let entry_count = frozen.len();
        let mut writer = SSTableWriter::new(&sst_path, self.config.block_size, entry_count);

        let mut min_key = None;
        let mut max_key = None;
        for (key, value) in frozen.iter() {
            if min_key.is_none() {
                min_key = Some(key.to_vec());
            }
            max_key = Some(key.to_vec());
            writer.add(key, value);
        }

        let info = writer.finish()?;

        let sst_entry = SSTableEntry {
            id: sst_id,
            level: 0,
            file_name: sst_name,
            min_key: min_key.unwrap_or_default(),
            max_key: max_key.unwrap_or_default(),
            entry_count: info.entry_count,
            file_size: info.file_size,
        };

        // Update manifest
        {
            let mut manifest = self.manifest.write();
            manifest.add_sstable(sst_entry.clone());
            manifest.save(&self.data_dir.join(MANIFEST_FILE))?;
        }

        // Update SSTable cache
        {
            let reader = SSTableReader::open(&sst_path)?;
            self.sstable_cache.write().push((sst_entry, reader));
        }

        // Remove the frozen MemTable from immutables
        self.immutable_memtables.write().retain(|m| !Arc::ptr_eq(m, &frozen));

        // Reset WAL (memtable data is now in SSTable)
        {
            let wal_path = self.data_dir.join(WAL_FILE);
            // Truncate WAL by recreating it
            let _ = std::fs::remove_file(&wal_path);
            *self.wal.write() = WalWriter::open(&wal_path)?;
        }

        Ok(())
    }

    /// Run compaction if needed.
    pub fn maybe_compact(&self) -> Result<()> {
        let level = {
            let manifest = self.manifest.read();
            self.compaction.needs_compaction(&manifest)
        };

        if let Some(level) = level {
            info!(level, "Running compaction");
            let mut manifest = self.manifest.write();
            self.compaction
                .compact(&mut manifest, &self.data_dir, level)?;
            manifest.save(&self.data_dir.join(MANIFEST_FILE))?;

            // Rebuild SSTable cache
            let mut cache = self.sstable_cache.write();
            cache.clear();
            for level_entries in &manifest.levels {
                for entry in level_entries {
                    let path = self.data_dir.join(&entry.file_name);
                    if path.exists() {
                        let reader = SSTableReader::open(&path)?;
                        cache.push((entry.clone(), reader));
                    }
                }
            }
        }

        Ok(())
    }
}

impl StorageEngine for LsmTree {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check active MemTable
        {
            let mt = self.active_memtable.read();
            if let Some(result) = mt.get(key) {
                return match result {
                    Some(value) => Ok(Some(value.to_vec())),
                    None => Ok(None), // tombstone
                };
            }
        }

        // 2. Check immutable MemTables (newest first)
        {
            let immutables = self.immutable_memtables.read();
            for mt in immutables.iter().rev() {
                if let Some(result) = mt.get(key) {
                    return match result {
                        Some(value) => Ok(Some(value.to_vec())),
                        None => Ok(None),
                    };
                }
            }
        }

        // 3. Check SSTables (L0 first, then L1, L2, etc.)
        // L0 SSTables are searched newest-first (may overlap)
        {
            let cache = self.sstable_cache.read();
            // Sort by level, then by ID descending (newest first within level)
            let mut entries: Vec<&(SSTableEntry, SSTableReader)> = cache.iter().collect();
            entries.sort_by(|a, b| {
                a.0.level
                    .cmp(&b.0.level)
                    .then(b.0.id.cmp(&a.0.id))
            });

            for (_, reader) in entries {
                if let Some(result) = reader.get(key)? {
                    return match result {
                        Some(value) => Ok(Some(value)),
                        None => Ok(None), // tombstone
                    };
                }
            }
        }

        Ok(None)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Write to WAL first for durability
        let op = WalOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        {
            let mut wal = self.wal.write();
            wal.append(&op.encode())?;
            wal.sync()?;
        }

        // Write to MemTable
        {
            let mut mt = self.active_memtable.write();
            mt.put(key.to_vec(), value.to_vec());
        }

        // Check if MemTable is full
        if self.active_memtable.read().is_full() {
            self.flush_memtable()?;
            self.maybe_compact()?;
        }

        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let op = WalOp::Delete {
            key: key.to_vec(),
        };
        {
            let mut wal = self.wal.write();
            wal.append(&op.encode())?;
            wal.sync()?;
        }

        {
            let mut mt = self.active_memtable.write();
            mt.delete(key.to_vec());
        }

        if self.active_memtable.read().is_full() {
            self.flush_memtable()?;
            self.maybe_compact()?;
        }

        Ok(())
    }

    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::collections::BTreeMap;

        // Merge results from all sources. Later sources (higher levels) are overwritten
        // by earlier sources (MemTable, L0). We build a merged BTreeMap.
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // SSTables: read from deepest level first (so newer data overwrites)
        {
            let cache = self.sstable_cache.read();
            let mut entries: Vec<&(SSTableEntry, SSTableReader)> = cache.iter().collect();
            // Sort deepest first, oldest first
            entries.sort_by(|a, b| {
                b.0.level
                    .cmp(&a.0.level)
                    .then(a.0.id.cmp(&b.0.id))
            });

            for (_, reader) in entries {
                for entry in reader.scan(start, end)? {
                    merged.insert(entry.key, entry.value);
                }
            }
        }

        // Immutable MemTables (oldest first so newer overwrites)
        {
            let immutables = self.immutable_memtables.read();
            for mt in immutables.iter() {
                for (key, value) in mt.scan(start, end) {
                    merged.insert(key.to_vec(), value.map(|v| v.to_vec()));
                }
            }
        }

        // Active MemTable (newest, overwrites everything)
        {
            let mt = self.active_memtable.read();
            for (key, value) in mt.scan(start, end) {
                merged.insert(key.to_vec(), value.map(|v| v.to_vec()));
            }
        }

        // Filter out tombstones
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    fn flush(&self) -> Result<()> {
        self.flush_memtable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_test_lsm(dir: &Path) -> LsmTree {
        LsmTree::open(
            dir,
            LsmConfig {
                memtable_size_limit: 512, // small for testing
                block_size: 128,
                l0_compaction_trigger: 4,
                level_size_ratio: 10,
            },
        )
        .unwrap()
    }

    #[test]
    fn basic_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let lsm = open_test_lsm(dir.path());

        lsm.put(b"hello", b"world").unwrap();
        assert_eq!(lsm.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(lsm.get(b"missing").unwrap(), None);
    }

    #[test]
    fn delete() {
        let dir = tempfile::tempdir().unwrap();
        let lsm = open_test_lsm(dir.path());

        lsm.put(b"key", b"val").unwrap();
        assert_eq!(lsm.get(b"key").unwrap(), Some(b"val".to_vec()));

        lsm.delete(b"key").unwrap();
        assert_eq!(lsm.get(b"key").unwrap(), None);
    }

    #[test]
    fn scan_range() {
        let dir = tempfile::tempdir().unwrap();
        let lsm = open_test_lsm(dir.path());

        for i in 0u8..10 {
            lsm.put(&[i], &[i * 10]).unwrap();
        }

        let results = lsm.scan(&[3], &[7]).unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0], (vec![3], vec![30]));
        assert_eq!(results[3], (vec![6], vec![60]));
    }

    #[test]
    fn survives_flush() {
        let dir = tempfile::tempdir().unwrap();
        let lsm = open_test_lsm(dir.path());

        // Write enough to trigger a flush
        for i in 0..100u32 {
            let key = format!("key-{:04}", i);
            let val = format!("val-{:04}", i);
            lsm.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        // Verify all keys readable
        for i in 0..100u32 {
            let key = format!("key-{:04}", i);
            let val = format!("val-{:04}", i);
            assert_eq!(
                lsm.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "missing key: {}",
                key
            );
        }
    }

    #[test]
    fn crash_recovery_via_wal() {
        let dir = tempfile::tempdir().unwrap();

        // Write data
        {
            let lsm = open_test_lsm(dir.path());
            lsm.put(b"persistent", b"data").unwrap();
            // Don't flush — data is only in WAL + MemTable
        }

        // Reopen — should recover from WAL
        {
            let lsm = open_test_lsm(dir.path());
            assert_eq!(
                lsm.get(b"persistent").unwrap(),
                Some(b"data".to_vec())
            );
        }
    }

    #[test]
    fn overwrite_across_flush() {
        let dir = tempfile::tempdir().unwrap();
        let lsm = open_test_lsm(dir.path());

        lsm.put(b"key", b"v1").unwrap();
        lsm.flush().unwrap(); // v1 is now in SSTable

        lsm.put(b"key", b"v2").unwrap(); // v2 is in MemTable

        // MemTable should shadow SSTable
        assert_eq!(lsm.get(b"key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_across_flush() {
        let dir = tempfile::tempdir().unwrap();
        let lsm = open_test_lsm(dir.path());

        lsm.put(b"key", b"val").unwrap();
        lsm.flush().unwrap();

        lsm.delete(b"key").unwrap(); // tombstone in MemTable

        assert_eq!(lsm.get(b"key").unwrap(), None);
    }
}
