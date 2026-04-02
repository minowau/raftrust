# Storage Engine Internals

This document describes the LSM-tree storage engine and MVCC layer.

## Storage Stack

```
  Client Request
       │
       ▼
  ┌──────────┐
  │ MvccStore │  Versioned keys, snapshot reads, transactions
  └────┬─────┘
       │
       ▼
  ┌──────────┐
  │ LsmTree  │  LSM-tree: MemTable → SSTable → Compaction
  └────┬─────┘
       │
       ▼
  ┌──────────┐
  │   WAL    │  Write-ahead log with CRC32 checksums
  └──────────┘
```

## Write-Ahead Log (WAL)

Every write is first appended to the WAL before being applied to the MemTable. This ensures durability: if the process crashes, the WAL is replayed on restart to recover the MemTable.

**Record format:**
```
┌──────────┬──────────┬──────────┬──────────┐
│ Length   │ CRC32    │ Key      │ Value    │
│ (4 bytes)│ (4 bytes)│ (var)    │ (var)    │
└──────────┴──────────┴──────────┴──────────┘
```

The CRC32 checksum covers the key and value bytes. On replay, any record with a mismatched checksum is treated as a partial write (crash during write) and is discarded along with all subsequent records.

## MemTable

An in-memory sorted map (`BTreeMap<Vec<u8>, Vec<u8>>`) that absorbs all writes. When the MemTable reaches the configured size limit (default 4MB), it is flushed to disk as an SSTable.

Properties:
- All reads check the MemTable first (most recent data)
- Writes are O(log n) in the number of keys
- The MemTable is rebuilt from the WAL on crash recovery

## SSTable (Sorted String Table)

Immutable, sorted files on disk. Each SSTable contains:

```
┌───────────────────────────────────┐
│ Data Block 0                      │
│   key₁ → value₁                  │
│   key₂ → value₂                  │
│   ...                             │
├───────────────────────────────────┤
│ Data Block 1                      │
│   ...                             │
├───────────────────────────────────┤
│ Index Block                       │
│   block₀ → first_key, offset     │
│   block₁ → first_key, offset     │
├───────────────────────────────────┤
│ Bloom Filter                      │
│   (probabilistic key membership)  │
├───────────────────────────────────┤
│ Footer                            │
│   index_offset, bloom_offset,     │
│   entry_count, checksum           │
└───────────────────────────────────┘
```

**Bloom filters** provide fast negative lookups: if the bloom filter says a key is absent, it definitely is. This avoids reading SSTable blocks for keys that don't exist, which is critical for point lookups across many levels.

## Leveled Compaction

SSTables are organized into 7 levels (L0-L6):
- **L0**: Direct flushes from MemTable (may have overlapping key ranges)
- **L1-L6**: Non-overlapping key ranges within each level, 10x size ratio between levels

When a level exceeds its size limit, compaction merges overlapping SSTables from level N with level N+1:

```
L0: [a-z] [a-m] [n-z]     ← overlapping, direct flushes
L1: [a-f] [g-m] [n-z]     ← non-overlapping after compaction
L2: [a-c] [d-f] ... [x-z] ← larger, non-overlapping
```

Compaction bounds write amplification to ~10x per level (vs. size-tiered which can be much higher).

## MVCC (Multi-Version Concurrency Control)

Every key is stored with a revision number, enabling point-in-time reads:

**Internal key format:**
```
┌──────────────┬──────────────────┐
│ User Key     │ Revision (u64 BE)│
└──────────────┴──────────────────┘
```

Revisions are stored in big-endian so that the LSM-tree's sorted order naturally groups all versions of a key together, with the newest version (highest revision) first.

**Versioned value:**
```json
{
  "value": [bytes] | null,    // null = tombstone (delete)
  "create_revision": 42,
  "mod_revision": 57,
  "lease_id": 0,
  "ttl_seconds": 0
}
```

### Read at latest revision
Scan the key prefix, return the first (newest) non-tombstone entry.

### Read at specific revision
Scan the key prefix, return the first entry with `mod_revision ≤ target_revision`.

### Snapshot isolation
Transactions read from a frozen revision, ensuring a consistent view even as concurrent writes create new versions.

## Garbage Collection

Old versions are candidates for garbage collection when:
- The version is older than the oldest active snapshot
- A newer version of the same key exists

GC runs during compaction: when merging SSTables, versions older than the GC watermark are dropped.
