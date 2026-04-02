# ADR-002: LSM-Tree over B-Tree for Storage Engine

## Status
Accepted

## Context
The KV store needs a persistent storage engine. The two dominant approaches are B-trees (used by PostgreSQL, SQLite) and LSM-trees (used by RocksDB, LevelDB, Cassandra).

## Options Considered

### B-Tree
- **Pros**: Predictable read latency, good for read-heavy workloads, in-place updates
- **Cons**: Write amplification from random I/O, more complex concurrency control, page splits cause fragmentation

### LSM-Tree (Log-Structured Merge Tree)
- **Pros**: Sequential write I/O (WAL + memtable flush), high write throughput, simple crash recovery (WAL replay), natural fit for append-only MVCC
- **Cons**: Read amplification (must check multiple levels), compaction causes periodic I/O spikes, space amplification from multiple copies

## Decision
We chose **LSM-tree** because:

1. **Write-optimized**: Raft consensus already serializes writes through a WAL. An LSM-tree's append-only nature aligns perfectly — the WAL serves double duty for both Raft durability and storage recovery.
2. **MVCC fit**: Our MVCC layer stores multiple versions of each key. LSM-trees handle this naturally since old versions live in lower levels until compacted.
3. **Simplicity**: The core implementation (MemTable → SSTable flush → leveled compaction) is straightforward and well-documented.
4. **etcd precedent**: etcd uses bbolt (B-tree), but the Raft paper's reference implementation and most modern distributed KV stores (TiKV, CockroachDB) use LSM variants.

## Implementation Details
- **MemTable**: `BTreeMap` for sorted in-memory writes
- **SSTable**: Block-based format with bloom filters for fast negative lookups
- **Compaction**: Leveled strategy (7 levels, 10x size ratio)
- **WAL**: CRC32 checksums on every record for corruption detection

## Consequences
- Range scans may touch multiple SSTables (mitigated by bloom filters and block indexes)
- Compaction creates periodic I/O load (mitigated by leveled strategy which bounds write amplification)
- Space usage is ~2x data size during compaction (acceptable for our scope)

## What Would Change This Decision
If the workload were >95% reads with very few writes, a B-tree would provide more predictable read latency. For our distributed KV store with Raft replication, write throughput is the bottleneck.
