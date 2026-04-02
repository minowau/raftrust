# Known Limitations and Future Work

This document honestly describes what the system does not handle and what the next engineering investment would be for each item.

## Explicitly Out of Scope

### Multi-region replication (cross-datacenter Raft)
**What would change:** Raft's round-trip latency for every write is bounded by the slowest quorum member. Cross-datacenter RTTs (50-200ms) would make write latency unacceptable. Solution: Multi-Raft with region-aware placement, or a different protocol (EPaxos, CRDTs) for cross-region writes.

**Complexity:** High. Requires rethinking the consensus layer and adding region-aware routing.

### Automatic horizontal sharding
**What would change:** Currently all data lives in a single Raft group. For datasets larger than a single node, we would need hash or range sharding with a placement driver (like TiKV's PD).

**Complexity:** Very high. Requires shard splitting/merging, cross-shard transactions, and a metadata service.

### Follower reads with tunable staleness
**What would change:** Currently reads go to the leader (linearizable) or are unordered. Adding follower reads with bounded staleness (e.g., "read data no more than 5s old") requires tracking replication lag per follower and routing reads accordingly.

**Complexity:** Medium. Requires lag tracking and a client-side routing layer.

### Encryption at rest
**What would change:** SSTable and WAL data would need to be encrypted before writing to disk. Requires key management (rotation, escrow) and encrypted I/O wrappers.

**Complexity:** Medium. The storage engine's `put`/`get` interface would need encryption/decryption hooks.

### Authentication / ACL system
**What would change:** gRPC interceptors for token validation, a role-based access control model for keys (read/write/admin), and secure credential storage.

**Complexity:** Medium. Well-understood problem, but requires careful key prefix-based permission design.

## Known Technical Limitations

### Single-threaded apply loop
All committed entries are applied sequentially. For CPU-heavy operations (e.g., large transactions), this becomes a bottleneck. Solution: batch apply with pipelining.

### No write batching at the Raft layer
Each client write becomes a separate Raft log entry. Batching multiple writes into a single entry would reduce per-write consensus overhead.

### Snapshot transfer is all-or-nothing
Large snapshots are transferred as a single blob. For very large datasets, chunked transfer with resumption would reduce the impact of network interruptions during snapshot installation.

### Clock-dependent lease expiry
Lease TTL enforcement depends on wall-clock time. In environments with significant clock skew (e.g., VMs without NTP), leases may expire early or late. The chaos testing framework includes clock skew simulation, but production deployments should ensure NTP is running.

### No WAL truncation during normal operation
The WAL grows until a snapshot is triggered. In high-write-rate scenarios, the WAL can consume significant disk space before compaction. Adding periodic WAL checkpointing would bound this.

### Memory usage scales with key count
The MemTable holds all recent writes in memory. The bloom filters for all SSTables are also held in memory. For very large datasets (millions of keys), memory usage could become significant.

## What's Working Well

- **Correctness**: 230+ tests covering unit, integration, and chaos scenarios
- **Crash recovery**: WAL replay with CRC32 validation recovers from any crash point
- **Leader election**: Pre-vote prevents disruptive elections from partitioned nodes
- **Linearizable reads**: Read index protocol ensures no stale reads without log writes
- **Membership changes**: Joint consensus prevents split-brain during reconfiguration
- **Observability**: Prometheus metrics, structured logging, health/ready endpoints
