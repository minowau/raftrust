# Distributed KV Store with Raft Consensus

A production-quality distributed key-value store built from scratch in Rust, implementing the Raft consensus algorithm for fault-tolerant replication. Equivalent to [etcd](https://etcd.io) — the distributed KV store that Kubernetes uses for all cluster state.

## What It Is

A strongly-consistent, replicated key-value store that:
- **Survives node failures**: any minority of nodes can crash without data loss
- **Elects leaders automatically**: with pre-vote to prevent disruptive elections
- **Provides linearizable reads**: via the read index protocol (no stale data)
- **Supports live reconfiguration**: add/remove nodes without downtime via joint consensus
- **Watches keys in real-time**: gRPC streaming for instant change notifications
- **Manages distributed locks**: via leases with automatic expiry on client death

## What It Guarantees

- Every write acknowledged by the leader is replicated to a majority before commit
- No two leaders exist in the same term (Raft safety property)
- Reads via the read index protocol are linearizable
- Membership changes never create split-brain (joint consensus)
- Crash recovery loses zero committed data (WAL with CRC32 checksums)
- Snapshots are integrity-verified (CRC32 checksum on every snapshot)

## What It Does Not Do

See [Known Limitations](docs/limitations.md) for a detailed and honest list. Key exclusions:
- No multi-region replication (single Raft group)
- No automatic sharding (all data on every node)
- No encryption at rest or authentication
- No SQL query layer

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Client                             │
│  KvClient (retry + leader tracking + backoff)           │
└──────────────────────┬──────────────────────────────────┘
                       │ gRPC
┌──────────────────────▼──────────────────────────────────┐
│                    RaftServer                            │
│  ┌──────────┐ ┌──────────┐ ┌───────┐ ┌──────────────┐  │
│  │KV Service│ │Watch Svc │ │Lease  │ │ Admin Service │  │
│  │get/put/  │ │streaming │ │grant/ │ │ add/remove   │  │
│  │delete/   │ │key change│ │revoke/│ │ transfer/    │  │
│  │range     │ │events    │ │keepalv│ │ backup/drain │  │
│  └────┬─────┘ └────┬─────┘ └───┬───┘ └──────┬───────┘  │
│       │            │           │             │          │
│  ┌────▼────────────▼───────────▼─────────────▼───────┐  │
│  │                  Apply Loop                       │  │
│  │  Committed entries → MVCC Store + Watch + Leases  │  │
│  └──────────────────────┬────────────────────────────┘  │
│                         │                               │
│  ┌──────────────────────▼────────────────────────────┐  │
│  │               Raft Consensus                      │  │
│  │  Election · Replication · Snapshots ·             │  │
│  │  Read Index · Leadership Transfer ·               │  │
│  │  Joint Consensus                                  │  │
│  └──────────────────────┬────────────────────────────┘  │
│                         │                               │
│  ┌──────────────────────▼────────────────────────────┐  │
│  │                MVCC Store                         │  │
│  │  Versioned keys · Snapshot isolation · OCC txns   │  │
│  └──────────────────────┬────────────────────────────┘  │
│                         │                               │
│  ┌──────────────────────▼────────────────────────────┐  │
│  │              LSM-Tree Storage                     │  │
│  │  WAL · MemTable · SSTables · Bloom Filters ·     │  │
│  │  Leveled Compaction                               │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  HTTP: /metrics · /health · /ready                      │
└─────────────────────────────────────────────────────────┘
```

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `raft-common` | Shared types, config, error handling, Prometheus metrics |
| `raft-storage` | LSM-tree engine: WAL, MemTable, SSTable, compaction, bloom filters |
| `raft-mvcc` | MVCC layer: versioned keys, snapshot reads, OCC transactions, TTL |
| `raft-consensus` | Raft algorithm: election, replication, snapshots, read index, leadership transfer, joint consensus |
| `raft-server` | Full server: KV/Watch/Lease/Admin gRPC services, apply loop, HTTP endpoints |
| `raft-client` | Client library: retry with exponential backoff, leader tracking |
| `raft-admin` | Admin CLI tool |
| `raft-chaos` | Chaos testing: network partitions, disk failures, clock skew, cluster harness |

## Running Tests

```bash
# Run all 230+ tests
cargo test

# Run specific crate tests
cargo test -p raft-consensus
cargo test -p raft-chaos

# Run benchmarks
cargo bench -p raft-storage
```

## Test Coverage

| Category | Count | What It Proves |
|----------|-------|----------------|
| Storage engine | 48 | WAL crash recovery, compaction, bloom filters, key encoding |
| MVCC + transactions | 24 | Snapshot isolation, OCC conflict detection, TTL, range scans |
| Raft consensus | 73 | Elections, replication, snapshots, read index, transfer, joint consensus |
| Server integration | 37 | Apply loop, watch events, lease expiry, backup format, HTTP endpoints, metrics |
| Chaos framework | 34 | Network partitions, disk failures, clock skew, cluster orchestration |
| Client | 2 | Leader hint parsing |
| Cluster integration | 8 | Multi-node election, replication, failover, snapshot install |
| Common | 4 | Metrics creation and encoding |

## Documentation

- **[Architecture Decisions](docs/decisions/)** — Why Raft over Paxos, LSM over B-tree, joint consensus, OCC, gRPC
- **[Raft Internals](docs/internals/raft-consensus.md)** — Deep-dive into the consensus implementation
- **[Storage Internals](docs/internals/storage-engine.md)** — LSM-tree, WAL, MVCC, compaction
- **[Known Limitations](docs/limitations.md)** — Honest scope boundaries and future work

## Tech Stack

| Component | Choice | Reason |
|-----------|--------|--------|
| Language | Rust | Ownership model enforces correctness, zero-cost abstractions |
| Async runtime | Tokio | Industry standard for async Rust |
| gRPC | tonic + prost | Streaming support, type-safe protobuf, HTTP/2 |
| Storage | Custom LSM-tree | Full control, educational value, WAL integration |
| Metrics | Prometheus | Industry standard, scrape-based, Grafana-compatible |
| Serialization | serde + protobuf | JSON for internal state, protobuf for wire protocol |

## License

MIT
