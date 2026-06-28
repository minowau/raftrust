# Raft

[![CI](https://img.shields.io/github/actions/workflow/status/louisphilipmarcoux/raft/ci.yml?branch=main&label=CI)](https://github.com/louisphilipmarcoux/raft/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/raft-consensus-core.svg)](https://crates.io/crates/raft-consensus-core)
[![Rust](https://img.shields.io/badge/Rust-1.75+-DEA584?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/Tests-230+-brightgreen)](https://github.com/louisphilipmarcoux/raft/actions)

A production-grade distributed key-value store written in Rust, implementing the Raft consensus algorithm for fault-tolerant replication.

Raft implements leader election with pre-vote algorithm, log replication, snapshots, linearizable reads, leadership transfer, joint consensus membership changes, MVCC with OCC transactions, and a custom LSM-tree storage engine — the same architecture used by [etcd](https://etcd.io), the distributed KV store that Kubernetes uses for all cluster state.

## What Raft Does

- **Consensus**: leader election with pre-vote, log replication, commit via quorum
- **Linearizable reads**: read index protocol — confirms leadership via heartbeat quorum before serving
- **Leadership transfer**: graceful handoff via TimeoutNow, proposal blocking during transfer
- **Membership changes**: joint consensus (C_old,new → C_new), add/remove nodes without downtime
- **Snapshots**: CRC32-verified snapshots, log compaction, InstallSnapshot for lagging followers
- **Storage engine**: custom LSM-tree — WAL with CRC32 checksums, MemTable, SSTables, bloom filters, leveled compaction
- **MVCC**: versioned keys, point-in-time reads, snapshot isolation, OCC transactions, TTL/key expiry
- **Watch API**: gRPC bidirectional streaming for real-time key change notifications
- **Leases**: grant/keepalive/revoke with auto-expiry, distributed locks via key attachment
- **Admin API**: add/remove nodes, transfer leadership, drain node, trigger compaction, backup/restore
- **Observability**: 27 Prometheus metrics, `/health` + `/ready` endpoints, structured JSON logging
- **Chaos testing**: network partition injection, disk failure simulation, clock skew, in-process cluster harness

## What Raft Does Not Do

These are explicit architectural boundaries, not missing features. See [docs/limitations.md](docs/limitations.md) for detailed rationale and future work estimates.

- No multi-region replication (single Raft group)
- No automatic horizontal sharding (single Raft group)
- No follower reads with tunable staleness
- No encryption at rest
- No authentication / ACL system
- No SQL query layer
- No Windows support

## Build

Requires Rust 1.75+ and `protoc` (protobuf compiler).

```bash
cargo build                # Build all crates
cargo test                 # Run all 230+ tests
cargo test -p raft-consensus  # Run consensus tests only
cargo test -p raft-chaos      # Run chaos framework tests
cargo bench -p raft-storage   # Run storage benchmarks
cargo clippy               # Lint
cargo fmt -- --check       # Check formatting
```

## Architecture

```text
┌─────────────────────────────────────────────────────────┐
│                      Client                             │
│  KvClient (retry + leader tracking + backoff)           │
└──────────────────────┬──────────────────────────────────┘
                       │ gRPC
┌──────────────────────▼──────────────────────────────────┐
│                    RaftServer                           |                  
│  ┌──────────┐ ┌──────────┐ ┌───────┐ ┌──────────────┐   │
│  │KV Service│ │Watch Svc │ │Lease  │ │ Admin Service│   │
│  │get/put/  │ │streaming │ │grant/ │ │ add/remove   │   │
│  │delete/   │ │key change│ │revoke/│ │ transfer/    │   │
│  │range     │ │events    │ │keepalv│ │ backup/drain │   │
│  └────┬─────┘ └────┬─────┘ └───┬───┘ └──────┬───────┘   │
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
│  │  WAL · MemTable · SSTables · Bloom Filters ·      │  │
│  │  Leveled Compaction                               │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  HTTP: /metrics · /health · /ready                      │
└─────────────────────────────────────────────────────────┘
```

## Project Structure

```text
raft/
├── crates/
│   ├── raft-common/       Shared types, config, error handling, Prometheus metrics
│   ├── raft-storage/      LSM-tree: WAL, MemTable, SSTable, compaction, bloom filters
│   ├── raft-mvcc/         MVCC: versioned keys, snapshot reads, OCC transactions, TTL
│   ├── raft-consensus/    Raft: election, replication, snapshots, read index, transfer, joint consensus
│   ├── raft-server/       Full server: KV/Watch/Lease/Admin gRPC, apply loop, HTTP endpoints
│   ├── raft-client/       Client library: retry with exponential backoff, leader tracking
│   ├── raft-admin/        Admin CLI tool
│   └── raft-chaos/        Chaos testing: network partitions, disk failures, clock skew
├── proto/                 Protobuf definitions (raft, kv, watch, lease, admin, membership)
├── benches/               Criterion storage benchmarks
├── docs/
│   ├── decisions/         Architecture Decision Records (ADRs)
│   ├── internals/         Deep-dive: Raft consensus, storage engine
│   └── limitations.md     Known limitations + future work
└── .github/workflows/     CI: test + lint + bench on every push
```

## Testing

| Category | Count | What It Proves |
| --- | --- | --- |
| Storage engine | 48 | WAL crash recovery, compaction, bloom filters, key encoding |
| MVCC + transactions | 24 | Snapshot isolation, OCC conflict detection, TTL, range scans |
| Raft consensus | 73 | Elections, replication, snapshots, read index, transfer, joint consensus |
| Server integration | 37 | Apply loop, watch events, lease expiry, backup format, HTTP endpoints, metrics |
| Chaos framework | 34 | Network partitions, disk failures, clock skew, cluster orchestration |
| Cluster integration | 8 | Multi-node election, replication, failover, snapshot install |
| Common + client | 6 | Metrics encoding, leader hint parsing |

## Documentation

- [ADR-001: Raft over Paxos](docs/decisions/001-raft-over-paxos.md) — understandability, industry validation, testability
- [ADR-002: LSM-tree over B-tree](docs/decisions/002-lsm-tree-over-btree.md) — write-optimized, MVCC fit
- [ADR-003: Joint Consensus](docs/decisions/003-joint-consensus-membership.md) — two-phase safety guarantee
- [ADR-004: OCC Transactions](docs/decisions/004-occ-transactions.md) — Raft integration, no deadlocks
- [ADR-005: gRPC + Protobuf](docs/decisions/005-grpc-for-rpcs.md) — streaming, type safety
- [Raft Internals](docs/internals/raft-consensus.md) — deep-dive into the consensus implementation
- [Storage Internals](docs/internals/storage-engine.md) — LSM-tree, WAL, MVCC, compaction
- [Known Limitations](docs/limitations.md) — honest scope boundaries and future work

## License

MIT — see [LICENSE](LICENSE).
