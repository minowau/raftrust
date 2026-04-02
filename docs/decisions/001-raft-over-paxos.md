# ADR-001: Raft over Paxos for Consensus

## Status
Accepted

## Context
We need a distributed consensus algorithm for the KV store's replication layer. The two main candidates are Paxos and Raft, both of which provide the same safety guarantees (agreement, validity, termination).

## Options Considered

### Paxos (Multi-Paxos)
- **Pros**: Theoretically minimal message complexity, well-studied formal proofs
- **Cons**: Notoriously difficult to implement correctly, the paper describes single-decree consensus and leaves multi-decree as an exercise, leader election is a separate concern, many implementation variants with subtle differences

### Raft
- **Pros**: Designed for understandability, leader-based (simplifies replication), explicit log structure, well-defined membership changes (joint consensus), extensive reference implementations (etcd, CockroachDB, TiKV)
- **Cons**: Slightly higher message count than optimal Paxos in some scenarios, leader bottleneck for writes

## Decision
We chose **Raft** because:

1. **Correctness confidence**: Raft's structure (leader election → log replication → safety) maps directly to implementation modules, reducing the gap between specification and code.
2. **Industry validation**: etcd (used by Kubernetes) and TiKV (used by TiDB) both use Raft, proving it works at scale.
3. **Testability**: Raft's deterministic state machine makes it straightforward to write chaos tests that verify safety properties.
4. **Feature completeness**: The Raft paper covers snapshots, membership changes, and linearizable reads — all features we need.

## Consequences
- Write throughput is bounded by the leader node. This is acceptable for our scope (single Raft group).
- To scale beyond a single leader's capacity, we would need multi-Raft (sharding), which is explicitly out of scope.

## What Would Change This Decision
If we needed leaderless writes (e.g., multi-region with local writes), we would revisit EPaxos or CRDTs.
