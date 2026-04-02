# ADR-004: Optimistic Concurrency Control for Transactions

## Status
Accepted

## Context
The KV store supports multi-key atomic transactions. We need a concurrency control mechanism that provides snapshot isolation while integrating cleanly with Raft's serial apply loop.

## Options Considered

### Pessimistic locking (2PL)
- **Pros**: Simple mental model, guaranteed to succeed once locks are acquired
- **Cons**: Deadlock potential, lock management overhead, poor fit for distributed systems where lock holders can fail

### Optimistic Concurrency Control (OCC)
- **Pros**: No locks needed, reads never block, conflict detection at commit time, natural fit for MVCC
- **Cons**: Transactions may abort on conflict and need retry, high-contention workloads suffer

## Decision
We chose **OCC** because:

1. **Raft integration**: All committed writes go through the Raft log sequentially. OCC's conflict detection at commit time maps directly to this — the apply loop processes transactions in log order and can detect conflicts deterministically.
2. **MVCC synergy**: With MVCC, every read sees a consistent snapshot at a point-in-time revision. OCC validates that the read set hasn't changed between the snapshot and commit.
3. **No distributed deadlocks**: In a distributed system, 2PL across nodes creates complex deadlock scenarios. OCC avoids this entirely.
4. **Read performance**: Readers never acquire locks, so read-heavy workloads aren't impacted by concurrent writes.

## Consequences
- High-contention workloads (many transactions touching the same keys) will see more aborts
- Clients must implement retry logic for aborted transactions
- Transaction size should be bounded (large read sets increase conflict probability)

## What Would Change This Decision
If the workload were dominated by high-contention writes to a small set of keys (e.g., a counter), pessimistic locking or serializable transactions would be more efficient. For our etcd-equivalent use case (config storage, service discovery), contention is typically low.
