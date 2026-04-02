# ADR-003: Joint Consensus for Membership Changes

## Status
Accepted

## Context
The cluster needs to support adding and removing nodes without downtime. Membership changes are one of the trickiest parts of Raft because a naive approach can create a window where two independent majorities exist, violating safety.

## Options Considered

### Single-server changes (Raft §6 simplified)
- **Pros**: Simpler implementation, one config change at a time
- **Cons**: Cannot handle simultaneous add+remove, requires careful ordering, etcd hit bugs with this approach

### Joint consensus (Raft §6 full)
- **Pros**: Handles arbitrary config changes safely, two-phase transition guarantees no split-brain, well-proven in production systems
- **Cons**: More complex implementation, two log entries per change

## Decision
We chose **joint consensus** because:

1. **Safety guarantee**: During the C_old,new phase, both the old and new configurations must independently reach quorum for any decision. This mathematically prevents two leaders from existing in the same term.
2. **Flexibility**: Supports adding and removing nodes in a single operation (though we restrict to one change at a time for simplicity).
3. **Production proven**: TiKV and CockroachDB both use joint consensus.

## How It Works
1. Leader proposes a `ConfigChange` entry containing both C_old and C_new
2. While this entry is uncommitted, quorum requires majorities in **both** configs
3. Once committed, leader proposes a final entry with only C_new
4. Once C_new is committed, nodes not in C_new can shut down

## Consequences
- One config change at a time (concurrent changes are rejected)
- Two log entries per membership change (slight latency overhead)
- `try_advance_commit` must check quorum against both configs during joint phase

## What Would Change This Decision
If we needed to change multiple nodes simultaneously (e.g., replacing 2 of 3 nodes), we would need to batch changes into a single joint consensus round. The current implementation handles this correctly since `begin_change` computes C_new from the full diff.
