# Raft Consensus Internals

This document describes how the Raft consensus algorithm is implemented in this project. It covers the core state machine, election protocol, log replication, and the Phase 6 extensions (linearizable reads, leadership transfer, joint consensus).

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                    RaftNode                           │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────┐ │
│  │ RaftState │  │ RaftLog  │  │   LeaderState      │ │
│  │ role      │  │ WAL      │  │   next_index[]     │ │
│  │ term      │  │ entries  │  │   match_index[]    │ │
│  │ voted_for │  │ snapshot │  │                    │ │
│  └──────────┘  └──────────┘  └────────────────────┘ │
│  ┌──────────────┐ ┌───────────────┐ ┌─────────────┐ │
│  │ReadIndexState│ │ TransferState │ │ Membership  │ │
│  │  pending[]   │ │  target       │ │  current    │ │
│  │              │ │  timeout_now  │ │  pending    │ │
│  └──────────────┘ └───────────────┘ └─────────────┘ │
└──────────────────────────────────────────────────────┘
         ▲                    ▲
         │ gRPC               │ gRPC
    ┌────┴────┐          ┌────┴────┐
    │  Peers  │          │ Clients │
    └─────────┘          └─────────┘
```

All mutable state is protected by a single `parking_lot::Mutex` on `RaftNodeInner`. This simplifies reasoning about concurrency — every method call is serialized, matching the Raft paper's assumption of a single-threaded state machine.

## State Machine Roles

```
                    timeout
  ┌───────────┐  ────────────►  ┌──────────────┐
  │  Follower │                 │ PreCandidate │
  └─────┬─────┘  ◄────────────  └──────┬───────┘
        │          higher term         │ quorum pre-vote
        │                              ▼
        │                       ┌──────────────┐
        │    higher term        │  Candidate   │
        │  ◄────────────────    └──────┬───────┘
        │                              │ quorum vote
        │                              ▼
        │                       ┌──────────────┐
        └───────────────────    │    Leader    │
              higher term       └──────────────┘
```

### Pre-vote (§9.6)
Before starting a real election, a node sends pre-vote requests at `term+1` without incrementing its own term. This prevents a partitioned node from incrementing its term and disrupting the cluster when it rejoins.

Pre-vote requests are evaluated like normal votes but:
- The receiver does not update its state
- The candidate does not increment its term
- Only if a majority grants pre-votes does the candidate proceed to a real election

## Leader Election

1. **Election timeout fires** → node becomes PreCandidate
2. **Pre-vote**: send `RequestVote(term+1, is_pre_vote=true)` to all peers
3. **Pre-vote quorum** → node becomes Candidate
4. **Real election**: increment term, vote for self, send `RequestVote(term)` to all peers
5. **Vote quorum** → node becomes Leader
6. **Leader initialization**: append a no-op entry at the new term (ensures leader can commit entries from previous terms per §5.4.2)

### Vote granting rules
A node grants a vote if:
- The candidate's term ≥ the node's current term
- The node hasn't voted for anyone else in this term (or voted for this candidate)
- The candidate's log is at least as up-to-date (compared by last entry's term, then index)

## Log Replication

The leader replicates entries via `AppendEntries` RPCs:

```
Leader                    Follower
  │                          │
  │  AppendEntries(          │
  │    term, prev_idx,       │
  │    prev_term, entries[], │
  │    leader_commit)        │
  │ ────────────────────────►│
  │                          │ Consistency check:
  │                          │   log[prev_idx].term == prev_term?
  │  AppendResponse(         │
  │    success, match_index) │
  │ ◄────────────────────────│
  │                          │
```

### Commit advancement
The leader advances `commit_index` to the highest N where:
- A quorum of nodes has `match_index ≥ N`
- `log[N].term == currentTerm` (safety requirement from §5.4.2)

During **joint consensus**, quorum requires independent majorities in both the old and new configurations.

## Linearizable Reads (Read Index Protocol)

Reads from the leader could be stale if a network partition has occurred and a new leader has been elected. The read index protocol prevents this:

1. Client sends read request to leader
2. Leader records `read_index = commit_index`
3. Leader sends heartbeats to all peers
4. Once a **quorum** acknowledges the heartbeat, the leader knows it's still authoritative
5. Leader waits for `last_applied ≥ read_index`
6. Leader serves the read from the state machine

This provides linearizable reads without writing to the Raft log, maintaining high read throughput.

## Leadership Transfer

Graceful leadership handoff for planned maintenance:

1. Admin calls `TransferLeadership(target)`
2. Leader stops accepting new proposals
3. Leader replicates to bring the target's log up to date
4. Once target's `match_index == leader's last_index`, leader sends `TimeoutNow`
5. Target starts an immediate election (skips pre-vote)
6. Target wins election, old leader steps down on seeing new term

The transfer has a timeout (election timeout). If the target doesn't become leader in time, the transfer is aborted and the leader resumes normal operation.

## Joint Consensus (Membership Changes)

Adding or removing nodes uses a two-phase protocol:

```
Phase 1: C_old → C_old,new
  - Leader proposes ConfigChange entry with both configs
  - Quorum requires majorities in BOTH C_old AND C_new
  - Once committed, proceed to Phase 2

Phase 2: C_old,new → C_new
  - Leader proposes entry with only C_new
  - Once committed, nodes not in C_new shut down
  - Cluster operates solely under C_new
```

This guarantees that at no point can two independent majorities exist, preventing split-brain.

## Snapshots and Log Compaction

When the log grows beyond a threshold:
1. Leader serializes the state machine into a snapshot
2. Log entries up to the snapshot index are discarded
3. Lagging followers receive the snapshot via `InstallSnapshot` RPC
4. Snapshot integrity is verified via CRC32 checksum

## Thread Safety Model

The `RaftNode` uses a single `Mutex<RaftNodeInner>` to protect all mutable state. This means:
- No data races by construction
- Every public method acquires the lock, performs its work, and releases it
- The lock is held for short durations (no I/O under lock, except state persistence)
- The server's event loop runs on a separate tokio task and calls `RaftNode` methods

This matches the Raft paper's model where the state machine processes one event at a time.
