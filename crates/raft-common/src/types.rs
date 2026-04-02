/// Unique identifier for a node in the Raft cluster.
pub type NodeId = u64;

/// Raft term number. Monotonically increasing.
pub type Term = u64;

/// Index into the Raft log. 1-indexed.
pub type LogIndex = u64;

/// Monotonically increasing revision number for MVCC.
pub type Revision = u64;
