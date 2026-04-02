use raft_common::types::{NodeId, Term};

/// The role of a Raft node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

/// Persistent and volatile state for a Raft node.
pub struct RaftState {
    pub id: NodeId,
    pub role: Role,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub leader_id: Option<NodeId>,
}
