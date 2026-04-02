use raft_common::types::{LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// The role of a Raft node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

/// Persistent state — must be durably stored before responding to RPCs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }

    pub fn load(path: &std::path::Path) -> Self {
        if path.exists() {
            let data = std::fs::read_to_string(path).unwrap_or_default();
            serde_json::from_str(&data).unwrap_or_else(|_| Self::new())
        } else {
            Self::new()
        }
    }

    pub fn save(&self, path: &std::path::Path) {
        let data = serde_json::to_string(self).unwrap();
        let tmp = path.with_extension("tmp");
        let _ = std::fs::write(&tmp, &data);
        let _ = std::fs::rename(&tmp, path);
    }
}

/// Volatile state on all servers.
#[derive(Debug)]
pub struct RaftState {
    pub id: NodeId,
    pub role: Role,
    pub persistent: PersistentState,
    pub leader_id: Option<NodeId>,
    /// Index of highest log entry known to be committed.
    pub commit_index: LogIndex,
    /// Index of highest log entry applied to state machine.
    pub last_applied: LogIndex,
}

/// Volatile state on leaders (reinitialized after election).
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each peer: index of the next log entry to send.
    pub next_index: std::collections::HashMap<NodeId, LogIndex>,
    /// For each peer: index of highest log entry known to be replicated.
    pub match_index: std::collections::HashMap<NodeId, LogIndex>,
}

impl LeaderState {
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        let mut next_index = std::collections::HashMap::new();
        let mut match_index = std::collections::HashMap::new();
        for &peer in peers {
            next_index.insert(peer, last_log_index + 1);
            match_index.insert(peer, 0);
        }
        Self {
            next_index,
            match_index,
        }
    }
}

/// Tracks votes received during an election.
#[derive(Debug)]
pub struct ElectionState {
    pub votes_received: HashSet<NodeId>,
    pub votes_needed: usize,
}

impl ElectionState {
    pub fn new(cluster_size: usize) -> Self {
        Self {
            votes_received: HashSet::new(),
            votes_needed: cluster_size / 2 + 1,
        }
    }

    pub fn record_vote(&mut self, from: NodeId) -> bool {
        self.votes_received.insert(from);
        self.has_quorum()
    }

    pub fn has_quorum(&self) -> bool {
        self.votes_received.len() >= self.votes_needed
    }
}
