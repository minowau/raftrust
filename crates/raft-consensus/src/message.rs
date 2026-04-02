use raft_common::types::{LogIndex, NodeId, Term};

/// Internal message types used within the Raft node.
/// These are the logical messages; the RPC layer converts to/from protobuf.

#[derive(Debug, Clone)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
    pub is_pre_vote: bool,
}

#[derive(Debug, Clone)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone)]
pub struct AppendResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub data: Vec<u8>,
    pub entry_type: EntryType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    Normal,
    ConfigChange,
    Noop,
}

/// A proposal from a client to be replicated via Raft.
#[derive(Debug)]
pub struct Proposal {
    pub data: Vec<u8>,
    pub response_tx: tokio::sync::oneshot::Sender<ProposalResult>,
}

#[derive(Debug)]
pub enum ProposalResult {
    Success { index: LogIndex },
    NotLeader { leader_id: Option<NodeId> },
    Error(String),
}
