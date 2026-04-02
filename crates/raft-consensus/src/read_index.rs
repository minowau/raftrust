use raft_common::types::{LogIndex, NodeId};
use std::collections::{HashMap, HashSet};

/// A pending read request waiting for leadership confirmation.
#[derive(Debug)]
pub struct ReadIndexRequest {
    /// The commit index at the time the read was requested.
    pub read_index: LogIndex,
    /// Nodes that have acknowledged this leader's heartbeat.
    pub acks: HashSet<NodeId>,
    /// Total cluster size for quorum calculation.
    pub cluster_size: usize,
}

impl ReadIndexRequest {
    pub fn new(read_index: LogIndex, self_id: NodeId, cluster_size: usize) -> Self {
        let mut acks = HashSet::new();
        acks.insert(self_id); // Leader counts itself
        Self {
            read_index,
            acks,
            cluster_size,
        }
    }

    /// Record a heartbeat acknowledgement from a peer.
    /// Returns true if we now have quorum confirmation.
    pub fn record_ack(&mut self, from: NodeId) -> bool {
        self.acks.insert(from);
        self.has_quorum()
    }

    /// Check if a quorum of nodes have confirmed this leader's authority.
    pub fn has_quorum(&self) -> bool {
        self.acks.len() >= self.quorum_size()
    }

    fn quorum_size(&self) -> usize {
        self.cluster_size / 2 + 1
    }
}

/// Manages pending read index requests.
///
/// The read index protocol ensures linearizable reads:
/// 1. Client sends a read request to the leader.
/// 2. Leader records its current commit_index as the "read index".
/// 3. Leader sends heartbeats to all peers and waits for quorum ack.
/// 4. Once quorum confirms, the leader knows it's still the leader.
/// 5. Leader waits for its state machine to advance past read_index.
/// 6. Leader serves the read from the state machine.
///
/// This avoids the cost of going through Raft log for every read while
/// still providing linearizability guarantees.
#[derive(Debug)]
pub struct ReadIndexState {
    /// Counter for assigning unique IDs to read requests.
    next_id: u64,
    /// Pending read requests: id -> request.
    pending: HashMap<u64, ReadIndexRequest>,
}

impl ReadIndexState {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            pending: HashMap::new(),
        }
    }

    /// Register a new read index request. Returns the request ID.
    pub fn register(&mut self, read_index: LogIndex, self_id: NodeId, cluster_size: usize) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.pending
            .insert(id, ReadIndexRequest::new(read_index, self_id, cluster_size));
        id
    }

    /// Record a heartbeat ack for all pending reads.
    /// Returns IDs of requests that now have quorum.
    pub fn record_ack(&mut self, from: NodeId) -> Vec<u64> {
        let mut confirmed = Vec::new();
        for (&id, req) in &mut self.pending {
            if req.record_ack(from) {
                confirmed.push(id);
            }
        }
        confirmed
    }

    /// Get the read index for a confirmed request and remove it.
    pub fn take_confirmed(&mut self, id: u64) -> Option<LogIndex> {
        self.pending.remove(&id).map(|req| req.read_index)
    }

    /// Get the read index for a request without removing it.
    pub fn get_read_index(&self, id: u64) -> Option<LogIndex> {
        self.pending.get(&id).map(|req| req.read_index)
    }

    /// Remove a pending request (e.g., on timeout or leader step-down).
    pub fn remove(&mut self, id: u64) -> Option<ReadIndexRequest> {
        self.pending.remove(&id)
    }

    /// Clear all pending reads (e.g., when stepping down from leader).
    pub fn clear(&mut self) {
        self.pending.clear();
    }

    /// Number of pending read requests.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Check if a specific request has quorum.
    pub fn is_confirmed(&self, id: u64) -> bool {
        self.pending.get(&id).is_some_and(|req| req.has_quorum())
    }
}

impl Default for ReadIndexState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_read_index() {
        let mut state = ReadIndexState::new();
        // Single node cluster: leader's self-ack is quorum
        let id = state.register(10, 1, 1);
        assert!(state.is_confirmed(id));
        assert_eq!(state.take_confirmed(id), Some(10));
    }

    #[test]
    fn three_node_read_index() {
        let mut state = ReadIndexState::new();
        let id = state.register(5, 1, 3);

        // Only self-ack, not quorum yet
        assert!(!state.is_confirmed(id));

        // Ack from node 2 gives us 2/3 = quorum
        let confirmed = state.record_ack(2);
        assert_eq!(confirmed, vec![id]);
        assert!(state.is_confirmed(id));
        assert_eq!(state.take_confirmed(id), Some(5));
    }

    #[test]
    fn five_node_read_index() {
        let mut state = ReadIndexState::new();
        let id = state.register(7, 1, 5);

        assert!(!state.is_confirmed(id));

        state.record_ack(2);
        assert!(!state.is_confirmed(id)); // 2/5, need 3

        let confirmed = state.record_ack(3);
        assert_eq!(confirmed, vec![id]); // 3/5, quorum!
        assert!(state.is_confirmed(id));
    }

    #[test]
    fn multiple_pending_reads() {
        let mut state = ReadIndexState::new();
        let id1 = state.register(5, 1, 3);
        let id2 = state.register(8, 1, 3);

        assert_eq!(state.pending_count(), 2);

        // One ack confirms both
        let confirmed = state.record_ack(2);
        assert_eq!(confirmed.len(), 2);
        assert!(confirmed.contains(&id1));
        assert!(confirmed.contains(&id2));
    }

    #[test]
    fn clear_on_step_down() {
        let mut state = ReadIndexState::new();
        state.register(5, 1, 3);
        state.register(8, 1, 3);
        assert_eq!(state.pending_count(), 2);

        state.clear();
        assert_eq!(state.pending_count(), 0);
    }

    #[test]
    fn duplicate_acks_ignored() {
        let mut state = ReadIndexState::new();
        let id = state.register(5, 1, 3);

        state.record_ack(2);
        assert!(state.is_confirmed(id));

        // Duplicate ack from node 2 is harmless
        state.record_ack(2);
        assert!(state.is_confirmed(id));
    }

    #[test]
    fn take_removes_request() {
        let mut state = ReadIndexState::new();
        let id = state.register(5, 1, 1);
        assert!(state.is_confirmed(id));

        assert_eq!(state.take_confirmed(id), Some(5));
        assert_eq!(state.take_confirmed(id), None); // Already taken
        assert_eq!(state.pending_count(), 0);
    }
}
