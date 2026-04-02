use raft_common::types::NodeId;
use std::time::Instant;

/// State for an in-progress leadership transfer.
///
/// Leadership transfer protocol:
/// 1. Admin/client calls TransferLeadership(target_node).
/// 2. Leader stops accepting new proposals.
/// 3. Leader sends AppendEntries to bring the target up to date.
/// 4. Once the target's log matches the leader's, leader sends a
///    TimeoutNow message telling the target to start an election immediately.
/// 5. The target starts an election (skipping pre-vote), wins, and becomes leader.
/// 6. The old leader steps down when it sees the new leader's term.
///
/// If the transfer doesn't complete within the election timeout, it's aborted.
#[derive(Debug)]
pub struct TransferState {
    /// The node we're transferring leadership to.
    pub target: NodeId,
    /// When the transfer was initiated (for timeout detection).
    pub started_at: Instant,
    /// Whether we've sent the TimeoutNow message to the target.
    pub timeout_now_sent: bool,
}

impl TransferState {
    pub fn new(target: NodeId) -> Self {
        Self {
            target,
            started_at: Instant::now(),
            timeout_now_sent: false,
        }
    }

    /// Check if the transfer has timed out.
    pub fn is_expired(&self, timeout_ms: u64) -> bool {
        self.started_at.elapsed().as_millis() as u64 > timeout_ms
    }
}

/// Result of initiating a leadership transfer.
#[derive(Debug, PartialEq, Eq)]
pub enum TransferError {
    /// This node is not the leader.
    NotLeader,
    /// A transfer is already in progress.
    AlreadyInProgress,
    /// The target node is not in the cluster.
    TargetNotInCluster,
    /// The target is already the leader.
    AlreadyLeader,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_state_creation() {
        let state = TransferState::new(2);
        assert_eq!(state.target, 2);
        assert!(!state.timeout_now_sent);
    }

    #[test]
    fn transfer_not_expired_immediately() {
        let state = TransferState::new(2);
        assert!(!state.is_expired(1000));
    }

    #[test]
    fn transfer_expires() {
        let state = TransferState::new(2);
        // Use 0ms timeout to force expiration
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(state.is_expired(0));
    }
}
