use raft_common::types::{LogIndex, Term};

use crate::message::VoteRequest;
use crate::state::RaftState;

/// Check whether to grant a vote to a candidate.
///
/// Grant the vote if:
/// 1. The candidate's term >= our current term
/// 2. We haven't voted for someone else in this term
/// 3. The candidate's log is at least as up-to-date as ours
pub fn should_grant_vote(
    state: &RaftState,
    request: &VoteRequest,
    our_last_log_index: LogIndex,
    our_last_log_term: Term,
) -> bool {
    // Term check
    if request.term < state.persistent.current_term {
        return false;
    }

    // Already voted for someone else in this term?
    if let Some(voted_for) = state.persistent.voted_for {
        if voted_for != request.candidate_id {
            return false;
        }
    }

    // Log up-to-date check (§5.4.1):
    // Candidate's log is at least as up-to-date if:
    // - Its last log term is greater, OR
    // - Its last log term is equal and its last log index >= ours
    is_log_up_to_date(
        request.last_log_term,
        request.last_log_index,
        our_last_log_term,
        our_last_log_index,
    )
}

/// Check if candidate's log is at least as up-to-date as ours.
pub fn is_log_up_to_date(
    candidate_last_term: Term,
    candidate_last_index: LogIndex,
    our_last_term: Term,
    our_last_index: LogIndex,
) -> bool {
    if candidate_last_term != our_last_term {
        candidate_last_term > our_last_term
    } else {
        candidate_last_index >= our_last_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{PersistentState, RaftState, Role};
    use raft_common::types::NodeId;

    fn make_state(term: Term, voted_for: Option<NodeId>) -> RaftState {
        RaftState {
            id: 1,
            role: Role::Follower,
            persistent: PersistentState {
                current_term: term,
                voted_for,
            },
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
        }
    }

    #[test]
    fn grant_vote_to_up_to_date_candidate() {
        let state = make_state(1, None);
        let req = VoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 5,
            last_log_term: 1,
            is_pre_vote: false,
        };
        assert!(should_grant_vote(&state, &req, 5, 1));
    }

    #[test]
    fn reject_stale_term() {
        let state = make_state(3, None);
        let req = VoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 2,
            is_pre_vote: false,
        };
        assert!(!should_grant_vote(&state, &req, 5, 1));
    }

    #[test]
    fn reject_already_voted() {
        let state = make_state(2, Some(3)); // voted for node 3
        let req = VoteRequest {
            term: 2,
            candidate_id: 2, // different candidate
            last_log_index: 10,
            last_log_term: 2,
            is_pre_vote: false,
        };
        assert!(!should_grant_vote(&state, &req, 5, 1));
    }

    #[test]
    fn grant_same_candidate() {
        let state = make_state(2, Some(2)); // already voted for this candidate
        let req = VoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 2,
            is_pre_vote: false,
        };
        assert!(should_grant_vote(&state, &req, 5, 1));
    }

    #[test]
    fn reject_outdated_log() {
        let state = make_state(1, None);
        let req = VoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 3,
            last_log_term: 1,
            is_pre_vote: false,
        };
        // Our log is at (index=5, term=2), candidate at (3, 1) — ours is more up-to-date
        assert!(!should_grant_vote(&state, &req, 5, 2));
    }

    #[test]
    fn log_up_to_date_checks() {
        // Higher term wins
        assert!(is_log_up_to_date(3, 1, 2, 100));
        assert!(!is_log_up_to_date(1, 100, 2, 1));

        // Same term, higher index wins
        assert!(is_log_up_to_date(2, 10, 2, 5));
        assert!(!is_log_up_to_date(2, 3, 2, 5));

        // Equal
        assert!(is_log_up_to_date(2, 5, 2, 5));
    }
}
