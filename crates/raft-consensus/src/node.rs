use parking_lot::Mutex;
use raft_common::types::{LogIndex, NodeId, Term};
use std::path::Path;
use tracing::{debug, info, warn};

use crate::election::should_grant_vote;
use crate::leadership_transfer::TransferState;
use crate::log::RaftLog;
use crate::membership::{ClusterConfig, ConfigChange, MembershipError, MembershipState};
use crate::message::*;
use crate::read_index::ReadIndexState;
use crate::state::*;
use crate::tick::TickConfig;

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub data_dir: String,
    pub tick_config: TickConfig,
}

/// The core Raft node. Thread-safe via internal locking.
///
/// This implements the Raft algorithm as described in the paper:
/// - Leader election with pre-vote
/// - Log replication with consistency checks
/// - Commit index advancement via quorum
pub struct RaftNode {
    inner: Mutex<RaftNodeInner>,
    config: NodeConfig,
}

struct RaftNodeInner {
    state: RaftState,
    log: RaftLog,
    leader_state: Option<LeaderState>,
    election_state: Option<ElectionState>,
    read_index_state: ReadIndexState,
    transfer_state: Option<TransferState>,
    membership: MembershipState,
}

impl RaftNode {
    /// Create a new Raft node.
    pub fn new(config: NodeConfig) -> raft_common::error::Result<Self> {
        let data_dir = Path::new(&config.data_dir);
        std::fs::create_dir_all(data_dir)?;

        let persistent_path = data_dir.join("raft_state.json");
        let persistent = PersistentState::load(&persistent_path);

        let wal_path = data_dir.join("raft_log.wal");
        let log = RaftLog::open(&wal_path)?;

        let state = RaftState {
            id: config.id,
            role: Role::Follower,
            persistent,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
        };

        let membership = MembershipState::new(config.id, &config.peers);

        Ok(Self {
            inner: Mutex::new(RaftNodeInner {
                state,
                log,
                leader_state: None,
                election_state: None,
                read_index_state: ReadIndexState::new(),
                transfer_state: None,
                membership,
            }),
            config,
        })
    }

    /// Get the current role.
    pub fn role(&self) -> Role {
        self.inner.lock().state.role
    }

    /// Get the current term.
    pub fn term(&self) -> Term {
        self.inner.lock().state.persistent.current_term
    }

    /// Get the current leader ID.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.inner.lock().state.leader_id
    }

    /// Get the commit index.
    pub fn commit_index(&self) -> LogIndex {
        self.inner.lock().state.commit_index
    }

    /// Get this node's ID.
    pub fn id(&self) -> NodeId {
        self.config.id
    }

    /// Cluster size (including self).
    pub fn cluster_size(&self) -> usize {
        self.config.peers.len() + 1
    }

    /// Get peer IDs.
    pub fn peers(&self) -> &[NodeId] {
        &self.config.peers
    }

    // ── Election ──

    /// Start a pre-vote phase. Called when election timeout fires.
    /// Returns vote requests to send to all peers.
    pub fn start_pre_vote(&self) -> Vec<(NodeId, VoteRequest)> {
        let mut inner = self.inner.lock();
        inner.state.role = Role::PreCandidate;

        let mut election = ElectionState::new(self.cluster_size());
        election.record_vote(self.config.id); // vote for self

        let request = VoteRequest {
            term: inner.state.persistent.current_term + 1, // hypothetical next term
            candidate_id: self.config.id,
            last_log_index: inner.log.last_index(),
            last_log_term: inner.log.last_term(),
            is_pre_vote: true,
        };

        inner.election_state = Some(election);

        self.config
            .peers
            .iter()
            .map(|&peer| (peer, request.clone()))
            .collect()
    }

    /// Start a real election. Called after pre-vote succeeds.
    /// Returns vote requests to send to all peers.
    pub fn start_election(&self) -> Vec<(NodeId, VoteRequest)> {
        let mut inner = self.inner.lock();

        // Increment term
        inner.state.persistent.current_term += 1;
        inner.state.persistent.voted_for = Some(self.config.id);
        inner.state.role = Role::Candidate;
        inner.state.leader_id = None;

        self.save_persistent(&inner.state.persistent);

        let mut election = ElectionState::new(self.cluster_size());
        election.record_vote(self.config.id);

        let request = VoteRequest {
            term: inner.state.persistent.current_term,
            candidate_id: self.config.id,
            last_log_index: inner.log.last_index(),
            last_log_term: inner.log.last_term(),
            is_pre_vote: false,
        };

        inner.election_state = Some(election);

        info!(
            node = self.config.id,
            term = inner.state.persistent.current_term,
            "Starting election"
        );

        self.config
            .peers
            .iter()
            .map(|&peer| (peer, request.clone()))
            .collect()
    }

    /// Handle a vote response from a peer. Returns true if we became leader.
    pub fn handle_vote_response(
        &self,
        from: NodeId,
        response: VoteResponse,
        was_pre_vote: bool,
    ) -> bool {
        let mut inner = self.inner.lock();

        // Step down if we see a higher term
        if response.term > inner.state.persistent.current_term {
            self.step_down_inner(&mut inner, response.term);
            return false;
        }

        if !response.vote_granted {
            return false;
        }

        if let Some(ref mut election) = inner.election_state {
            let has_quorum = election.record_vote(from);

            if has_quorum {
                if was_pre_vote {
                    // Pre-vote succeeded — start real election
                    // (caller should call start_election)
                    debug!(
                        node = self.config.id,
                        "Pre-vote succeeded, starting real election"
                    );
                    return false; // signal to start real election handled by caller
                }

                // Won the real election — become leader
                self.become_leader_inner(&mut inner);
                return true;
            }
        }

        false
    }

    /// Check if pre-vote has quorum (caller uses this to decide whether to start real election).
    pub fn pre_vote_has_quorum(&self) -> bool {
        let inner = self.inner.lock();
        inner
            .election_state
            .as_ref()
            .is_some_and(|e| e.has_quorum())
    }

    // ── Vote handling ──

    /// Handle an incoming vote request. Returns the response.
    pub fn handle_vote_request(&self, request: VoteRequest) -> VoteResponse {
        let mut inner = self.inner.lock();

        // If this is a pre-vote, don't modify our state
        if request.is_pre_vote {
            let would_grant = request.term > inner.state.persistent.current_term
                && crate::election::is_log_up_to_date(
                    request.last_log_term,
                    request.last_log_index,
                    inner.log.last_term(),
                    inner.log.last_index(),
                );
            return VoteResponse {
                term: inner.state.persistent.current_term,
                vote_granted: would_grant,
            };
        }

        // Step down if we see a higher term
        if request.term > inner.state.persistent.current_term {
            self.step_down_inner(&mut inner, request.term);
        }

        let grant = should_grant_vote(
            &inner.state,
            &request,
            inner.log.last_index(),
            inner.log.last_term(),
        );

        if grant {
            inner.state.persistent.voted_for = Some(request.candidate_id);
            self.save_persistent(&inner.state.persistent);
            debug!(
                node = self.config.id,
                candidate = request.candidate_id,
                term = request.term,
                "Granted vote"
            );
        }

        VoteResponse {
            term: inner.state.persistent.current_term,
            vote_granted: grant,
        }
    }

    // ── AppendEntries ──

    /// Handle an AppendEntries request from the leader.
    pub fn handle_append_entries(&self, request: AppendRequest) -> AppendResponse {
        let mut inner = self.inner.lock();

        // Reject if term < currentTerm
        if request.term < inner.state.persistent.current_term {
            return AppendResponse {
                term: inner.state.persistent.current_term,
                success: false,
                match_index: 0,
            };
        }

        // Step down if we see a higher or equal term from a leader
        if request.term >= inner.state.persistent.current_term {
            if request.term > inner.state.persistent.current_term {
                self.step_down_inner(&mut inner, request.term);
            }
            inner.state.role = Role::Follower;
            inner.state.leader_id = Some(request.leader_id);
        }

        // Log consistency check
        if request.prev_log_index > 0
            && !inner
                .log
                .match_term(request.prev_log_index, request.prev_log_term)
        {
            return AppendResponse {
                term: inner.state.persistent.current_term,
                success: false,
                match_index: inner.log.last_index(),
            };
        }

        // Append new entries (handle conflicts)
        if !request.entries.is_empty() {
            // Find point of conflict
            let mut new_entries = Vec::new();
            for entry in request.entries {
                if let Some(existing) = inner.log.get(entry.index) {
                    if existing.term != entry.term {
                        // Conflict — truncate from here
                        inner.log.truncate_after(entry.index - 1);
                        new_entries.push(entry);
                    }
                    // If terms match, entry already present — skip
                } else {
                    new_entries.push(entry);
                }
            }

            if !new_entries.is_empty() {
                let _ = inner.log.append(new_entries);
            }
        }

        // Update commit index
        if request.leader_commit > inner.state.commit_index {
            inner.state.commit_index = std::cmp::min(request.leader_commit, inner.log.last_index());
        }

        AppendResponse {
            term: inner.state.persistent.current_term,
            success: true,
            match_index: inner.log.last_index(),
        }
    }

    // ── Leader operations ──

    /// Propose a new entry (client write). Only valid on the leader.
    pub fn propose(&self, data: Vec<u8>) -> Result<LogIndex, ProposalResult> {
        let mut inner = self.inner.lock();

        if inner.state.role != Role::Leader {
            return Err(ProposalResult::NotLeader {
                leader_id: inner.state.leader_id,
            });
        }

        // Reject proposals during leadership transfer
        if inner.transfer_state.is_some() {
            return Err(ProposalResult::TransferInProgress);
        }

        let index = inner.log.last_index() + 1;
        let entry = LogEntry {
            index,
            term: inner.state.persistent.current_term,
            data,
            entry_type: EntryType::Normal,
        };

        inner
            .log
            .append(vec![entry])
            .map_err(|e| ProposalResult::Error(e.to_string()))?;

        // Update own match_index
        let last_idx = inner.log.last_index();
        if let Some(ref mut leader) = inner.leader_state {
            leader.match_index.insert(self.config.id, last_idx);
        }

        Ok(index)
    }

    /// Generate AppendEntries requests for all peers (heartbeat or replication).
    pub fn create_append_requests(&self) -> Vec<(NodeId, AppendRequest)> {
        let inner = self.inner.lock();

        if inner.state.role != Role::Leader {
            return vec![];
        }

        let leader = match &inner.leader_state {
            Some(l) => l,
            None => return vec![],
        };

        let mut requests = Vec::new();
        for &peer in &self.config.peers {
            let next_idx = leader.next_index.get(&peer).copied().unwrap_or(1);
            let prev_log_index = next_idx.saturating_sub(1);
            let prev_log_term = inner.log.term_at(prev_log_index).unwrap_or(0);

            let entries: Vec<LogEntry> = inner.log.entries_from(next_idx).to_vec();

            requests.push((
                peer,
                AppendRequest {
                    term: inner.state.persistent.current_term,
                    leader_id: self.config.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: inner.state.commit_index,
                },
            ));
        }

        requests
    }

    /// Handle an AppendEntries response from a peer.
    pub fn handle_append_response(&self, from: NodeId, response: AppendResponse) {
        let mut inner = self.inner.lock();

        if response.term > inner.state.persistent.current_term {
            self.step_down_inner(&mut inner, response.term);
            return;
        }

        if inner.state.role != Role::Leader {
            return;
        }

        let leader = match inner.leader_state.as_mut() {
            Some(l) => l,
            None => return,
        };

        if response.success {
            leader.match_index.insert(from, response.match_index);
            leader.next_index.insert(from, response.match_index + 1);
        } else {
            // Decrement nextIndex and retry
            let next = leader.next_index.entry(from).or_insert(1);
            *next = next.saturating_sub(1).max(1);
        }

        // Try to advance commit index
        self.try_advance_commit(&mut inner);
    }

    /// Get entries that have been committed but not yet applied.
    pub fn take_committed_entries(&self) -> Vec<LogEntry> {
        let mut inner = self.inner.lock();
        let mut entries = Vec::new();

        while inner.state.last_applied < inner.state.commit_index {
            inner.state.last_applied += 1;
            if let Some(entry) = inner.log.get(inner.state.last_applied) {
                entries.push(entry.clone());
            }
        }

        entries
    }

    // ── Snapshots ──

    /// Trigger a snapshot at the current commit index.
    /// `snapshot_data` is the serialized state machine provided by the caller.
    /// Returns the snapshot metadata, or None if there's nothing to snapshot.
    pub fn trigger_snapshot(
        &self,
        snapshot_data: &[u8],
    ) -> Option<crate::snapshot::SnapshotMetadata> {
        let mut inner = self.inner.lock();
        let commit_index = inner.state.commit_index;
        if commit_index == 0 {
            return None;
        }

        let commit_term = inner.log.term_at(commit_index).unwrap_or(0);

        let snapshot_dir = Path::new(&self.config.data_dir).join("snapshots");
        let mgr = crate::snapshot::SnapshotManager::new(&snapshot_dir).ok()?;
        let meta = mgr
            .create_snapshot(commit_index, commit_term, snapshot_data)
            .ok()?;

        // Compact the log
        inner.log.compact(commit_index, commit_term);

        info!(
            node = self.config.id,
            index = commit_index,
            "Snapshot created, log compacted"
        );

        Some(meta)
    }

    /// Check if we should trigger a snapshot (log is too long).
    pub fn should_snapshot(&self, threshold: u64) -> bool {
        let inner = self.inner.lock();
        inner.log.len() as u64 > threshold
    }

    /// Handle an InstallSnapshot from the leader.
    /// Returns the current term.
    pub fn handle_install_snapshot(
        &self,
        leader_term: Term,
        leader_id: NodeId,
        metadata: &crate::snapshot::SnapshotMetadata,
        data: &[u8],
    ) -> Term {
        let mut inner = self.inner.lock();

        if leader_term < inner.state.persistent.current_term {
            return inner.state.persistent.current_term;
        }

        if leader_term > inner.state.persistent.current_term {
            self.step_down_inner(&mut inner, leader_term);
        }
        inner.state.role = Role::Follower;
        inner.state.leader_id = Some(leader_id);

        // If we already have this snapshot or beyond, ignore
        if metadata.last_included_index <= inner.log.snapshot_index() {
            return inner.state.persistent.current_term;
        }

        // Save snapshot to disk
        let snapshot_dir = Path::new(&self.config.data_dir).join("snapshots");
        if let Ok(mgr) = crate::snapshot::SnapshotManager::new(&snapshot_dir) {
            if mgr.receive_snapshot(metadata, data).is_err() {
                return inner.state.persistent.current_term;
            }
        }

        // Discard log entries up to the snapshot
        inner
            .log
            .compact(metadata.last_included_index, metadata.last_included_term);

        // Reset state machine index
        if metadata.last_included_index > inner.state.commit_index {
            inner.state.commit_index = metadata.last_included_index;
        }
        if metadata.last_included_index > inner.state.last_applied {
            inner.state.last_applied = metadata.last_included_index;
        }

        info!(
            node = self.config.id,
            snapshot_index = metadata.last_included_index,
            "Installed snapshot from leader"
        );

        inner.state.persistent.current_term
    }

    /// Get snapshot info for sending to a lagging follower.
    /// Returns (metadata, data) or None if no snapshot exists.
    pub fn get_snapshot_for_follower(
        &self,
    ) -> Option<(crate::snapshot::SnapshotMetadata, Vec<u8>)> {
        let snapshot_dir = Path::new(&self.config.data_dir).join("snapshots");
        let mgr = crate::snapshot::SnapshotManager::new(&snapshot_dir).ok()?;
        mgr.load_latest().ok()?
    }

    /// Get the snapshot index (for determining if a follower needs a snapshot).
    pub fn snapshot_index(&self) -> LogIndex {
        self.inner.lock().log.snapshot_index()
    }

    // ── Read Index (Linearizable Reads) ──

    /// Request a read index for linearizable reads.
    /// Returns (request_id, read_index) if this node is the leader.
    /// The caller must wait for heartbeat quorum confirmation before serving the read.
    pub fn request_read_index(&self) -> Result<(u64, LogIndex), ProposalResult> {
        let mut inner = self.inner.lock();

        if inner.state.role != Role::Leader {
            return Err(ProposalResult::NotLeader {
                leader_id: inner.state.leader_id,
            });
        }

        let read_index = inner.state.commit_index;
        let cluster_size = inner.membership.current_size();
        let id = inner
            .read_index_state
            .register(read_index, self.config.id, cluster_size);

        // For single-node clusters, the read is immediately confirmed
        Ok((id, read_index))
    }

    /// Record heartbeat acks for pending read index requests.
    /// Returns IDs of read requests that are now confirmed (have quorum).
    pub fn read_index_ack(&self, from: NodeId) -> Vec<u64> {
        let mut inner = self.inner.lock();
        inner.read_index_state.record_ack(from)
    }

    /// Check if a read index request is confirmed (has quorum).
    pub fn is_read_index_confirmed(&self, id: u64) -> bool {
        self.inner.lock().read_index_state.is_confirmed(id)
    }

    /// Take a confirmed read index and remove it from pending.
    pub fn take_read_index(&self, id: u64) -> Option<LogIndex> {
        self.inner.lock().read_index_state.take_confirmed(id)
    }

    /// Get the last applied index (for checking if state machine is caught up).
    pub fn last_applied(&self) -> LogIndex {
        self.inner.lock().state.last_applied
    }

    // ── Leadership Transfer ──

    /// Initiate a leadership transfer to the target node.
    /// The leader will stop accepting proposals and bring the target up to date.
    pub fn transfer_leadership(
        &self,
        target: NodeId,
    ) -> Result<(), crate::leadership_transfer::TransferError> {
        use crate::leadership_transfer::TransferError;

        let mut inner = self.inner.lock();

        if inner.state.role != Role::Leader {
            return Err(TransferError::NotLeader);
        }
        if target == self.config.id {
            return Err(TransferError::AlreadyLeader);
        }
        if inner.transfer_state.is_some() {
            return Err(TransferError::AlreadyInProgress);
        }
        if !inner.membership.is_voter(target) {
            return Err(TransferError::TargetNotInCluster);
        }

        info!(
            node = self.config.id,
            target = target,
            "Initiating leadership transfer"
        );
        inner.transfer_state = Some(TransferState::new(target));
        Ok(())
    }

    /// Check if the transfer target's log is caught up and we should send TimeoutNow.
    /// Returns Some(target) if we should send TimeoutNow, None otherwise.
    pub fn check_transfer_progress(&self) -> Option<NodeId> {
        let mut inner = self.inner.lock();

        let target = match inner.transfer_state {
            Some(ref t) if !t.timeout_now_sent => t.target,
            _ => return None,
        };

        let our_last = inner.log.last_index();
        let target_match = inner
            .leader_state
            .as_ref()
            .and_then(|l| l.match_index.get(&target).copied())
            .unwrap_or(0);

        if target_match >= our_last {
            if let Some(ref mut transfer) = inner.transfer_state {
                transfer.timeout_now_sent = true;
            }
            return Some(target);
        }

        None
    }

    /// Check if a leadership transfer has timed out and should be aborted.
    /// Returns true if the transfer was aborted.
    pub fn check_transfer_timeout(&self, timeout_ms: u64) -> bool {
        let mut inner = self.inner.lock();
        if let Some(ref transfer) = inner.transfer_state {
            if transfer.is_expired(timeout_ms) {
                warn!(
                    node = self.config.id,
                    target = transfer.target,
                    "Leadership transfer timed out, aborting"
                );
                inner.transfer_state = None;
                return true;
            }
        }
        false
    }

    /// Whether a leadership transfer is in progress (blocks new proposals).
    pub fn is_transfer_in_progress(&self) -> bool {
        self.inner.lock().transfer_state.is_some()
    }

    /// Handle a TimeoutNow message from the current leader.
    /// This node should immediately start an election (skip pre-vote).
    pub fn handle_timeout_now(&self, msg: TimeoutNow) -> Vec<(NodeId, VoteRequest)> {
        let mut inner = self.inner.lock();

        // Only handle if it's from a legitimate leader at current or higher term
        if msg.term < inner.state.persistent.current_term {
            return vec![];
        }

        info!(
            node = self.config.id,
            from = msg.leader_id,
            "Received TimeoutNow, starting immediate election"
        );

        // Start election immediately (skip pre-vote per Raft spec for transfers)
        inner.state.persistent.current_term += 1;
        inner.state.persistent.voted_for = Some(self.config.id);
        inner.state.role = Role::Candidate;
        inner.state.leader_id = None;

        self.save_persistent(&inner.state.persistent);

        let mut election = ElectionState::new(self.cluster_size());
        election.record_vote(self.config.id);

        let request = VoteRequest {
            term: inner.state.persistent.current_term,
            candidate_id: self.config.id,
            last_log_index: inner.log.last_index(),
            last_log_term: inner.log.last_term(),
            is_pre_vote: false,
        };

        inner.election_state = Some(election);

        self.config
            .peers
            .iter()
            .map(|&peer| (peer, request.clone()))
            .collect()
    }

    // ── Membership Changes (Joint Consensus) ──

    /// Propose a membership change (add or remove a node).
    /// Initiates joint consensus by proposing a C_old,new config entry.
    /// Returns the log index of the proposed config change entry.
    pub fn propose_config_change(&self, change: ConfigChange) -> Result<LogIndex, MembershipError> {
        let mut inner = self.inner.lock();

        if inner.state.role != Role::Leader {
            return Err(MembershipError::NotLeader);
        }

        let joint_data = inner.membership.begin_change(&change)?;
        let encoded = joint_data.encode();

        let index = inner.log.last_index() + 1;
        let entry = LogEntry {
            index,
            term: inner.state.persistent.current_term,
            data: encoded,
            entry_type: EntryType::ConfigChange,
        };

        inner
            .log
            .append(vec![entry])
            .map_err(|_| MembershipError::InvalidConfigData)?;

        // Update own match_index
        let last_idx = inner.log.last_index();
        if let Some(ref mut leader) = inner.leader_state {
            leader.match_index.insert(self.config.id, last_idx);
        }

        info!(
            node = self.config.id,
            index = index,
            "Proposed config change entry (joint consensus)"
        );

        Ok(index)
    }

    /// Called when a config change entry is committed.
    /// If it's a joint config (C_old,new), proposes the final C_new entry.
    /// Returns the new config if a finalization entry was proposed.
    pub fn apply_config_change(&self, data: &[u8]) -> Option<ClusterConfig> {
        use crate::membership::JointConfigData;

        let mut inner = self.inner.lock();

        // Try to decode as joint config first
        if let Ok(joint) = JointConfigData::decode(data) {
            // This is the C_old,new entry being committed.
            // Apply the joint config and, if we're leader, propose C_new.
            inner.membership.pending = Some(joint.new.clone());

            if inner.state.role == Role::Leader {
                // Finalize: propose C_new entry
                if let Some(new_config) = inner.membership.finalize_joint() {
                    let encoded = serde_json::to_vec(&new_config).unwrap_or_default();
                    let index = inner.log.last_index() + 1;
                    let entry = LogEntry {
                        index,
                        term: inner.state.persistent.current_term,
                        data: encoded,
                        entry_type: EntryType::ConfigChange,
                    };
                    let _ = inner.log.append(vec![entry]);

                    let last_idx = inner.log.last_index();
                    if let Some(ref mut leader) = inner.leader_state {
                        leader.match_index.insert(self.config.id, last_idx);
                    }

                    info!(node = self.config.id, "Proposed final config (C_new) entry");
                    return Some(new_config);
                }
            }
            return None;
        }

        // Try to decode as final config (C_new)
        if let Ok(new_config) = serde_json::from_slice::<ClusterConfig>(data) {
            inner.membership.apply_new_config(new_config.clone());
            info!(
                node = self.config.id,
                members = ?new_config.member_ids(),
                "Applied new cluster configuration"
            );
            return Some(new_config);
        }

        warn!(
            node = self.config.id,
            "Failed to decode config change entry"
        );
        None
    }

    /// Get the current membership state.
    pub fn membership(&self) -> MembershipState {
        self.inner.lock().membership.clone()
    }

    /// Check if this node is still a voter in the current configuration.
    pub fn is_voter(&self) -> bool {
        self.inner.lock().membership.is_voter(self.config.id)
    }

    // ── Internal helpers ──

    fn become_leader_inner(&self, inner: &mut RaftNodeInner) {
        info!(
            node = self.config.id,
            term = inner.state.persistent.current_term,
            "Became leader"
        );

        inner.state.role = Role::Leader;
        inner.state.leader_id = Some(self.config.id);
        inner.election_state = None;

        // Initialize leader state
        inner.leader_state = Some(LeaderState::new(&self.config.peers, inner.log.last_index()));

        // Append a no-op entry to commit entries from previous terms
        let noop = LogEntry {
            index: inner.log.last_index() + 1,
            term: inner.state.persistent.current_term,
            data: vec![],
            entry_type: EntryType::Noop,
        };
        let _ = inner.log.append(vec![noop]);

        // Update own match_index
        let last_idx = inner.log.last_index();
        if let Some(ref mut leader) = inner.leader_state {
            leader.match_index.insert(self.config.id, last_idx);
        }
    }

    fn step_down_inner(&self, inner: &mut RaftNodeInner, new_term: Term) {
        debug!(
            node = self.config.id,
            old_term = inner.state.persistent.current_term,
            new_term,
            "Stepping down"
        );
        inner.state.persistent.current_term = new_term;
        inner.state.persistent.voted_for = None;
        inner.state.role = Role::Follower;
        inner.state.leader_id = None;
        inner.leader_state = None;
        inner.election_state = None;
        inner.read_index_state.clear();
        inner.transfer_state = None;
        inner.membership.abort_change();
        self.save_persistent(&inner.state.persistent);
    }

    fn try_advance_commit(&self, inner: &mut RaftNodeInner) {
        let leader = match inner.leader_state.as_ref() {
            Some(l) => l,
            None => return,
        };

        // Find the highest N such that a quorum has match_index >= N
        // and log[N].term == currentTerm (§5.4.2).
        // During joint consensus, quorum requires majorities in BOTH configs.
        let mut match_indices: Vec<LogIndex> = leader.match_index.values().copied().collect();
        match_indices.sort_unstable_by(|a, b| b.cmp(a)); // descending

        for &candidate in &match_indices {
            if candidate <= inner.state.commit_index {
                break;
            }

            // Check if this candidate index has quorum
            let voters: std::collections::HashSet<NodeId> = leader
                .match_index
                .iter()
                .filter(|(_, &idx)| idx >= candidate)
                .map(|(&id, _)| id)
                .collect();

            if inner.membership.has_quorum(&voters) {
                if let Some(entry) = inner.log.get(candidate) {
                    if entry.term == inner.state.persistent.current_term {
                        inner.state.commit_index = candidate;
                        return;
                    }
                }
            }
        }
    }

    fn save_persistent(&self, persistent: &PersistentState) {
        let path = Path::new(&self.config.data_dir).join("raft_state.json");
        persistent.save(&path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: NodeId, peers: Vec<NodeId>, dir: &Path) -> RaftNode {
        let data_dir = dir.join(format!("node-{}", id));
        RaftNode::new(NodeConfig {
            id,
            peers,
            data_dir: data_dir.to_string_lossy().to_string(),
            tick_config: TickConfig::default(),
        })
        .unwrap()
    }

    #[test]
    fn initial_state() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![2, 3], dir.path());

        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 0);
        assert_eq!(node.leader_id(), None);
        assert_eq!(node.cluster_size(), 3);
    }

    #[test]
    fn single_node_election() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![], dir.path());

        // Single node: pre-vote gets self-vote, has quorum immediately
        let requests = node.start_pre_vote();
        assert!(requests.is_empty()); // no peers
        assert!(node.pre_vote_has_quorum());

        // Start real election
        let requests = node.start_election();
        assert!(requests.is_empty());

        // Single node wins immediately since it already has its own vote
        // In start_election, we record our own vote. For a single node,
        // that's already quorum, but we need to explicitly become leader.
        // The election_state has quorum, so handle_vote_response isn't needed.
        // Let's check: the election state should have quorum.
        assert_eq!(node.role(), Role::Candidate);
        // For single node, manually trigger leader transition:
        // In practice, the event loop checks quorum after self-vote.
        {
            let mut inner = node.inner.lock();
            if inner
                .election_state
                .as_ref()
                .is_some_and(|e| e.has_quorum())
            {
                node.become_leader_inner(&mut inner);
            }
        }
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.leader_id(), Some(1));
    }

    #[test]
    fn three_node_election() {
        let dir = tempfile::tempdir().unwrap();
        let node1 = make_node(1, vec![2, 3], dir.path());
        let node2 = make_node(2, vec![1, 3], dir.path());
        let node3 = make_node(3, vec![1, 2], dir.path());

        // Node 1 starts pre-vote
        let pre_vote_requests = node1.start_pre_vote();
        assert_eq!(pre_vote_requests.len(), 2);

        // Nodes 2 and 3 respond to pre-vote
        let resp2 = node2.handle_vote_request(pre_vote_requests[0].1.clone());
        let resp3 = node3.handle_vote_request(pre_vote_requests[1].1.clone());
        assert!(resp2.vote_granted);
        assert!(resp3.vote_granted);

        // Handle pre-vote responses
        node1.handle_vote_response(2, resp2, true);
        assert!(node1.pre_vote_has_quorum());

        // Now start real election
        let vote_requests = node1.start_election();
        assert_eq!(vote_requests.len(), 2);
        assert_eq!(node1.term(), 1);

        // Node 2 grants vote
        let resp2 = node2.handle_vote_request(vote_requests[0].1.clone());
        assert!(resp2.vote_granted);

        // Handle response — should become leader with 2/3 quorum
        let became_leader = node1.handle_vote_response(2, resp2, false);
        assert!(became_leader);
        assert_eq!(node1.role(), Role::Leader);
        assert_eq!(node1.term(), 1);
    }

    #[test]
    fn reject_vote_stale_term() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![2], dir.path());

        // Force node1 to term 5
        {
            let mut inner = node.inner.lock();
            inner.state.persistent.current_term = 5;
        }

        let req = VoteRequest {
            term: 3, // stale
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            is_pre_vote: false,
        };
        let resp = node.handle_vote_request(req);
        assert!(!resp.vote_granted);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn step_down_on_higher_term() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![2, 3], dir.path());

        // Become leader at term 1
        node.start_election();
        {
            let mut inner = node.inner.lock();
            node.become_leader_inner(&mut inner);
        }
        assert_eq!(node.role(), Role::Leader);

        // Receive AppendEntries from higher term
        let req = AppendRequest {
            term: 5,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        node.handle_append_entries(req);

        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 5);
        assert_eq!(node.leader_id(), Some(2));
    }

    #[test]
    fn log_replication() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_node(1, vec![2, 3], dir.path());
        let follower = make_node(2, vec![1, 3], dir.path());

        // Make node 1 leader
        leader.start_election();
        {
            let mut inner = leader.inner.lock();
            node_become_leader(&leader, &mut inner);
        }

        // Propose an entry
        let index = leader.propose(b"hello".to_vec()).unwrap();
        assert!(index > 0);

        // Generate append requests
        let requests = leader.create_append_requests();
        assert_eq!(requests.len(), 2);

        // Follower handles append
        let (_, req) = requests.into_iter().find(|(id, _)| *id == 2).unwrap();
        let resp = follower.handle_append_entries(req);
        assert!(resp.success);

        // Leader handles response
        leader.handle_append_response(2, resp);

        // With 2/3 replicas (leader + follower2), commit should advance
        assert!(leader.commit_index() > 0);
    }

    fn node_become_leader(node: &RaftNode, inner: &mut RaftNodeInner) {
        node.become_leader_inner(inner);
    }

    #[test]
    fn append_entries_consistency_check() {
        let dir = tempfile::tempdir().unwrap();
        let follower = make_node(1, vec![2], dir.path());

        // Follower has no entries — request with prev_log_index=5 should fail
        let req = AppendRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 5,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        };
        let resp = follower.handle_append_entries(req);
        assert!(!resp.success);

        // Request with prev_log_index=0 should succeed (empty log matches sentinel)
        let req = AppendRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                data: b"first".to_vec(),
                entry_type: EntryType::Normal,
            }],
            leader_commit: 0,
        };
        let resp = follower.handle_append_entries(req);
        assert!(resp.success);
        assert_eq!(resp.match_index, 1);
    }

    // ── Phase 6: Read Index Tests ──

    fn make_leader(dir: &Path) -> RaftNode {
        let node = make_node(1, vec![2, 3], dir);
        node.start_election();
        {
            let mut inner = node.inner.lock();
            node.become_leader_inner(&mut inner);
        }
        assert_eq!(node.role(), Role::Leader);
        node
    }

    #[test]
    fn read_index_single_node() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![], dir.path());
        node.start_election();
        {
            let mut inner = node.inner.lock();
            node.become_leader_inner(&mut inner);
        }

        let (id, _read_idx) = node.request_read_index().unwrap();
        // Single node: immediately confirmed
        assert!(node.is_read_index_confirmed(id));
    }

    #[test]
    fn read_index_three_node_quorum() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        let (id, _read_idx) = leader.request_read_index().unwrap();
        // Not yet confirmed — need 1 more ack
        assert!(!leader.is_read_index_confirmed(id));

        // Simulate heartbeat ack from node 2
        leader.read_index_ack(2);
        assert!(leader.is_read_index_confirmed(id));
    }

    #[test]
    fn read_index_rejected_on_follower() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![2, 3], dir.path());
        assert!(node.request_read_index().is_err());
    }

    #[test]
    fn read_index_cleared_on_step_down() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        let (id, _) = leader.request_read_index().unwrap();
        assert!(!leader.is_read_index_confirmed(id));

        // Step down via higher term
        let req = AppendRequest {
            term: 10,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        leader.handle_append_entries(req);
        assert_eq!(leader.role(), Role::Follower);

        // Read index should be cleared
        assert!(!leader.is_read_index_confirmed(id));
        assert_eq!(leader.take_read_index(id), None);
    }

    // ── Phase 6: Leadership Transfer Tests ──

    #[test]
    fn transfer_leadership_basic() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        assert!(leader.transfer_leadership(2).is_ok());
        assert!(leader.is_transfer_in_progress());
    }

    #[test]
    fn transfer_rejects_proposals() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        leader.transfer_leadership(2).unwrap();

        // Proposals should be rejected during transfer
        let result = leader.propose(b"data".to_vec());
        assert!(result.is_err());
    }

    #[test]
    fn transfer_rejects_non_leader() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![2, 3], dir.path());

        let result = node.transfer_leadership(2);
        assert!(result.is_err());
    }

    #[test]
    fn transfer_rejects_self() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        let result = leader.transfer_leadership(1); // self
        assert!(result.is_err());
    }

    #[test]
    fn transfer_rejects_unknown_target() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        let result = leader.transfer_leadership(99);
        assert!(result.is_err());
    }

    #[test]
    fn transfer_rejects_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        leader.transfer_leadership(2).unwrap();
        let result = leader.transfer_leadership(3);
        assert!(result.is_err());
    }

    #[test]
    fn transfer_timeout_now_sent_when_caught_up() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());
        let follower = make_node(2, vec![1, 3], dir.path());

        // Replicate to follower so it's caught up
        let requests = leader.create_append_requests();
        let (_, req) = requests.into_iter().find(|(id, _)| *id == 2).unwrap();
        let resp = follower.handle_append_entries(req);
        leader.handle_append_response(2, resp);

        // Start transfer
        leader.transfer_leadership(2).unwrap();

        // Target is caught up — should return target for TimeoutNow
        let target = leader.check_transfer_progress();
        assert_eq!(target, Some(2));

        // Second check: already sent
        assert_eq!(leader.check_transfer_progress(), None);
    }

    #[test]
    fn transfer_timeout_now_not_sent_when_behind() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        // Propose some entries (follower won't have them)
        leader.propose(b"data1".to_vec()).unwrap();
        leader.propose(b"data2".to_vec()).unwrap();

        leader.transfer_leadership(2).unwrap();

        // Target is behind — no TimeoutNow yet
        assert_eq!(leader.check_transfer_progress(), None);
    }

    #[test]
    fn handle_timeout_now_starts_election() {
        let dir = tempfile::tempdir().unwrap();
        let follower = make_node(2, vec![1, 3], dir.path());

        let msg = TimeoutNow {
            term: 1,
            leader_id: 1,
        };
        let vote_requests = follower.handle_timeout_now(msg);

        assert_eq!(follower.role(), Role::Candidate);
        assert_eq!(follower.term(), 1);
        assert_eq!(vote_requests.len(), 2); // Requests to peers 1 and 3
                                            // Verify requests are NOT pre-vote (skip pre-vote for transfers)
        for (_, req) in &vote_requests {
            assert!(!req.is_pre_vote);
        }
    }

    #[test]
    fn transfer_cleared_on_step_down() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        leader.transfer_leadership(2).unwrap();
        assert!(leader.is_transfer_in_progress());

        // Step down
        let req = AppendRequest {
            term: 10,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        leader.handle_append_entries(req);

        assert!(!leader.is_transfer_in_progress());
    }

    #[test]
    fn transfer_timeout_aborts() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        leader.transfer_leadership(2).unwrap();
        assert!(leader.is_transfer_in_progress());

        // Use 0ms timeout to force expiration
        std::thread::sleep(std::time::Duration::from_millis(1));
        let aborted = leader.check_transfer_timeout(0);
        assert!(aborted);
        assert!(!leader.is_transfer_in_progress());
    }

    // ── Phase 6: Membership Change Tests ──

    #[test]
    fn propose_add_node() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        let index = leader
            .propose_config_change(ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();
        assert!(index > 0);

        let membership = leader.membership();
        assert!(membership.in_joint_consensus());
    }

    #[test]
    fn propose_remove_node() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        let index = leader
            .propose_config_change(ConfigChange::RemoveNode { id: 3 })
            .unwrap();
        assert!(index > 0);

        let membership = leader.membership();
        assert!(membership.in_joint_consensus());
    }

    #[test]
    fn config_change_rejected_on_follower() {
        let dir = tempfile::tempdir().unwrap();
        let node = make_node(1, vec![2, 3], dir.path());

        let result = node.propose_config_change(ConfigChange::AddNode {
            id: 4,
            address: "127.0.0.1:5004".to_string(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn config_change_rejected_concurrent() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        leader
            .propose_config_change(ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();

        // Second change should be rejected
        let result = leader.propose_config_change(ConfigChange::AddNode {
            id: 5,
            address: "127.0.0.1:5005".to_string(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn joint_quorum_during_add() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());
        let follower2 = make_node(2, vec![1, 3], dir.path());
        let follower3 = make_node(3, vec![1, 2], dir.path());

        // Propose adding node 4
        leader
            .propose_config_change(ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();

        // During joint consensus, need quorum in both old (1,2,3) and new (1,2,3,4).
        // Replicate to follower 2 — gives us {1,2}: 2/3 old quorum, 2/4 new = NOT quorum
        let requests = leader.create_append_requests();
        let (_, req2) = requests.iter().find(|(id, _)| *id == 2).unwrap();
        let resp2 = follower2.handle_append_entries(req2.clone());
        leader.handle_append_response(2, resp2);

        // Commit index should NOT advance (need 3/4 in new config)
        // The no-op is committed but config change needs joint quorum
        // Actually, the leader has {1,2} matching. Old: 2/3 ok. New: 2/4 not ok.
        // But the no-op entry at index 1 was committed before joint config started.
        // The config change entry itself needs joint quorum to commit.

        // Now replicate to follower 3 as well
        let requests = leader.create_append_requests();
        let (_, req3) = requests.iter().find(|(id, _)| *id == 3).unwrap();
        let resp3 = follower3.handle_append_entries(req3.clone());
        leader.handle_append_response(3, resp3);

        // Now {1,2,3}: 3/3 old quorum, 3/4 new quorum — both satisfied
        // Commit index should advance
        assert!(leader.commit_index() > 0);
    }

    #[test]
    fn membership_abort_on_step_down() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_leader(dir.path());

        leader
            .propose_config_change(ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();
        assert!(leader.membership().in_joint_consensus());

        // Step down
        let req = AppendRequest {
            term: 10,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        leader.handle_append_entries(req);

        // Joint consensus should be aborted
        let membership = leader.membership();
        assert!(!membership.in_joint_consensus());
        assert!(!membership.change_in_progress);
    }

    // ── Phase 6: End-to-End Leadership Transfer via TimeoutNow ──

    #[test]
    fn full_leadership_transfer_flow() {
        let dir = tempfile::tempdir().unwrap();
        let leader = make_node(1, vec![2, 3], dir.path());
        let target = make_node(2, vec![1, 3], dir.path());
        let node3 = make_node(3, vec![1, 2], dir.path());

        // Elect node 1 as leader
        leader.start_election();
        {
            let mut inner = leader.inner.lock();
            leader.become_leader_inner(&mut inner);
        }

        // Replicate to node 2 so it's caught up
        let requests = leader.create_append_requests();
        let (_, req) = requests.into_iter().find(|(id, _)| *id == 2).unwrap();
        let resp = target.handle_append_entries(req);
        leader.handle_append_response(2, resp);

        // Initiate transfer to node 2
        leader.transfer_leadership(2).unwrap();

        // Node 2 is caught up — check_transfer_progress returns target
        let transfer_target = leader.check_transfer_progress();
        assert_eq!(transfer_target, Some(2));

        // Send TimeoutNow to node 2
        let msg = TimeoutNow {
            term: leader.term(),
            leader_id: 1,
        };
        let vote_requests = target.handle_timeout_now(msg);
        assert_eq!(target.role(), Role::Candidate);

        // Node 2 gets vote from node 3
        let (_, vote_req) = vote_requests.iter().find(|(id, _)| *id == 3).unwrap();
        let resp = node3.handle_vote_request(vote_req.clone());
        assert!(resp.vote_granted);

        let became_leader = target.handle_vote_response(3, resp, false);
        assert!(became_leader);
        assert_eq!(target.role(), Role::Leader);

        // Original leader steps down when it sees the new term
        let new_term = target.term();
        let req = AppendRequest {
            term: new_term,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        leader.handle_append_entries(req);
        assert_eq!(leader.role(), Role::Follower);
        assert_eq!(leader.leader_id(), Some(2));
    }
}
