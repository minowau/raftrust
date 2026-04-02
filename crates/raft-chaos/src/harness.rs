use raft_consensus::node::{NodeConfig, RaftNode};
use raft_consensus::state::Role;
use raft_consensus::tick::TickConfig;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::info;

use crate::network::NetworkSim;

/// An in-process Raft cluster for chaos testing.
///
/// Orchestrates N nodes with a simulated network layer that supports
/// partition injection, message loss, and latency. All communication
/// happens via direct method calls (no actual gRPC), making tests
/// deterministic and fast.
pub struct ChaosCluster {
    pub nodes: HashMap<u64, RaftNode>,
    pub network: NetworkSim,
    data_dir: PathBuf,
    node_ids: Vec<u64>,
}

impl ChaosCluster {
    /// Create a new cluster with `n` nodes.
    pub fn new(n: usize, data_dir: &Path) -> Self {
        let node_ids: Vec<u64> = (1..=n as u64).collect();
        let mut nodes = HashMap::new();

        for &id in &node_ids {
            let peers: Vec<u64> = node_ids.iter().copied().filter(|&p| p != id).collect();
            let node_dir = data_dir.join(format!("node-{}", id));
            let node = RaftNode::new(NodeConfig {
                id,
                peers,
                data_dir: node_dir.to_string_lossy().to_string(),
                tick_config: TickConfig::new(150, 300, 50),
            })
            .unwrap();
            nodes.insert(id, node);
        }

        Self {
            nodes,
            network: NetworkSim::new(),
            data_dir: data_dir.to_path_buf(),
            node_ids,
        }
    }

    /// Get all node IDs.
    pub fn node_ids(&self) -> &[u64] {
        &self.node_ids
    }

    /// Find the current leader (if any).
    pub fn find_leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .find(|(_, n)| n.role() == Role::Leader)
            .map(|(&id, _)| id)
    }

    /// Elect a leader by running a full election on the given node.
    /// Returns the leader's ID.
    pub fn elect_leader(&self, candidate_id: u64) -> u64 {
        let candidate = &self.nodes[&candidate_id];

        // Pre-vote
        let pre_vote_reqs = candidate.start_pre_vote();
        for (peer_id, req) in &pre_vote_reqs {
            if self.network.should_deliver(candidate_id, *peer_id) {
                if let Some(peer) = self.nodes.get(peer_id) {
                    let resp = peer.handle_vote_request(req.clone());
                    candidate.handle_vote_response(*peer_id, resp, true);
                }
            }
        }

        if !candidate.pre_vote_has_quorum() {
            return 0; // Pre-vote failed
        }

        // Real election
        let vote_reqs = candidate.start_election();
        for (peer_id, req) in &vote_reqs {
            if self.network.should_deliver(candidate_id, *peer_id) {
                if let Some(peer) = self.nodes.get(peer_id) {
                    let resp = peer.handle_vote_request(req.clone());
                    if self.network.should_deliver(*peer_id, candidate_id) {
                        let won = candidate.handle_vote_response(*peer_id, resp, false);
                        if won {
                            return candidate_id;
                        }
                    }
                }
            }
        }

        if candidate.role() == Role::Leader {
            candidate_id
        } else {
            0
        }
    }

    /// Replicate from leader to all reachable followers.
    /// Returns the number of successful replications.
    pub fn replicate(&self, leader_id: u64) -> usize {
        let leader = match self.nodes.get(&leader_id) {
            Some(n) if n.role() == Role::Leader => n,
            _ => return 0,
        };

        let requests = leader.create_append_requests();
        let mut success_count = 0;

        for (peer_id, req) in requests {
            if self.network.should_deliver(leader_id, peer_id) {
                if let Some(peer) = self.nodes.get(&peer_id) {
                    let resp = peer.handle_append_entries(req);
                    if self.network.should_deliver(peer_id, leader_id) {
                        leader.handle_append_response(peer_id, resp);
                        success_count += 1;
                    }
                }
            }
        }

        success_count
    }

    /// Propose a value through the leader. Returns the log index or None.
    pub fn propose(&self, leader_id: u64, data: Vec<u8>) -> Option<u64> {
        let leader = self.nodes.get(&leader_id)?;
        leader.propose(data).ok()
    }

    /// Run one round of replication and return commit index of the leader.
    pub fn replicate_and_commit(&self, leader_id: u64) -> u64 {
        self.replicate(leader_id);
        self.nodes
            .get(&leader_id)
            .map(|n| n.commit_index())
            .unwrap_or(0)
    }

    /// Kill a random node (remove from the cluster temporarily).
    /// Returns the killed node's ID and the removed node.
    pub fn kill_random(&mut self) -> Option<(u64, RaftNode)> {
        let mut rng = rand::thread_rng();
        let id = *self.node_ids.choose(&mut rng)?;
        self.kill_node(id)
    }

    /// Kill a specific node.
    pub fn kill_node(&mut self, id: u64) -> Option<(u64, RaftNode)> {
        let node = self.nodes.remove(&id)?;
        // Isolate the killed node's network
        self.network.isolate(id, &self.node_ids);
        info!(node = id, "Killed node");
        Some((id, node))
    }

    /// Restart a previously killed node.
    pub fn restart_node(&mut self, id: u64, node: RaftNode) {
        self.network.heal_all(); // Simplification: heal all on restart
        self.nodes.insert(id, node);
        info!(node = id, "Restarted node");
    }

    /// Create a fresh node (simulates a node that lost all state).
    pub fn create_fresh_node(&mut self, id: u64) -> &RaftNode {
        let peers: Vec<u64> = self.node_ids.iter().copied().filter(|&p| p != id).collect();
        let node_dir = self.data_dir.join(format!("node-{}-fresh", id));
        let node = RaftNode::new(NodeConfig {
            id,
            peers,
            data_dir: node_dir.to_string_lossy().to_string(),
            tick_config: TickConfig::new(150, 300, 50),
        })
        .unwrap();
        self.nodes.insert(id, node);
        self.network.heal_all();
        &self.nodes[&id]
    }

    /// Get a node by ID.
    pub fn node(&self, id: u64) -> Option<&RaftNode> {
        self.nodes.get(&id)
    }

    /// Number of alive nodes.
    pub fn alive_count(&self) -> usize {
        self.nodes.len()
    }
}

/// Record of a linearizable operation for history verification.
#[derive(Debug, Clone)]
pub struct Operation {
    pub op_type: OpType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub start_time: std::time::Instant,
    pub end_time: std::time::Instant,
    pub result: OpResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpType {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    Ok,
    Error(String),
}

/// Collects operation history for linearizability checking.
#[derive(Debug, Default)]
pub struct OperationHistory {
    pub operations: Vec<Operation>,
}

impl OperationHistory {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn record(&mut self, op: Operation) {
        self.operations.push(op);
    }

    /// Basic consistency check: every successful read of a key returns
    /// the value from the most recent successful write.
    /// This is a simplified linearizability check.
    pub fn check_consistency(&self) -> Result<(), String> {
        let mut latest_writes: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        // Sort by end_time to process in order
        let mut sorted_ops = self.operations.clone();
        sorted_ops.sort_by_key(|op| op.end_time);

        for op in &sorted_ops {
            if op.result != OpResult::Ok {
                continue;
            }

            match op.op_type {
                OpType::Write => {
                    latest_writes.insert(op.key.clone(), op.value.clone());
                }
                OpType::Read => {
                    if let Some(expected) = latest_writes.get(&op.key) {
                        if op.value != *expected {
                            return Err(format!(
                                "Stale read: key={:?} expected={:?} got={:?}",
                                String::from_utf8_lossy(&op.key),
                                String::from_utf8_lossy(expected),
                                String::from_utf8_lossy(&op.value),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn successful_ops(&self) -> usize {
        self.operations
            .iter()
            .filter(|op| op.result == OpResult::Ok)
            .count()
    }

    pub fn failed_ops(&self) -> usize {
        self.operations
            .iter()
            .filter(|op| op.result != OpResult::Ok)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = ChaosCluster::new(3, dir.path());

        assert_eq!(cluster.alive_count(), 3);
        assert_eq!(cluster.node_ids().len(), 3);
        assert!(cluster.find_leader().is_none());
    }

    #[test]
    fn elect_and_propose() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = ChaosCluster::new(3, dir.path());

        let leader = cluster.elect_leader(1);
        assert_eq!(leader, 1);
        assert_eq!(cluster.find_leader(), Some(1));

        let index = cluster.propose(1, b"hello".to_vec());
        assert!(index.is_some());

        let commit = cluster.replicate_and_commit(1);
        assert!(commit > 0);
    }

    #[test]
    fn propose_and_replicate_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = ChaosCluster::new(3, dir.path());

        cluster.elect_leader(1);

        for i in 0..10 {
            cluster.propose(1, format!("entry-{}", i).into_bytes());
        }

        cluster.replicate(1);
        let commit = cluster.nodes[&1].commit_index();
        assert!(commit >= 10);
    }

    #[test]
    fn election_fails_with_partition() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = ChaosCluster::new(3, dir.path());

        // Partition node 1 from everyone
        cluster.network.isolate(1, &[1, 2, 3]);

        let leader = cluster.elect_leader(1);
        assert_eq!(leader, 0); // Should fail — can't reach quorum
    }

    #[test]
    fn replication_survives_one_node_down() {
        let dir = tempfile::tempdir().unwrap();
        let mut cluster = ChaosCluster::new(3, dir.path());

        cluster.elect_leader(1);
        cluster.propose(1, b"data".to_vec());

        // Kill node 3
        let (killed_id, _killed_node) = cluster.kill_node(3).unwrap();
        assert_eq!(killed_id, 3);
        assert_eq!(cluster.alive_count(), 2);

        // Should still replicate with 2/3 quorum
        let replicated = cluster.replicate(1);
        assert!(replicated >= 1);

        let commit = cluster.nodes[&1].commit_index();
        assert!(commit > 0);
    }

    #[test]
    fn kill_leader_and_reelect() {
        let dir = tempfile::tempdir().unwrap();
        let mut cluster = ChaosCluster::new(3, dir.path());

        cluster.elect_leader(1);
        assert_eq!(cluster.find_leader(), Some(1));

        // Kill the leader
        let (_id, _node) = cluster.kill_node(1).unwrap();
        assert!(cluster.find_leader().is_none());

        // Elect a new leader from remaining nodes
        // Need to heal network first since kill_node isolates
        cluster.network.heal_all();
        let new_leader = cluster.elect_leader(2);
        assert_eq!(new_leader, 2);
    }

    #[test]
    fn operation_history_consistency() {
        let mut history = OperationHistory::new();
        let now = std::time::Instant::now();

        // Write key=a, value=1
        history.record(Operation {
            op_type: OpType::Write,
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            start_time: now,
            end_time: now,
            result: OpResult::Ok,
        });

        // Read key=a → should be "1"
        history.record(Operation {
            op_type: OpType::Read,
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            start_time: now,
            end_time: now,
            result: OpResult::Ok,
        });

        assert!(history.check_consistency().is_ok());
    }

    #[test]
    fn operation_history_detects_stale_read() {
        let mut history = OperationHistory::new();
        let now = std::time::Instant::now();
        let later = now + std::time::Duration::from_millis(1);

        // Write key=a, value=2
        history.record(Operation {
            op_type: OpType::Write,
            key: b"a".to_vec(),
            value: b"2".to_vec(),
            start_time: now,
            end_time: now,
            result: OpResult::Ok,
        });

        // Stale read returns old value "1"
        history.record(Operation {
            op_type: OpType::Read,
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            start_time: later,
            end_time: later,
            result: OpResult::Ok,
        });

        assert!(history.check_consistency().is_err());
    }

    #[test]
    fn network_partition_blocks_replication() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = ChaosCluster::new(3, dir.path());

        cluster.elect_leader(1);
        cluster.propose(1, b"before-partition".to_vec());

        // Partition leader from all followers
        cluster.network.isolate(1, &[1, 2, 3]);

        let replicated = cluster.replicate(1);
        assert_eq!(replicated, 0); // No replication through partition
    }

    #[test]
    fn heal_partition_resumes_replication() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = ChaosCluster::new(3, dir.path());

        cluster.elect_leader(1);
        cluster.propose(1, b"data".to_vec());

        // Partition then heal
        cluster.network.isolate(1, &[1, 2, 3]);
        assert_eq!(cluster.replicate(1), 0);

        cluster.network.heal_all();
        let replicated = cluster.replicate(1);
        assert!(replicated > 0);
    }
}
