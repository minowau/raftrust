use raft_common::types::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Configuration of a cluster — the set of members.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    /// Nodes in this configuration: id -> address.
    pub members: Vec<MemberInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberInfo {
    pub id: NodeId,
    pub address: String,
}

impl ClusterConfig {
    pub fn new(members: Vec<MemberInfo>) -> Self {
        Self { members }
    }

    pub fn member_ids(&self) -> HashSet<NodeId> {
        self.members.iter().map(|m| m.id).collect()
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.members.iter().any(|m| m.id == id)
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

/// A configuration change request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConfigChange {
    AddNode { id: NodeId, address: String },
    RemoveNode { id: NodeId },
}

/// The membership state of the cluster, supporting joint consensus.
///
/// Joint consensus (Raft §6):
/// During a membership change, the cluster transitions through two phases:
///
/// 1. **C_old** → **C_old,new** (joint config): A config change entry is proposed
///    containing both old and new configurations. During joint consensus,
///    decisions (elections and commits) require majorities from BOTH
///    the old and new configurations independently.
///
/// 2. **C_old,new** → **C_new**: Once the joint config entry is committed,
///    a second entry is proposed with only the new configuration.
///    Once committed, nodes not in C_new can shut down.
///
/// This two-phase approach guarantees that no two leaders can be elected
/// for the same term during the transition.
#[derive(Debug, Clone)]
pub struct MembershipState {
    /// The current (committed) configuration.
    pub current: ClusterConfig,
    /// If in joint consensus, the new configuration being transitioned to.
    /// When Some, we are in the C_old,new joint phase.
    pub pending: Option<ClusterConfig>,
    /// Whether a config change is in progress (prevents concurrent changes).
    pub change_in_progress: bool,
}

impl MembershipState {
    /// Create initial membership from node config.
    pub fn new(self_id: NodeId, peers: &[NodeId]) -> Self {
        let mut members = vec![MemberInfo {
            id: self_id,
            address: String::new(), // Address resolved at server layer
        }];
        for &peer in peers {
            members.push(MemberInfo {
                id: peer,
                address: String::new(),
            });
        }
        Self {
            current: ClusterConfig::new(members),
            pending: None,
            change_in_progress: false,
        }
    }

    /// Create from an explicit config.
    pub fn from_config(config: ClusterConfig) -> Self {
        Self {
            current: config,
            pending: None,
            change_in_progress: false,
        }
    }

    /// Whether we are currently in a joint consensus phase.
    pub fn in_joint_consensus(&self) -> bool {
        self.pending.is_some()
    }

    /// Get all unique node IDs in the current effective configuration.
    /// During joint consensus, this is the union of old and new configs.
    pub fn all_voters(&self) -> HashSet<NodeId> {
        let mut voters = self.current.member_ids();
        if let Some(ref pending) = self.pending {
            voters.extend(pending.member_ids());
        }
        voters
    }

    /// The cluster size for quorum calculations.
    /// During joint consensus, both configs must independently reach quorum.
    pub fn current_size(&self) -> usize {
        self.current.len()
    }

    /// Check if a set of nodes forms a quorum under the current config.
    /// During joint consensus, requires majorities in BOTH old and new configs.
    pub fn has_quorum(&self, voters: &HashSet<NodeId>) -> bool {
        let old_count = self
            .current
            .members
            .iter()
            .filter(|m| voters.contains(&m.id))
            .count();
        let old_quorum = self.current.len() / 2 + 1;

        if old_count < old_quorum {
            return false;
        }

        if let Some(ref new_config) = self.pending {
            let new_count = new_config
                .members
                .iter()
                .filter(|m| voters.contains(&m.id))
                .count();
            let new_quorum = new_config.len() / 2 + 1;
            if new_count < new_quorum {
                return false;
            }
        }

        true
    }

    /// Begin a joint consensus transition by proposing C_old,new.
    /// Returns the joint config entry data to be proposed through Raft,
    /// or an error if a change is already in progress.
    pub fn begin_change(
        &mut self,
        change: &ConfigChange,
    ) -> Result<JointConfigData, MembershipError> {
        if self.change_in_progress {
            return Err(MembershipError::ChangeInProgress);
        }

        let mut new_members = self.current.members.clone();

        match change {
            ConfigChange::AddNode { id, address } => {
                if self.current.contains(*id) {
                    return Err(MembershipError::NodeAlreadyExists(*id));
                }
                new_members.push(MemberInfo {
                    id: *id,
                    address: address.clone(),
                });
            }
            ConfigChange::RemoveNode { id } => {
                if !self.current.contains(*id) {
                    return Err(MembershipError::NodeNotFound(*id));
                }
                new_members.retain(|m| m.id != *id);
                if new_members.is_empty() {
                    return Err(MembershipError::CannotRemoveLastNode);
                }
            }
        }

        let new_config = ClusterConfig::new(new_members);
        self.pending = Some(new_config.clone());
        self.change_in_progress = true;

        Ok(JointConfigData {
            old: self.current.clone(),
            new: new_config,
        })
    }

    /// Complete the joint consensus transition: move to C_new.
    /// Called when the joint config entry is committed.
    /// Returns the new config data to be proposed as the final entry.
    pub fn finalize_joint(&mut self) -> Option<ClusterConfig> {
        if let Some(new_config) = self.pending.take() {
            self.current = new_config.clone();
            self.change_in_progress = false;
            Some(new_config)
        } else {
            None
        }
    }

    /// Apply a committed new configuration (C_new entry committed).
    pub fn apply_new_config(&mut self, config: ClusterConfig) {
        self.current = config;
        self.pending = None;
        self.change_in_progress = false;
    }

    /// Abort an in-progress change (e.g., leader stepped down).
    pub fn abort_change(&mut self) {
        self.pending = None;
        self.change_in_progress = false;
    }

    /// Check if a node is in the current effective configuration.
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.all_voters().contains(&id)
    }
}

/// Data for a joint consensus config entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JointConfigData {
    pub old: ClusterConfig,
    pub new: ClusterConfig,
}

impl JointConfigData {
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn decode(data: &[u8]) -> Result<Self, MembershipError> {
        serde_json::from_slice(data).map_err(|_| MembershipError::InvalidConfigData)
    }
}

/// Errors from membership change operations.
#[derive(Debug, PartialEq, Eq)]
pub enum MembershipError {
    ChangeInProgress,
    NodeAlreadyExists(NodeId),
    NodeNotFound(NodeId),
    CannotRemoveLastNode,
    InvalidConfigData,
    NotLeader,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(ids: &[NodeId]) -> ClusterConfig {
        ClusterConfig::new(
            ids.iter()
                .map(|&id| MemberInfo {
                    id,
                    address: format!("127.0.0.1:{}", 5000 + id),
                })
                .collect(),
        )
    }

    #[test]
    fn initial_membership() {
        let state = MembershipState::new(1, &[2, 3]);
        assert_eq!(state.current.len(), 3);
        assert!(!state.in_joint_consensus());
        assert!(state.current.contains(1));
        assert!(state.current.contains(2));
        assert!(state.current.contains(3));
    }

    #[test]
    fn quorum_three_nodes() {
        let state = MembershipState::from_config(make_config(&[1, 2, 3]));
        let mut voters = HashSet::new();

        // 1 node: not quorum
        voters.insert(1);
        assert!(!state.has_quorum(&voters));

        // 2 nodes: quorum
        voters.insert(2);
        assert!(state.has_quorum(&voters));

        // 3 nodes: still quorum
        voters.insert(3);
        assert!(state.has_quorum(&voters));
    }

    #[test]
    fn add_node_joint_consensus() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));

        let result = state.begin_change(&ConfigChange::AddNode {
            id: 4,
            address: "127.0.0.1:5004".to_string(),
        });
        assert!(result.is_ok());
        assert!(state.in_joint_consensus());

        let joint = result.unwrap();
        assert_eq!(joint.old.len(), 3);
        assert_eq!(joint.new.len(), 4);

        // During joint consensus, need quorum in BOTH configs
        let mut voters = HashSet::new();
        voters.insert(1);
        voters.insert(2);
        // 2/3 in old = quorum, but 2/4 in new = not quorum
        assert!(!state.has_quorum(&voters));

        voters.insert(4);
        // 2/3 in old, 3/4 in new = quorum in both
        assert!(state.has_quorum(&voters));
    }

    #[test]
    fn remove_node_joint_consensus() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));

        let result = state.begin_change(&ConfigChange::RemoveNode { id: 3 });
        assert!(result.is_ok());
        assert!(state.in_joint_consensus());

        let joint = result.unwrap();
        assert_eq!(joint.old.len(), 3);
        assert_eq!(joint.new.len(), 2);

        // Need quorum in both: old (3 nodes) and new (2 nodes)
        let mut voters = HashSet::new();
        voters.insert(1);
        voters.insert(2);
        // 2/3 in old, 2/2 in new = quorum
        assert!(state.has_quorum(&voters));
    }

    #[test]
    fn finalize_joint_consensus() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));

        state
            .begin_change(&ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();

        let new_config = state.finalize_joint().unwrap();
        assert_eq!(new_config.len(), 4);
        assert!(!state.in_joint_consensus());
        assert!(!state.change_in_progress);
        assert_eq!(state.current.len(), 4);
    }

    #[test]
    fn reject_concurrent_changes() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));

        state
            .begin_change(&ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();

        let result = state.begin_change(&ConfigChange::AddNode {
            id: 5,
            address: "127.0.0.1:5005".to_string(),
        });
        assert_eq!(result, Err(MembershipError::ChangeInProgress));
    }

    #[test]
    fn reject_add_duplicate_node() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));
        let result = state.begin_change(&ConfigChange::AddNode {
            id: 2,
            address: "127.0.0.1:5002".to_string(),
        });
        assert_eq!(result, Err(MembershipError::NodeAlreadyExists(2)));
    }

    #[test]
    fn reject_remove_nonexistent_node() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));
        let result = state.begin_change(&ConfigChange::RemoveNode { id: 99 });
        assert_eq!(result, Err(MembershipError::NodeNotFound(99)));
    }

    #[test]
    fn reject_remove_last_node() {
        let mut state = MembershipState::from_config(make_config(&[1]));
        let result = state.begin_change(&ConfigChange::RemoveNode { id: 1 });
        assert_eq!(result, Err(MembershipError::CannotRemoveLastNode));
    }

    #[test]
    fn abort_change() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));

        state
            .begin_change(&ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();
        assert!(state.in_joint_consensus());

        state.abort_change();
        assert!(!state.in_joint_consensus());
        assert!(!state.change_in_progress);
        assert_eq!(state.current.len(), 3); // Unchanged
    }

    #[test]
    fn joint_config_data_roundtrip() {
        let data = JointConfigData {
            old: make_config(&[1, 2, 3]),
            new: make_config(&[1, 2, 3, 4]),
        };
        let encoded = data.encode();
        let decoded = JointConfigData::decode(&encoded).unwrap();
        assert_eq!(decoded.old, data.old);
        assert_eq!(decoded.new, data.new);
    }

    #[test]
    fn all_voters_during_joint() {
        let mut state = MembershipState::from_config(make_config(&[1, 2, 3]));
        state
            .begin_change(&ConfigChange::AddNode {
                id: 4,
                address: "127.0.0.1:5004".to_string(),
            })
            .unwrap();

        let voters = state.all_voters();
        assert_eq!(voters.len(), 4);
        assert!(voters.contains(&4));
    }

    #[test]
    fn five_node_quorum() {
        let state = MembershipState::from_config(make_config(&[1, 2, 3, 4, 5]));
        let mut voters = HashSet::new();

        voters.insert(1);
        voters.insert(2);
        assert!(!state.has_quorum(&voters)); // 2/5

        voters.insert(3);
        assert!(state.has_quorum(&voters)); // 3/5
    }
}
