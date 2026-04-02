use raft_consensus::node::{NodeConfig, RaftNode};
use raft_consensus::state::Role;
use raft_consensus::tick::TickConfig;
use std::path::Path;

/// Create a test node with in-memory directory.
fn make_node(id: u64, peers: Vec<u64>, dir: &Path) -> RaftNode {
    let data_dir = dir.join(format!("node-{}", id));
    RaftNode::new(NodeConfig {
        id,
        peers,
        data_dir: data_dir.to_string_lossy().to_string(),
        tick_config: TickConfig::default(),
    })
    .unwrap()
}

/// Simulate a full election and return the leader node index.
fn elect_leader(nodes: &[&RaftNode]) -> usize {
    let node = nodes[0];

    // Pre-vote
    let pre_vote_reqs = node.start_pre_vote();
    for (peer_id, req) in &pre_vote_reqs {
        let peer = nodes.iter().find(|n| n.id() == *peer_id).unwrap();
        let resp = peer.handle_vote_request(req.clone());
        node.handle_vote_response(*peer_id, resp, true);
    }
    assert!(node.pre_vote_has_quorum());

    // Real election
    let vote_reqs = node.start_election();
    for (peer_id, req) in &vote_reqs {
        let peer = nodes.iter().find(|n| n.id() == *peer_id).unwrap();
        let resp = peer.handle_vote_request(req.clone());
        let won = node.handle_vote_response(*peer_id, resp, false);
        if won {
            break;
        }
    }
    assert_eq!(node.role(), Role::Leader);
    0
}

/// Replicate from leader to all followers and return match results.
fn replicate(leader: &RaftNode, followers: &[&RaftNode]) {
    let requests = leader.create_append_requests();
    for (peer_id, req) in requests {
        let follower = followers.iter().find(|n| n.id() == peer_id).unwrap();
        let resp = follower.handle_append_entries(req);
        leader.handle_append_response(peer_id, resp);
    }
}

#[test]
fn three_node_election_and_replication() {
    let dir = tempfile::tempdir().unwrap();
    let n1 = make_node(1, vec![2, 3], dir.path());
    let n2 = make_node(2, vec![1, 3], dir.path());
    let n3 = make_node(3, vec![1, 2], dir.path());

    let nodes: Vec<&RaftNode> = vec![&n1, &n2, &n3];
    elect_leader(&nodes);

    // Propose an entry
    let index = n1.propose(b"hello world".to_vec()).unwrap();
    assert_eq!(index, 2); // 1 = noop, 2 = our entry

    // Replicate to followers
    replicate(&n1, &[&n2, &n3]);

    // Commit should have advanced (leader + 2 followers = quorum)
    assert!(n1.commit_index() >= 2);

    // Take committed entries
    let entries = n1.take_committed_entries();
    assert!(!entries.is_empty());
}

#[test]
fn leader_step_down_on_higher_term() {
    let dir = tempfile::tempdir().unwrap();
    let n1 = make_node(1, vec![2, 3], dir.path());
    let n2 = make_node(2, vec![1, 3], dir.path());
    let n3 = make_node(3, vec![1, 2], dir.path());

    let nodes: Vec<&RaftNode> = vec![&n1, &n2, &n3];
    elect_leader(&nodes);
    assert_eq!(n1.role(), Role::Leader);

    // Node 3 starts election at higher term
    n3.start_election();
    let vote_reqs_from_n3 = n3.start_election();

    // Node 1 receives the vote request with higher term
    for (peer_id, req) in &vote_reqs_from_n3 {
        if *peer_id == 1 {
            n1.handle_vote_request(req.clone());
        }
    }

    // Node 1 should have stepped down
    assert_eq!(n1.role(), Role::Follower);
}

#[test]
fn follower_catches_up_after_missed_entries() {
    let dir = tempfile::tempdir().unwrap();
    let n1 = make_node(1, vec![2, 3], dir.path());
    let n2 = make_node(2, vec![1, 3], dir.path());
    let n3 = make_node(3, vec![1, 2], dir.path());

    let nodes: Vec<&RaftNode> = vec![&n1, &n2, &n3];
    elect_leader(&nodes);

    // Propose 5 entries, only replicate to n2 initially
    for i in 0..5 {
        n1.propose(format!("entry-{}", i).into_bytes()).unwrap();
    }

    // Replicate only to n2
    let requests = n1.create_append_requests();
    for (peer_id, req) in &requests {
        if *peer_id == 2 {
            let resp = n2.handle_append_entries(req.clone());
            n1.handle_append_response(*peer_id, resp);
        }
    }

    // Now replicate to n3 — should catch up in one round
    let requests = n1.create_append_requests();
    for (peer_id, req) in &requests {
        if *peer_id == 3 {
            let resp = n3.handle_append_entries(req.clone());
            assert!(resp.success, "n3 should catch up");
            n1.handle_append_response(*peer_id, resp);
        }
    }
}

#[test]
fn write_survives_one_node_down() {
    let dir = tempfile::tempdir().unwrap();
    let n1 = make_node(1, vec![2, 3], dir.path());
    let n2 = make_node(2, vec![1, 3], dir.path());

    // Elect with just n1 and n2 (pretend n3 is down)
    let pre_vote_reqs = n1.start_pre_vote();
    for (peer_id, req) in &pre_vote_reqs {
        if *peer_id == 2 {
            let resp = n2.handle_vote_request(req.clone());
            n1.handle_vote_response(*peer_id, resp, true);
        }
    }

    let vote_reqs = n1.start_election();
    for (peer_id, req) in &vote_reqs {
        if *peer_id == 2 {
            let resp = n2.handle_vote_request(req.clone());
            n1.handle_vote_response(*peer_id, resp, false);
        }
    }
    assert_eq!(n1.role(), Role::Leader);

    // Propose and replicate only to n2 (n3 is down)
    n1.propose(b"data".to_vec()).unwrap();
    let requests = n1.create_append_requests();
    for (peer_id, req) in requests {
        if peer_id == 2 {
            let resp = n2.handle_append_entries(req);
            n1.handle_append_response(peer_id, resp);
        }
    }

    // Should still commit with 2/3 quorum
    assert!(n1.commit_index() >= 2);
}

#[test]
fn apply_loop_integration() {
    use raft_mvcc::mvcc::MvccStore;
    use raft_server::apply::{ApplyLoop, KvCommand};
    use raft_storage::lsm::{LsmConfig, LsmTree};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();

    // Create MVCC store
    let engine = Arc::new(
        LsmTree::open(
            &dir.path().join("storage"),
            LsmConfig {
                memtable_size_limit: 64 * 1024,
                block_size: 256,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let store = Arc::new(MvccStore::new(engine));
    let apply = ApplyLoop::new(store.clone());

    // Create a Raft cluster and elect leader
    let n1 = make_node(1, vec![2, 3], dir.path());
    let n2 = make_node(2, vec![1, 3], dir.path());
    let n3 = make_node(3, vec![1, 2], dir.path());
    let nodes: Vec<&RaftNode> = vec![&n1, &n2, &n3];
    elect_leader(&nodes);

    // Propose a KV command through Raft
    let cmd = KvCommand::Put {
        key: b"name".to_vec(),
        value: b"raft".to_vec(),
        lease_id: 0,
        ttl_seconds: 0,
    };
    n1.propose(cmd.encode()).unwrap();

    // Replicate and commit
    replicate(&n1, &[&n2, &n3]);

    // Apply committed entries to MVCC store
    let entries = n1.take_committed_entries();
    let applied = apply.apply(&entries).unwrap();
    assert!(applied >= 1);

    // Verify data is in MVCC store
    let kv = store.get(b"name").unwrap().unwrap();
    assert_eq!(kv.value, b"raft");
}
