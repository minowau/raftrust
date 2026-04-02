use raft_common::types::NodeId;
use raft_consensus::message::TimeoutNow;
use raft_consensus::node::{NodeConfig, RaftNode};
use raft_consensus::proto::raft::raft_service_server::RaftServiceServer;
use raft_consensus::rpc::client::PeerClient;
use raft_consensus::rpc::server::RaftRpcServer;
use raft_consensus::state::Role;
use raft_consensus::tick::TickConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Instant};
use tonic::transport::Server;
use tracing::{debug, info};

/// Configuration for the full server (Raft + KV).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub node_id: NodeId,
    /// Map of all nodes: node_id -> "host:port"
    pub cluster: HashMap<NodeId, String>,
    pub data_dir: String,
    pub listen_addr: String,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}

/// The main server that runs Raft consensus + KV service.
pub struct RaftServer {
    node: Arc<RaftNode>,
    peers: Arc<Mutex<HashMap<NodeId, PeerClient>>>,
    config: ServerConfig,
    tick_config: TickConfig,
}

impl RaftServer {
    pub fn new(config: ServerConfig) -> raft_common::error::Result<Self> {
        let peer_ids: Vec<NodeId> = config
            .cluster
            .keys()
            .filter(|&&id| id != config.node_id)
            .copied()
            .collect();

        let tick_config = TickConfig::new(
            config.election_timeout_min_ms,
            config.election_timeout_max_ms,
            config.heartbeat_interval_ms,
        );

        let node = Arc::new(RaftNode::new(NodeConfig {
            id: config.node_id,
            peers: peer_ids.clone(),
            data_dir: config.data_dir.clone(),
            tick_config: tick_config.clone(),
        })?);

        let mut peers = HashMap::new();
        for &peer_id in &peer_ids {
            if let Some(addr) = config.cluster.get(&peer_id) {
                peers.insert(peer_id, PeerClient::new(peer_id, addr.clone()));
            }
        }

        Ok(Self {
            node,
            peers: Arc::new(Mutex::new(peers)),
            config,
            tick_config,
        })
    }

    /// Get a reference to the underlying RaftNode.
    pub fn node(&self) -> &Arc<RaftNode> {
        &self.node
    }

    /// Run the Raft server: gRPC listener + election/heartbeat loop.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = self.config.listen_addr.parse()?;
        let node = self.node.clone();
        let peers = self.peers.clone();
        let tick_config = self.tick_config.clone();

        // Spawn gRPC server
        let raft_service = RaftRpcServer::new(node.clone());
        let grpc_handle = tokio::spawn(async move {
            info!(addr = %addr, "Starting gRPC server");
            Server::builder()
                .add_service(RaftServiceServer::new(raft_service))
                .serve(addr)
                .await
        });

        // Spawn Raft event loop
        let event_handle = tokio::spawn(Self::event_loop(node.clone(), peers.clone(), tick_config));

        tokio::select! {
            result = grpc_handle => {
                result??;
            }
            result = event_handle => {
                result?;
            }
        }

        Ok(())
    }

    /// The main Raft event loop: election timeouts and heartbeats.
    async fn event_loop(
        node: Arc<RaftNode>,
        peers: Arc<Mutex<HashMap<NodeId, PeerClient>>>,
        tick_config: TickConfig,
    ) {
        let mut election_deadline = Instant::now() + tick_config.random_election_timeout();
        let election_timeout_ms = tick_config.election_timeout_max_ms();

        loop {
            let role = node.role();
            let timeout = if role == Role::Leader {
                tick_config.heartbeat_interval
            } else {
                election_deadline.saturating_duration_since(Instant::now())
            };

            time::sleep(timeout).await;

            match node.role() {
                Role::Leader => {
                    // Send heartbeats / replicate entries
                    Self::send_append_entries(&node, &peers).await;

                    // Check leadership transfer progress
                    Self::check_transfer(&node, &peers, election_timeout_ms).await;
                }
                Role::Follower | Role::PreCandidate => {
                    if Instant::now() >= election_deadline {
                        // Election timeout — start pre-vote
                        debug!(node = node.id(), "Election timeout, starting pre-vote");
                        let requests = node.start_pre_vote();
                        let granted = Self::send_vote_requests(&node, &peers, requests, true).await;

                        if granted && node.pre_vote_has_quorum() {
                            // Pre-vote succeeded — start real election
                            let requests = node.start_election();
                            Self::send_vote_requests(&node, &peers, requests, false).await;
                        }

                        election_deadline = Instant::now() + tick_config.random_election_timeout();
                    }
                }
                Role::Candidate => {
                    if Instant::now() >= election_deadline {
                        // Election timed out — restart
                        let requests = node.start_election();
                        Self::send_vote_requests(&node, &peers, requests, false).await;
                        election_deadline = Instant::now() + tick_config.random_election_timeout();
                    }
                }
            }

            // Apply committed entries
            let _committed = node.take_committed_entries();
            // Phase 4: these will be applied to MvccStore via the apply loop
        }
    }

    /// Send AppendEntries to all peers concurrently.
    /// Also records heartbeat acks for pending read index requests.
    async fn send_append_entries(
        node: &Arc<RaftNode>,
        peers: &Arc<Mutex<HashMap<NodeId, PeerClient>>>,
    ) {
        let requests = node.create_append_requests();
        if requests.is_empty() {
            return;
        }

        for (peer_id, request) in requests {
            let node = node.clone();
            let peers = peers.clone();
            tokio::spawn(async move {
                let mut peers_guard = peers.lock().await;
                if let Some(client) = peers_guard.get_mut(&peer_id) {
                    match client.append_entries(&request).await {
                        Ok(response) => {
                            if response.success {
                                // Record heartbeat ack for read index protocol
                                node.read_index_ack(peer_id);
                            }
                            node.handle_append_response(peer_id, response);
                        }
                        Err(e) => {
                            debug!(peer = peer_id, error = %e, "AppendEntries failed");
                            client.reset();
                        }
                    }
                }
            });
        }
    }

    /// Check leadership transfer progress and send TimeoutNow if target is caught up.
    async fn check_transfer(
        node: &Arc<RaftNode>,
        peers: &Arc<Mutex<HashMap<NodeId, PeerClient>>>,
        election_timeout_ms: u64,
    ) {
        // Check for timeout first
        node.check_transfer_timeout(election_timeout_ms);

        // Check if target's log is caught up — if so, send TimeoutNow
        if let Some(target) = node.check_transfer_progress() {
            let msg = TimeoutNow {
                term: node.term(),
                leader_id: node.id(),
            };
            let mut peers_guard = peers.lock().await;
            if let Some(client) = peers_guard.get_mut(&target) {
                match client.timeout_now(&msg).await {
                    Ok(_) => {
                        info!(target = target, "Sent TimeoutNow to transfer target");
                    }
                    Err(e) => {
                        debug!(target = target, error = %e, "Failed to send TimeoutNow");
                        client.reset();
                    }
                }
            }
        }
    }

    /// Send vote requests to all peers. Returns true if any were sent successfully.
    async fn send_vote_requests(
        node: &Arc<RaftNode>,
        peers: &Arc<Mutex<HashMap<NodeId, PeerClient>>>,
        requests: Vec<(NodeId, raft_consensus::message::VoteRequest)>,
        is_pre_vote: bool,
    ) -> bool {
        let mut any_success = false;

        for (peer_id, request) in requests {
            let mut peers_guard = peers.lock().await;
            if let Some(client) = peers_guard.get_mut(&peer_id) {
                match client.request_vote(&request).await {
                    Ok(response) => {
                        let became_leader =
                            node.handle_vote_response(peer_id, response, is_pre_vote);
                        any_success = true;
                        if became_leader {
                            info!(node = node.id(), "Won election");
                            return true;
                        }
                    }
                    Err(e) => {
                        debug!(peer = peer_id, error = %e, "Vote request failed");
                        client.reset();
                    }
                }
            }
        }

        any_success
    }
}
