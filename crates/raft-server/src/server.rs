use raft_common::metrics::Metrics;
use raft_common::types::NodeId;
use raft_consensus_core::message::TimeoutNow;
use raft_consensus_core::node::{NodeConfig, RaftNode};
use raft_consensus_core::proto::raft::raft_service_server::RaftServiceServer;
use raft_consensus_core::rpc::client::PeerClient;
use raft_consensus_core::rpc::server::RaftRpcServer;
use raft_consensus_core::state::Role;
use raft_consensus_core::tick::TickConfig;
use raft_mvcc::mvcc::MvccStore;
use raft_storage::lsm::{LsmConfig, LsmTree};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Instant};
use tonic::transport::Server;
use tracing::{debug, info};

use crate::admin_service::AdminRpcService;
use crate::apply::ApplyLoop;
use crate::http::HttpServer;
use crate::kv_service::KvRpcService;
use crate::lease::LeaseManager;
use crate::lease_service::LeaseRpcService;
use crate::proto::admin::admin_service_server::AdminServiceServer;
use crate::proto::kv::kv_service_server::KvServiceServer;
use crate::proto::lease::lease_service_server::LeaseServiceServer;
use crate::proto::watch::watch_service_server::WatchServiceServer;
use crate::watch::WatchHub;
use crate::watch_service::WatchRpcService;

/// Configuration for the full server (Raft + KV).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub node_id: NodeId,
    /// Map of all nodes: node_id -> "host:port"
    pub cluster: HashMap<NodeId, String>,
    pub data_dir: String,
    pub listen_addr: String,
    /// Optional separate HTTP address for /metrics, /health, /ready.
    /// If None, HTTP endpoints are not started.
    pub http_addr: Option<String>,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}

/// The main server that runs Raft consensus + KV + Watch + Lease + Admin services.
pub struct RaftServer {
    node: Arc<RaftNode>,
    store: Arc<MvccStore>,
    peers: Arc<Mutex<HashMap<NodeId, PeerClient>>>,
    watch_hub: Arc<WatchHub>,
    lease_mgr: Arc<LeaseManager>,
    metrics: Arc<Metrics>,
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

        // Create storage engine
        let store_dir = std::path::Path::new(&config.data_dir).join("store");
        std::fs::create_dir_all(&store_dir)?;
        let engine = Arc::new(LsmTree::open(&store_dir, LsmConfig::default())?);
        let store = Arc::new(MvccStore::new(engine));

        let mut peers = HashMap::new();
        for &peer_id in &peer_ids {
            if let Some(addr) = config.cluster.get(&peer_id) {
                peers.insert(peer_id, PeerClient::new(peer_id, addr.clone()));
            }
        }

        let watch_hub = Arc::new(WatchHub::default());
        let lease_mgr = Arc::new(LeaseManager::new());
        let metrics = Arc::new(Metrics::new());

        Ok(Self {
            node,
            store,
            peers: Arc::new(Mutex::new(peers)),
            watch_hub,
            lease_mgr,
            metrics,
            config,
            tick_config,
        })
    }

    /// Get a reference to the underlying RaftNode.
    pub fn node(&self) -> &Arc<RaftNode> {
        &self.node
    }

    /// Run the full server: gRPC (Raft + KV + Watch + Lease + Admin) + HTTP + event loop.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = self.config.listen_addr.parse()?;
        let node = self.node.clone();
        let store = self.store.clone();
        let peers = self.peers.clone();
        let tick_config = self.tick_config.clone();
        let watch_hub = self.watch_hub.clone();
        let lease_mgr = self.lease_mgr.clone();
        let metrics = self.metrics.clone();

        // Build all gRPC services
        let raft_service = RaftRpcServer::new(node.clone());
        let kv_service = KvRpcService::new(node.clone(), store.clone());
        let watch_service = WatchRpcService::new(watch_hub.clone());
        let lease_service = LeaseRpcService::new(lease_mgr.clone());
        let admin_service = AdminRpcService::new(node.clone(), store.clone());

        // Spawn gRPC server with all services
        let grpc_handle = tokio::spawn(async move {
            info!(addr = %addr, "Starting gRPC server");
            Server::builder()
                .add_service(RaftServiceServer::new(raft_service))
                .add_service(KvServiceServer::new(kv_service))
                .add_service(WatchServiceServer::new(watch_service))
                .add_service(LeaseServiceServer::new(lease_service))
                .add_service(AdminServiceServer::new(admin_service))
                .serve(addr)
                .await
        });

        // Spawn HTTP server for /metrics, /health, /ready
        if let Some(http_addr) = &self.config.http_addr {
            let http_server = HttpServer::new(node.clone(), metrics.clone());
            let http_addr = http_addr.clone();
            tokio::spawn(async move {
                if let Err(e) = http_server.run(&http_addr).await {
                    tracing::error!(error = %e, "HTTP server failed");
                }
            });
        }

        // Build the apply loop
        let apply = ApplyLoop::full(
            store.clone(),
            node.clone(),
            watch_hub.clone(),
            lease_mgr.clone(),
        );

        // Spawn Raft event loop (includes apply)
        let event_handle = tokio::spawn(Self::event_loop(
            node.clone(),
            peers.clone(),
            tick_config,
            apply,
            lease_mgr.clone(),
            metrics.clone(),
        ));

        info!(
            node = self.config.node_id,
            "Server started — waiting for leader election"
        );

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

    /// The main Raft event loop: election timeouts, heartbeats, apply, lease expiry.
    async fn event_loop(
        node: Arc<RaftNode>,
        peers: Arc<Mutex<HashMap<NodeId, PeerClient>>>,
        tick_config: TickConfig,
        apply: ApplyLoop,
        lease_mgr: Arc<LeaseManager>,
        metrics: Arc<Metrics>,
    ) {
        let mut election_deadline = Instant::now() + tick_config.random_election_timeout();
        let election_timeout_ms = tick_config.election_timeout_max_ms();
        let mut lease_check_interval = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            let role = node.role();
            let timeout = if role == Role::Leader {
                tick_config.heartbeat_interval
            } else {
                election_deadline.saturating_duration_since(Instant::now())
            };

            tokio::select! {
                _ = time::sleep(timeout) => {
                    match node.role() {
                        Role::Leader => {
                            Self::send_append_entries(&node, &peers).await;
                            Self::check_transfer(&node, &peers, election_timeout_ms).await;
                        }
                        Role::Follower | Role::PreCandidate => {
                            // Reset election deadline if we recently received a heartbeat
                            let since_heartbeat = node.time_since_last_heartbeat();
                            if since_heartbeat < tick_config.election_timeout_min {
                                election_deadline = Instant::now() + tick_config.random_election_timeout();
                            }

                            if Instant::now() >= election_deadline {
                                debug!(node = node.id(), "Election timeout, starting pre-vote");
                                metrics.raft_elections_total.inc();
                                let requests = node.start_pre_vote();
                                let granted = Self::send_vote_requests(&node, &peers, requests, true).await;

                                if granted && node.pre_vote_has_quorum() {
                                    let requests = node.start_election();
                                    let won = Self::send_vote_requests(&node, &peers, requests, false).await;
                                    if won && node.role() == Role::Leader {
                                        metrics.raft_elections_won.inc();
                                        metrics.raft_leader_changes_total.inc();
                                    }
                                }

                                election_deadline = Instant::now() + tick_config.random_election_timeout();
                            }
                        }
                        Role::Candidate => {
                            if Instant::now() >= election_deadline {
                                let requests = node.start_election();
                                Self::send_vote_requests(&node, &peers, requests, false).await;
                                election_deadline = Instant::now() + tick_config.random_election_timeout();
                            }
                        }
                    }
                }
                _ = lease_check_interval.tick() => {
                    // Expire leases and delete their keys
                    let expired = lease_mgr.collect_expired();
                    for (lease_id, keys) in expired {
                        metrics.lease_expirations_total.inc();
                        for key in keys {
                            let _ = node.propose(
                                crate::apply::KvCommand::Delete { key }.encode()
                            );
                        }
                        debug!(lease_id = lease_id, "Expired lease, deleting keys");
                    }
                }
            }

            // Apply committed entries to MVCC store
            let committed = node.take_committed_entries();
            if !committed.is_empty() {
                match apply.apply(&committed) {
                    Ok(n) => {
                        if n > 0 {
                            debug!(count = n, "Applied entries");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to apply entries");
                    }
                }
            }
        }
    }

    /// Send AppendEntries to all peers concurrently with a per-RPC timeout.
    async fn send_append_entries(
        node: &Arc<RaftNode>,
        peers: &Arc<Mutex<HashMap<NodeId, PeerClient>>>,
    ) {
        let requests = node.create_append_requests();
        if requests.is_empty() {
            return;
        }

        // Send to each peer with a short timeout to prevent one slow peer
        // from blocking heartbeats to other peers.
        let mut futures = Vec::new();
        for (peer_id, request) in requests {
            let node = node.clone();
            let peers = peers.clone();
            futures.push(tokio::spawn(async move {
                let result = tokio::time::timeout(std::time::Duration::from_millis(2000), async {
                    let mut peers_guard = peers.lock().await;
                    if let Some(client) = peers_guard.get_mut(&peer_id) {
                        client.append_entries(&request).await
                    } else {
                        Err(tonic::Status::unavailable("no client"))
                    }
                })
                .await;

                match result {
                    Ok(Ok(response)) => {
                        if response.success {
                            node.read_index_ack(peer_id);
                        }
                        node.handle_append_response(peer_id, response);
                    }
                    Ok(Err(e)) => {
                        debug!(peer = peer_id, error = %e, "AppendEntries failed");
                        let mut peers_guard = peers.lock().await;
                        if let Some(client) = peers_guard.get_mut(&peer_id) {
                            client.reset();
                        }
                    }
                    Err(_) => {
                        debug!(peer = peer_id, "AppendEntries timed out");
                        let mut peers_guard = peers.lock().await;
                        if let Some(client) = peers_guard.get_mut(&peer_id) {
                            client.reset();
                        }
                    }
                }
            }));
        }

        // Wait for all to complete
        for f in futures {
            let _ = f.await;
        }
    }

    /// Check leadership transfer progress and send TimeoutNow if target is caught up.
    async fn check_transfer(
        node: &Arc<RaftNode>,
        peers: &Arc<Mutex<HashMap<NodeId, PeerClient>>>,
        election_timeout_ms: u64,
    ) {
        node.check_transfer_timeout(election_timeout_ms);

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
        requests: Vec<(NodeId, raft_consensus_core::message::VoteRequest)>,
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
