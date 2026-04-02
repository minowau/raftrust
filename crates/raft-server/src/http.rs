use raft_common::metrics::Metrics;
use raft_consensus_core::node::RaftNode;
use raft_consensus_core::state::Role;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

/// Lightweight HTTP server for operational endpoints.
///
/// Endpoints:
/// - `GET /metrics` — Prometheus text exposition format
/// - `GET /health` — Returns 200 if the process is alive
/// - `GET /ready` — Returns 200 only when the node has a leader and is operational
pub struct HttpServer {
    node: Arc<RaftNode>,
    metrics: Arc<Metrics>,
}

impl HttpServer {
    pub fn new(node: Arc<RaftNode>, metrics: Arc<Metrics>) -> Self {
        Self { node, metrics }
    }

    /// Start the HTTP server on the given address (e.g., "0.0.0.0:9090").
    pub async fn run(&self, addr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(addr).await?;
        info!(addr = addr, "HTTP server listening");

        loop {
            let (mut stream, peer) = listener.accept().await?;
            let node = self.node.clone();
            let metrics = self.metrics.clone();

            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = match tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };

                let request = String::from_utf8_lossy(&buf[..n]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");

                debug!(peer = %peer, path = path, "HTTP request");

                let response = match path {
                    "/metrics" => {
                        // Update gauges before scraping
                        update_raft_gauges(&node, &metrics);
                        let body = metrics.encode();
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                    "/health" => {
                        let body = r#"{"status":"ok"}"#;
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                    "/ready" => {
                        let role = node.role();
                        let has_leader = node.leader_id().is_some();
                        let is_ready = has_leader || role == Role::Leader;

                        if is_ready {
                            let body = format!(
                                r#"{{"status":"ready","role":"{:?}","leader":{}}}"#,
                                role,
                                node.leader_id().unwrap_or(0)
                            );
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                body.len(),
                                body
                            )
                        } else {
                            let body = r#"{"status":"not_ready","reason":"no_leader"}"#;
                            format!(
                                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                body.len(),
                                body
                            )
                        }
                    }
                    _ => {
                        let body = "Not Found";
                        format!(
                            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                };

                if let Err(e) = stream.write_all(response.as_bytes()).await {
                    warn!(error = %e, "Failed to write HTTP response");
                }
            });
        }
    }
}

/// Update Raft gauge metrics from current node state.
fn update_raft_gauges(node: &RaftNode, metrics: &Metrics) {
    let role_num = match node.role() {
        Role::Follower | Role::PreCandidate => 0,
        Role::Candidate => 1,
        Role::Leader => 2,
    };
    metrics.raft_role.set(role_num);
    metrics.raft_term.set(node.term() as i64);
    metrics.raft_commit_index.set(node.commit_index() as i64);
    metrics.raft_applied_index.set(node.last_applied() as i64);
    metrics
        .raft_leader_id
        .set(node.leader_id().unwrap_or(0) as i64);
    metrics
        .raft_cluster_size
        .set(node.membership().current_size() as i64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_consensus_core::node::NodeConfig;
    use raft_consensus_core::tick::TickConfig;

    fn test_node(dir: &std::path::Path) -> Arc<RaftNode> {
        Arc::new(
            RaftNode::new(NodeConfig {
                id: 1,
                peers: vec![2, 3],
                data_dir: dir.to_string_lossy().to_string(),
                tick_config: TickConfig::default(),
            })
            .unwrap(),
        )
    }

    #[test]
    fn update_gauges_from_node() {
        let dir = tempfile::tempdir().unwrap();
        let node = test_node(dir.path());
        let metrics = Metrics::new();

        update_raft_gauges(&node, &metrics);

        assert_eq!(metrics.raft_role.get(), 0); // Follower
        assert_eq!(metrics.raft_term.get(), 0);
        assert_eq!(metrics.raft_cluster_size.get(), 3);
    }

    #[test]
    fn metrics_encode_contains_all_families() {
        let metrics = Metrics::new();

        // Initialize a label value so Vec metrics appear in output
        metrics.kv_ops_total.with_label_values(&["get"]).inc();

        let output = metrics.encode();

        // Spot-check that key metric families appear
        assert!(output.contains("raft_term"));
        assert!(output.contains("raft_commit_index"));
        assert!(output.contains("kv_ops_total"));
        assert!(output.contains("lease_active"));
        assert!(output.contains("watch_active"));
    }
}
