use raft_common::types::NodeId;
use std::collections::HashMap;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::debug;

use crate::proto::kv::kv_service_client::KvServiceClient;
use crate::proto::kv::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse,
};

/// KV client with automatic leader tracking and retry with exponential backoff.
pub struct KvClient {
    /// All known node addresses.
    nodes: HashMap<NodeId, String>,
    /// Current believed leader.
    leader_id: Option<NodeId>,
    /// Cached gRPC connections.
    connections: HashMap<NodeId, KvServiceClient<Channel>>,
    /// Maximum number of retries.
    max_retries: usize,
    /// Base delay for exponential backoff.
    base_delay: Duration,
}

impl KvClient {
    pub fn new(nodes: HashMap<NodeId, String>) -> Self {
        Self {
            nodes,
            leader_id: None,
            connections: HashMap::new(),
            max_retries: 5,
            base_delay: Duration::from_millis(100),
        }
    }

    async fn get_connection(
        &mut self,
        node_id: NodeId,
    ) -> Result<&mut KvServiceClient<Channel>, tonic::Status> {
        if !self.connections.contains_key(&node_id) {
            let addr = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| tonic::Status::not_found(format!("unknown node {}", node_id)))?
                .clone();
            let channel = Channel::from_shared(addr)
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .connect()
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            self.connections
                .insert(node_id, KvServiceClient::new(channel));
        }
        Ok(self.connections.get_mut(&node_id).unwrap())
    }

    /// Pick a node to try: leader if known, otherwise first node.
    fn pick_node(&self) -> NodeId {
        self.leader_id
            .unwrap_or_else(|| *self.nodes.keys().next().unwrap())
    }

    /// Try the next node (round-robin through known nodes).
    fn next_node(&self, current: NodeId) -> NodeId {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        let pos = ids.iter().position(|&id| id == current).unwrap_or(0);
        ids[(pos + 1) % ids.len()]
    }

    /// Parse leader hint from "not leader" error messages.
    fn parse_leader_hint(status: &tonic::Status) -> Option<NodeId> {
        let msg = status.message();
        // Format: "not leader, leader is Some(2)"
        if let Some(start) = msg.find("Some(") {
            let rest = &msg[start + 5..];
            if let Some(end) = rest.find(')') {
                return rest[..end].parse().ok();
            }
        }
        None
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<GetResponse, tonic::Status> {
        let mut node_id = self.pick_node();

        for attempt in 0..self.max_retries {
            let client = self.get_connection(node_id).await?;
            match client
                .get(GetRequest {
                    key: key.to_vec(),
                    linearizable: false,
                })
                .await
            {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status) => {
                    if status.code() == tonic::Code::FailedPrecondition {
                        if let Some(leader) = Self::parse_leader_hint(&status) {
                            self.leader_id = Some(leader);
                            node_id = leader;
                            continue;
                        }
                    }
                    self.connections.remove(&node_id);
                    node_id = self.next_node(node_id);
                    let delay = self.base_delay * 2u32.pow(attempt as u32);
                    debug!(attempt, delay_ms = delay.as_millis(), "Retrying");
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(tonic::Status::unavailable("all retries exhausted"))
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<PutResponse, tonic::Status> {
        self.put_with_options(key, value, 0, 0).await
    }

    pub async fn put_with_options(
        &mut self,
        key: &[u8],
        value: &[u8],
        lease_id: i64,
        ttl_seconds: i64,
    ) -> Result<PutResponse, tonic::Status> {
        let mut node_id = self.pick_node();

        for attempt in 0..self.max_retries {
            let client = self.get_connection(node_id).await?;
            match client
                .put(PutRequest {
                    key: key.to_vec(),
                    value: value.to_vec(),
                    lease_id,
                    ttl_seconds,
                })
                .await
            {
                Ok(resp) => {
                    self.leader_id = Some(node_id);
                    return Ok(resp.into_inner());
                }
                Err(status) => {
                    if status.code() == tonic::Code::FailedPrecondition {
                        if let Some(leader) = Self::parse_leader_hint(&status) {
                            self.leader_id = Some(leader);
                            node_id = leader;
                            continue;
                        }
                    }
                    self.connections.remove(&node_id);
                    node_id = self.next_node(node_id);
                    let delay = self.base_delay * 2u32.pow(attempt as u32);
                    debug!(attempt, delay_ms = delay.as_millis(), "Retrying put");
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(tonic::Status::unavailable("all retries exhausted"))
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<DeleteResponse, tonic::Status> {
        let mut node_id = self.pick_node();

        for attempt in 0..self.max_retries {
            let client = self.get_connection(node_id).await?;
            match client.delete(DeleteRequest { key: key.to_vec() }).await {
                Ok(resp) => {
                    self.leader_id = Some(node_id);
                    return Ok(resp.into_inner());
                }
                Err(status) => {
                    if status.code() == tonic::Code::FailedPrecondition {
                        if let Some(leader) = Self::parse_leader_hint(&status) {
                            self.leader_id = Some(leader);
                            node_id = leader;
                            continue;
                        }
                    }
                    self.connections.remove(&node_id);
                    node_id = self.next_node(node_id);
                    let delay = self.base_delay * 2u32.pow(attempt as u32);
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(tonic::Status::unavailable("all retries exhausted"))
    }

    pub async fn range(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        limit: i64,
    ) -> Result<RangeResponse, tonic::Status> {
        let node_id = self.pick_node();
        let client = self.get_connection(node_id).await?;
        let resp = client
            .range(RangeRequest {
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                limit,
            })
            .await?;
        Ok(resp.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_leader_hint_from_error() {
        let status = tonic::Status::failed_precondition("not leader, leader is Some(2)");
        assert_eq!(KvClient::parse_leader_hint(&status), Some(2));
    }

    #[test]
    fn parse_leader_hint_none() {
        let status = tonic::Status::failed_precondition("not leader, leader is None");
        assert_eq!(KvClient::parse_leader_hint(&status), None);
    }
}
