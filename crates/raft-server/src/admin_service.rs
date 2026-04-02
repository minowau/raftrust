use raft_consensus::membership::ConfigChange;
use raft_consensus::node::RaftNode;
use raft_consensus::state::Role;
use raft_mvcc::mvcc::MvccStore;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::backup;
use crate::proto::admin::admin_service_server::AdminService;
use crate::proto::admin::{
    AddNodeRequest, AddNodeResponse, BackupRequest, BackupResponse, DrainNodeRequest,
    DrainNodeResponse, RemoveNodeRequest, RemoveNodeResponse, RestoreRequest, RestoreResponse,
    StatusRequest, StatusResponse, TransferLeadershipRequest, TransferLeadershipResponse,
    TriggerCompactionRequest, TriggerCompactionResponse,
};

/// Admin gRPC service: membership changes, transfer leadership, compaction, backup/restore.
pub struct AdminRpcService {
    node: Arc<RaftNode>,
    store: Arc<MvccStore>,
}

impl AdminRpcService {
    pub fn new(node: Arc<RaftNode>, store: Arc<MvccStore>) -> Self {
        Self { node, store }
    }
}

#[tonic::async_trait]
impl AdminService for AdminRpcService {
    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();

        self.node
            .propose_config_change(ConfigChange::AddNode {
                id: req.node_id,
                address: req.address,
            })
            .map_err(|e| Status::failed_precondition(format!("{:?}", e)))?;

        Ok(Response::new(AddNodeResponse { success: true }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();

        self.node
            .propose_config_change(ConfigChange::RemoveNode { id: req.node_id })
            .map_err(|e| Status::failed_precondition(format!("{:?}", e)))?;

        Ok(Response::new(RemoveNodeResponse { success: true }))
    }

    async fn transfer_leadership(
        &self,
        request: Request<TransferLeadershipRequest>,
    ) -> Result<Response<TransferLeadershipResponse>, Status> {
        let req = request.into_inner();

        self.node
            .transfer_leadership(req.transferee)
            .map_err(|e| Status::failed_precondition(format!("{:?}", e)))?;

        Ok(Response::new(TransferLeadershipResponse { success: true }))
    }

    async fn drain_node(
        &self,
        _request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeResponse>, Status> {
        // Drain: if this node is the leader, transfer leadership to another node.
        // Then the node can be safely shut down for maintenance.
        if self.node.role() == Role::Leader {
            // Pick the first peer as the transfer target
            let peers = self.node.peers();
            if let Some(&target) = peers.first() {
                self.node
                    .transfer_leadership(target)
                    .map_err(|e| Status::failed_precondition(format!("{:?}", e)))?;

                info!(target = target, "Draining: initiated leadership transfer");
            }
        }

        Ok(Response::new(DrainNodeResponse { success: true }))
    }

    async fn trigger_compaction(
        &self,
        _request: Request<TriggerCompactionRequest>,
    ) -> Result<Response<TriggerCompactionResponse>, Status> {
        // Serialize current MVCC state as snapshot data
        // In a real system this would be a proper state machine snapshot.
        // For now, we trigger a Raft snapshot with a marker.
        let snapshot_data = b"compaction-triggered";

        match self.node.trigger_snapshot(snapshot_data) {
            Some(meta) => {
                info!(
                    index = meta.last_included_index,
                    "Triggered manual compaction/snapshot"
                );
                Ok(Response::new(TriggerCompactionResponse { success: true }))
            }
            None => Err(Status::failed_precondition(
                "nothing to compact (commit_index is 0)",
            )),
        }
    }

    type BackupStream = ReceiverStream<Result<BackupResponse, Status>>;

    async fn backup(
        &self,
        _request: Request<BackupRequest>,
    ) -> Result<Response<Self::BackupStream>, Status> {
        let (metadata, data) = backup::create_backup(&self.node, &self.store)
            .ok_or_else(|| Status::failed_precondition("no snapshot available for backup"))?;

        info!(
            revision = metadata.revision,
            commit_index = metadata.commit_index,
            "Streaming backup"
        );

        let (tx, rx) = mpsc::channel(16);

        // Stream the backup data in chunks
        tokio::spawn(async move {
            let chunk_size = 64 * 1024; // 64KB chunks
            for chunk in data.chunks(chunk_size) {
                if tx
                    .send(Ok(BackupResponse {
                        data: chunk.to_vec(),
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn restore(
        &self,
        request: Request<tonic::Streaming<RestoreRequest>>,
    ) -> Result<Response<RestoreResponse>, Status> {
        let mut stream = request.into_inner();
        let mut data = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            data.extend_from_slice(&chunk.data);
        }

        if data.is_empty() {
            return Err(Status::invalid_argument("empty restore data"));
        }

        let (metadata, snapshot_data) = backup::parse_backup(&data)
            .ok_or_else(|| Status::invalid_argument("invalid backup format"))?;

        info!(
            revision = metadata.revision,
            commit_index = metadata.commit_index,
            "Restoring from backup"
        );

        // Install the snapshot via the Raft node
        let snap_meta = raft_consensus::snapshot::SnapshotMetadata {
            last_included_index: metadata.commit_index,
            last_included_term: metadata.term,
            checksum: crc32fast::hash(&snapshot_data),
            size: snapshot_data.len() as u64,
        };

        self.node.handle_install_snapshot(
            metadata.term,
            metadata.node_id,
            &snap_meta,
            &snapshot_data,
        );

        Ok(Response::new(RestoreResponse { success: true }))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let membership = self.node.membership();

        Ok(Response::new(StatusResponse {
            node_id: self.node.id(),
            role: format!("{:?}", self.node.role()),
            term: self.node.term(),
            leader_id: self.node.leader_id().unwrap_or(0),
            commit_index: self.node.commit_index(),
            applied_index: self.node.last_applied(),
            cluster_size: membership.current_size() as u64,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_consensus::node::NodeConfig;
    use raft_consensus::tick::TickConfig;
    use raft_storage::lsm::{LsmConfig, LsmTree};

    fn test_setup(dir: &std::path::Path) -> (Arc<RaftNode>, Arc<MvccStore>) {
        let node_dir = dir.join("node");
        let store_dir = dir.join("store");
        std::fs::create_dir_all(&node_dir).unwrap();
        std::fs::create_dir_all(&store_dir).unwrap();

        let node = Arc::new(
            RaftNode::new(NodeConfig {
                id: 1,
                peers: vec![2, 3],
                data_dir: node_dir.to_string_lossy().to_string(),
                tick_config: TickConfig::default(),
            })
            .unwrap(),
        );

        let engine = Arc::new(
            LsmTree::open(
                &store_dir,
                LsmConfig {
                    memtable_size_limit: 64 * 1024,
                    block_size: 256,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let store = Arc::new(MvccStore::new(engine));

        (node, store)
    }

    #[test]
    fn admin_service_creation() {
        let dir = tempfile::tempdir().unwrap();
        let (node, store) = test_setup(dir.path());
        let _service = AdminRpcService::new(node, store);
    }
}
