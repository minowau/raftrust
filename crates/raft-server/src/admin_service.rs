use raft_consensus::membership::ConfigChange;
use raft_consensus::node::RaftNode;
use std::sync::Arc;
use tonic::{Request, Response, Status};

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
}

impl AdminRpcService {
    pub fn new(node: Arc<RaftNode>) -> Self {
        Self { node }
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
        // Phase 8: drain stops accepting writes and transfers leadership
        Err(Status::unimplemented("drain not yet implemented"))
    }

    async fn trigger_compaction(
        &self,
        _request: Request<TriggerCompactionRequest>,
    ) -> Result<Response<TriggerCompactionResponse>, Status> {
        // Phase 8: trigger manual compaction
        Err(Status::unimplemented(
            "trigger_compaction not yet implemented",
        ))
    }

    type BackupStream = tokio_stream::wrappers::ReceiverStream<Result<BackupResponse, Status>>;

    async fn backup(
        &self,
        _request: Request<BackupRequest>,
    ) -> Result<Response<Self::BackupStream>, Status> {
        // Phase 8: point-in-time backup
        Err(Status::unimplemented("backup not yet implemented"))
    }

    async fn restore(
        &self,
        _request: Request<tonic::Streaming<RestoreRequest>>,
    ) -> Result<Response<RestoreResponse>, Status> {
        // Phase 8: restore from backup
        Err(Status::unimplemented("restore not yet implemented"))
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
