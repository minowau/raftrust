use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::message::{AppendRequest, EntryType, LogEntry, TimeoutNow, VoteRequest};
use crate::node::RaftNode;
use crate::proto::raft::raft_service_server::RaftService;
use crate::proto::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotChunk, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest, TimeoutNowResponse,
    TransferLeadershipRequest, TransferLeadershipResponse,
};

/// gRPC service implementation for Raft RPCs.
pub struct RaftRpcServer {
    node: Arc<RaftNode>,
}

impl RaftRpcServer {
    pub fn new(node: Arc<RaftNode>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl RaftService for RaftRpcServer {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let vote_req = VoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
            is_pre_vote: req.is_pre_vote,
        };

        let resp = self.node.handle_vote_request(vote_req);

        Ok(Response::new(RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let entries: Vec<LogEntry> = req
            .entries
            .into_iter()
            .map(|e| LogEntry {
                index: e.index,
                term: e.term,
                data: e.data,
                entry_type: match e.entry_type {
                    1 => EntryType::ConfigChange,
                    2 => EntryType::Noop,
                    _ => EntryType::Normal,
                },
            })
            .collect();

        let append_req = AppendRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries,
            leader_commit: req.leader_commit,
        };

        let resp = self.node.handle_append_entries(append_req);

        Ok(Response::new(AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            match_index: resp.match_index,
        }))
    }

    async fn install_snapshot(
        &self,
        _request: Request<tonic::Streaming<InstallSnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        // Phase 5 implementation
        Err(Status::unimplemented(
            "install_snapshot not yet implemented",
        ))
    }

    async fn transfer_leadership(
        &self,
        request: Request<TransferLeadershipRequest>,
    ) -> Result<Response<TransferLeadershipResponse>, Status> {
        let req = request.into_inner();

        match self.node.transfer_leadership(req.transferee) {
            Ok(()) => Ok(Response::new(TransferLeadershipResponse { success: true })),
            Err(e) => Err(Status::failed_precondition(format!("{:?}", e))),
        }
    }

    async fn timeout_now(
        &self,
        request: Request<TimeoutNowRequest>,
    ) -> Result<Response<TimeoutNowResponse>, Status> {
        let req = request.into_inner();
        let msg = TimeoutNow {
            term: req.term,
            leader_id: req.leader_id,
        };

        // handle_timeout_now starts an immediate election and returns vote requests.
        // The actual vote sending is handled by the event loop, so we just trigger the state change.
        let _vote_requests = self.node.handle_timeout_now(msg);

        Ok(Response::new(TimeoutNowResponse {
            term: self.node.term(),
        }))
    }
}
