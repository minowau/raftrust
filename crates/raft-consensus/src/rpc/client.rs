use raft_common::types::NodeId;
use tonic::transport::Channel;

use crate::message::{AppendRequest, AppendResponse, EntryType, VoteRequest, VoteResponse};
use crate::proto::raft::raft_service_client::RaftServiceClient;
use crate::proto::raft::{self as pb, AppendEntriesRequest, RequestVoteRequest};

/// gRPC client for peer-to-peer Raft communication.
pub struct PeerClient {
    pub node_id: NodeId,
    pub address: String,
    client: Option<RaftServiceClient<Channel>>,
}

impl PeerClient {
    pub fn new(node_id: NodeId, address: String) -> Self {
        Self {
            node_id,
            address,
            client: None,
        }
    }

    async fn connect(&mut self) -> Result<&mut RaftServiceClient<Channel>, tonic::Status> {
        if self.client.is_none() {
            let channel = Channel::from_shared(self.address.clone())
                .map_err(|e| tonic::Status::internal(e.to_string()))?
                .connect()
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            self.client = Some(RaftServiceClient::new(channel));
        }
        Ok(self.client.as_mut().unwrap())
    }

    /// Send a RequestVote RPC.
    pub async fn request_vote(&mut self, req: &VoteRequest) -> Result<VoteResponse, tonic::Status> {
        let client = self.connect().await?;
        let response = client
            .request_vote(RequestVoteRequest {
                term: req.term,
                candidate_id: req.candidate_id,
                last_log_index: req.last_log_index,
                last_log_term: req.last_log_term,
                is_pre_vote: req.is_pre_vote,
            })
            .await?;

        let resp = response.into_inner();
        Ok(VoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        })
    }

    /// Send an AppendEntries RPC.
    pub async fn append_entries(
        &mut self,
        req: &AppendRequest,
    ) -> Result<AppendResponse, tonic::Status> {
        let client = self.connect().await?;

        let entries: Vec<pb::LogEntry> = req
            .entries
            .iter()
            .map(|e| pb::LogEntry {
                index: e.index,
                term: e.term,
                data: e.data.clone(),
                entry_type: match e.entry_type {
                    EntryType::Normal => 0,
                    EntryType::ConfigChange => 1,
                    EntryType::Noop => 2,
                },
            })
            .collect();

        let response = client
            .append_entries(AppendEntriesRequest {
                term: req.term,
                leader_id: req.leader_id,
                prev_log_index: req.prev_log_index,
                prev_log_term: req.prev_log_term,
                entries,
                leader_commit: req.leader_commit,
            })
            .await?;

        let resp = response.into_inner();
        Ok(AppendResponse {
            term: resp.term,
            success: resp.success,
            match_index: resp.match_index,
        })
    }

    /// Reset the connection (e.g., after a failure).
    pub fn reset(&mut self) {
        self.client = None;
    }
}
