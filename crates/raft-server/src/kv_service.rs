use raft_consensus_core::node::RaftNode;
use raft_consensus_core::state::Role;
use raft_mvcc::mvcc::MvccStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::apply::KvCommand;
use crate::proto::kv::kv_service_server::KvService;
use crate::proto::kv::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, KeyValue, PutRequest, PutResponse,
    RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};

/// KV gRPC service: proposes writes through Raft, reads from MvccStore.
pub struct KvRpcService {
    node: Arc<RaftNode>,
    store: Arc<MvccStore>,
    /// Whether to use the read index protocol for linearizable reads.
    linearizable_reads: bool,
}

impl KvRpcService {
    pub fn new(node: Arc<RaftNode>, store: Arc<MvccStore>) -> Self {
        Self {
            node,
            store,
            linearizable_reads: true,
        }
    }

    /// Create with linearizable reads disabled (for testing or when eventual consistency is acceptable).
    pub fn with_serializable_reads(node: Arc<RaftNode>, store: Arc<MvccStore>) -> Self {
        Self {
            node,
            store,
            linearizable_reads: false,
        }
    }

    #[allow(clippy::result_large_err)]
    fn check_leader(&self) -> Result<(), Status> {
        if self.node.role() != Role::Leader {
            let leader = self.node.leader_id();
            return Err(Status::failed_precondition(format!(
                "not leader, leader is {:?}",
                leader
            )));
        }
        Ok(())
    }

    /// Wait for the read index protocol to confirm leadership,
    /// then wait for the state machine to catch up to the read index.
    async fn wait_for_linearizable_read(&self) -> Result<(), Status> {
        if !self.linearizable_reads {
            return Ok(());
        }

        // Only the leader can serve linearizable reads
        self.check_leader()?;

        let (id, read_index) = self
            .node
            .request_read_index()
            .map_err(|e| Status::internal(format!("{:?}", e)))?;

        // For single-node clusters, the read is immediately confirmed
        if self.node.is_read_index_confirmed(id) {
            self.node.take_read_index(id);
            // Wait for state machine to catch up
            self.wait_for_applied(read_index).await?;
            return Ok(());
        }

        // Wait for heartbeat quorum to confirm leadership.
        // The event loop sends heartbeats and records acks, which will
        // eventually confirm this read request.
        let mut attempts = 0;
        while !self.node.is_read_index_confirmed(id) {
            attempts += 1;
            if attempts > 200 {
                self.node.take_read_index(id); // Clean up
                return Err(Status::deadline_exceeded(
                    "read index confirmation timed out",
                ));
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

            // Check if we're still leader
            if self.node.role() != Role::Leader {
                return Err(Status::failed_precondition("lost leadership during read"));
            }
        }

        self.node.take_read_index(id);
        self.wait_for_applied(read_index).await
    }

    /// Wait for the state machine to advance past the given index.
    async fn wait_for_applied(&self, target_index: u64) -> Result<(), Status> {
        let mut attempts = 0;
        while self.node.last_applied() < target_index {
            attempts += 1;
            if attempts > 100 {
                return Err(Status::deadline_exceeded(
                    "timed out waiting for state machine to catch up",
                ));
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl KvService for KvRpcService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        // Linearizable read: confirm leadership before serving
        if req.linearizable {
            self.wait_for_linearizable_read().await?;
        } else {
            self.check_leader()?;
        }

        let result = self
            .store
            .get(&req.key)
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Some(kv) => Ok(Response::new(GetResponse {
                value: kv.value,
                revision: kv.mod_revision,
            })),
            None => Ok(Response::new(GetResponse {
                value: vec![],
                revision: 0,
            })),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        self.check_leader()?;

        let req = request.into_inner();
        let cmd = KvCommand::Put {
            key: req.key,
            value: req.value,
            lease_id: req.lease_id,
            ttl_seconds: req.ttl_seconds,
        };

        let index = self
            .node
            .propose(cmd.encode())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;

        // In a full implementation, we'd wait for the entry to be committed
        // and applied, then return the revision. For now, return the log index.
        Ok(Response::new(PutResponse { revision: index }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.check_leader()?;

        let req = request.into_inner();
        let cmd = KvCommand::Delete { key: req.key };

        let index = self
            .node
            .propose(cmd.encode())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;

        Ok(Response::new(DeleteResponse {
            revision: index,
            deleted: true,
        }))
    }

    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();

        // Linearizable read: confirm leadership before serving
        self.wait_for_linearizable_read().await?;

        let results = self
            .store
            .scan(&req.start_key, &req.end_key)
            .map_err(|e| Status::internal(e.to_string()))?;

        let kvs: Vec<KeyValue> = results
            .into_iter()
            .take(if req.limit > 0 {
                req.limit as usize
            } else {
                usize::MAX
            })
            .map(|kv| KeyValue {
                key: kv.key,
                value: kv.value,
                create_revision: kv.create_revision,
                mod_revision: kv.mod_revision,
                lease_id: kv.lease_id,
            })
            .collect();

        let count = kvs.len() as u64;
        Ok(Response::new(RangeResponse { kvs, count }))
    }

    async fn txn(&self, _request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        // Full transaction support through Raft requires
        // serializing the entire transaction as a single log entry.
        // This will be wired in when we add the transaction proposal path.
        Err(Status::unimplemented("txn not yet implemented"))
    }
}
