use raft_consensus::node::RaftNode;
use raft_consensus::state::Role;
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
}

impl KvRpcService {
    pub fn new(node: Arc<RaftNode>, store: Arc<MvccStore>) -> Self {
        Self { node, store }
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
}

#[tonic::async_trait]
impl KvService for KvRpcService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        // Reads can be served from any node for now.
        // Phase 6 adds linearizable reads via read index.
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
