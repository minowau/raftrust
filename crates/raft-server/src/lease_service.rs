use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

use crate::lease::LeaseManager;
use crate::proto::lease::lease_service_server::LeaseService;
use crate::proto::lease::{
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseRevokeRequest, LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
};

/// gRPC service for lease operations.
///
/// Leases are time-bound: if a client fails to send keepalive before the TTL,
/// the lease expires and all attached keys are deleted. This enables:
/// - **Distributed locks**: attach a key like `/locks/my-resource` to a lease;
///   if the lock holder dies, the lease expires and the key is deleted.
/// - **Ephemeral keys**: service registration keys that auto-expire on failure.
pub struct LeaseRpcService {
    lease_mgr: Arc<LeaseManager>,
}

impl LeaseRpcService {
    pub fn new(lease_mgr: Arc<LeaseManager>) -> Self {
        Self { lease_mgr }
    }
}

#[tonic::async_trait]
impl LeaseService for LeaseRpcService {
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let req = request.into_inner();

        let (id, ttl) = self
            .lease_mgr
            .grant(req.id, req.ttl)
            .map_err(|e| Status::failed_precondition(format!("{:?}", e)))?;

        Ok(Response::new(LeaseGrantResponse { id, ttl }))
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let req = request.into_inner();

        // Revoke returns the keys that need to be deleted.
        // In a full implementation, these deletes would be proposed through Raft.
        // For now, we revoke the lease and the expiry loop handles key deletion.
        let _keys = self
            .lease_mgr
            .revoke(req.id)
            .map_err(|e| Status::failed_precondition(format!("{:?}", e)))?;

        Ok(Response::new(LeaseRevokeResponse {}))
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, Status>>;

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let mut in_stream = request.into_inner();
        let lease_mgr = self.lease_mgr.clone();

        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        let response = match lease_mgr.keepalive(req.id) {
                            Ok(ttl) => LeaseKeepAliveResponse { id: req.id, ttl },
                            Err(e) => {
                                debug!(lease_id = req.id, error = ?e, "Keepalive failed");
                                LeaseKeepAliveResponse {
                                    id: req.id,
                                    ttl: 0, // 0 indicates lease not found or expired
                                }
                            }
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "Keepalive stream error");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let req = request.into_inner();

        let info = self
            .lease_mgr
            .get(req.id)
            .map_err(|e| Status::not_found(format!("{:?}", e)))?;

        Ok(Response::new(LeaseTimeToLiveResponse {
            id: info.id,
            ttl: info.ttl,
            granted_ttl: info.granted_ttl,
            keys: if req.keys { info.keys } else { vec![] },
        }))
    }
}
