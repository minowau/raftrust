use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use crate::proto::kv::KeyValue;
use crate::proto::watch::watch_service_server::WatchService;
use crate::proto::watch::{watch_request::Request as WatchReqType, Event, WatchResponse};
use crate::watch::{WatchEventType, WatchHub};

/// gRPC streaming Watch service.
///
/// Clients open a bidirectional stream:
/// - Send WatchCreateRequest to start watching a key/range
/// - Send WatchCancelRequest to stop watching
/// - Receive WatchResponse with events as keys change
pub struct WatchRpcService {
    hub: Arc<WatchHub>,
}

impl WatchRpcService {
    pub fn new(hub: Arc<WatchHub>) -> Self {
        Self { hub }
    }
}

#[tonic::async_trait]
impl WatchService for WatchRpcService {
    type WatchStream = ReceiverStream<Result<WatchResponse, Status>>;

    async fn watch(
        &self,
        request: Request<Streaming<crate::proto::watch::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut in_stream = request.into_inner();
        let hub = self.hub.clone();

        // Channel for sending responses back to the client
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            // Active watch IDs for this connection and their broadcast receivers
            let mut active_watches: Vec<(
                i64,
                tokio::sync::broadcast::Receiver<Arc<crate::watch::WatchEvent>>,
            )> = Vec::new();

            loop {
                tokio::select! {
                    // Handle incoming requests from client
                    maybe_req = in_stream.next() => {
                        match maybe_req {
                            Some(Ok(req)) => {
                                match req.request {
                                    Some(WatchReqType::Create(create)) => {
                                        let (watch_id, receiver) = hub.create_watcher(
                                            create.key,
                                            create.range_end,
                                            create.start_revision,
                                        );
                                        active_watches.push((watch_id, receiver));

                                        // Send created confirmation
                                        let resp = WatchResponse {
                                            watch_id,
                                            created: true,
                                            canceled: false,
                                            events: vec![],
                                        };
                                        if tx.send(Ok(resp)).await.is_err() {
                                            break;
                                        }
                                    }
                                    Some(WatchReqType::Cancel(cancel)) => {
                                        let canceled = hub.cancel_watcher(cancel.watch_id);
                                        active_watches.retain(|(id, _)| *id != cancel.watch_id);

                                        let resp = WatchResponse {
                                            watch_id: cancel.watch_id,
                                            created: false,
                                            canceled,
                                            events: vec![],
                                        };
                                        if tx.send(Ok(resp)).await.is_err() {
                                            break;
                                        }
                                    }
                                    None => {}
                                }
                            }
                            Some(Err(e)) => {
                                warn!(error = %e, "Watch stream error");
                                break;
                            }
                            None => {
                                debug!("Watch client disconnected");
                                break;
                            }
                        }
                    }
                    // Poll all active watch receivers for events
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                        for (watch_id, receiver) in &mut active_watches {
                            while let Ok(event) = receiver.try_recv() {
                                if !hub.watcher_matches(*watch_id, &event) {
                                    continue;
                                }

                                let proto_event = Event {
                                    event_type: match event.event_type {
                                        WatchEventType::Put => 0,
                                        WatchEventType::Delete => 1,
                                    },
                                    kv: Some(KeyValue {
                                        key: event.key.clone(),
                                        value: event.value.clone(),
                                        create_revision: event.create_revision,
                                        mod_revision: event.mod_revision,
                                        lease_id: 0,
                                    }),
                                    prev_kv: event.prev_value.as_ref().map(|pv| KeyValue {
                                        key: event.key.clone(),
                                        value: pv.clone(),
                                        create_revision: 0,
                                        mod_revision: 0,
                                        lease_id: 0,
                                    }),
                                };

                                let resp = WatchResponse {
                                    watch_id: *watch_id,
                                    created: false,
                                    canceled: false,
                                    events: vec![proto_event],
                                };

                                if tx.send(Ok(resp)).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            // Cleanup: cancel all watchers on disconnect
            for (watch_id, _) in &active_watches {
                hub.cancel_watcher(*watch_id);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
