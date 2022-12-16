use std::{collections::HashMap, sync::Arc};

use clippy_utilities::OverflowArithmetic;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::debug;

use super::command::KeyRange;
use crate::{
    rpc::{
        RequestUnion, ResponseHeader, Watch, WatchCancelRequest, WatchCreateRequest, WatchRequest,
        WatchResponse,
    },
    storage::kvwatcher::{KvWatcher, WatchEvent, WatchId, Watcher},
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

/// Watch Server
#[derive(Debug)]
pub(crate) struct WatchServer {
    /// KV watcher
    watcher: Arc<KvWatcher>,
}

impl WatchServer {
    /// New `WatchServer`
    pub(crate) fn new(watcher: Arc<KvWatcher>) -> Self {
        Self { watcher }
    }
}

/// Handler for one watch connection
#[derive(Debug)]
struct WatchHandle {
    /// KV watcher
    kv_watcher: Arc<KvWatcher>,
    /// `WatchResponse` Sender
    response_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
    /// `WatchRequest` receiver
    request_rx: tonic::Streaming<WatchRequest>,
    /// Event receiver
    event_rx: mpsc::Receiver<WatchEvent>,
    /// Event sender
    event_tx: mpsc::Sender<WatchEvent>,
    /// Watch ID to watcher map
    watcher_map: HashMap<WatchId, Watcher>,
    /// Next available `WatchId`
    next_id: WatchId,
}

impl WatchHandle {
    /// New `WatchHandle`
    fn new(
        kv_watcher: Arc<KvWatcher>,
        response_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
        request_rx: tonic::Streaming<WatchRequest>,
        event_rx: mpsc::Receiver<WatchEvent>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> Self {
        Self {
            kv_watcher,
            response_tx,
            request_rx,
            event_rx,
            event_tx,
            watcher_map: HashMap::new(),
            next_id: 0,
        }
    }

    /// Get next available `WatchId`, return None if id is not available
    fn get_next_watch_id(&mut self, id: WatchId) -> Option<WatchId> {
        // 0 means auto-generate
        if id == 0 {
            loop {
                let next = self.next_id;
                self.next_id = self.next_id.overflow_add(1);
                if self.watcher_map.get(&next).is_none() {
                    break Some(next);
                }
            }
        } else if self.watcher_map.get(&id).is_some() {
            None
        } else {
            Some(id)
        }
    }

    /// Handle `WatchCreateRequest`
    async fn handle_watch_create(&mut self, req: WatchCreateRequest) {
        let watch_id = self.get_next_watch_id(req.watch_id);
        if let Some(watch_id) = watch_id {
            let key_range = KeyRange {
                start: req.key,
                end: req.range_end,
            };

            let (watcher, events, revision) = self
                .kv_watcher
                .watch(
                    watch_id,
                    key_range,
                    req.start_revision,
                    req.filters,
                    self.event_tx.clone(),
                )
                .await;
            assert!(
                self.watcher_map.insert(watch_id, watcher).is_none(),
                "WatchId {} already exists in watcher_map",
                watch_id
            );

            let response = WatchResponse {
                header: Some(ResponseHeader {
                    revision,
                    ..ResponseHeader::default()
                }),
                watch_id,
                created: true,
                ..WatchResponse::default()
            };
            // TODO: handle client closes connection
            assert!(
                self.response_tx.send(Ok(response)).await.is_ok(),
                "Watch client closes connection"
            );
            if !events.is_empty() {
                let event_response = WatchResponse {
                    header: Some(ResponseHeader {
                        revision,
                        ..ResponseHeader::default()
                    }),
                    watch_id,
                    events,
                    ..WatchResponse::default()
                };
                // TODO: handle client closes connection
                assert!(
                    self.response_tx.send(Ok(event_response)).await.is_ok(),
                    "Watch client closes connection"
                );
            }
        } else {
            // TODO: handle client closes connection
            assert!(
                self.response_tx
                    .send(Err(tonic::Status::already_exists(format!(
                        "Watch ID {} has already been used",
                        req.watch_id
                    ))))
                    .await
                    .is_ok(),
                "Watch client closes connection"
            );
        }
    }

    /// Handle `WatchCancelRequest`
    async fn handle_watch_cancel(&mut self, req: WatchCancelRequest) {
        let watch_id = req.watch_id;
        if let Some(watcher) = self.watcher_map.get(&watch_id) {
            let revision = self.kv_watcher.cancel(watcher);
            let _prev = self.watcher_map.remove(&watch_id);
            let response = WatchResponse {
                header: Some(ResponseHeader {
                    revision,
                    ..ResponseHeader::default()
                }),
                watch_id,
                canceled: true,
                ..WatchResponse::default()
            };
            // TODO: handle client closes connection
            assert!(
                self.response_tx.send(Ok(response)).await.is_ok(),
                "Watch client closes connection"
            );
        } else {
            // TODO: handle client closes connection
            assert!(
                self.response_tx
                    .send(Err(tonic::Status::not_found(format!(
                        "Watch ID {} doesn't exist",
                        req.watch_id
                    ))))
                    .await
                    .is_ok(),
                "Watch client closes connection"
            );
        }
    }

    /// Handle `WatchRequest`
    async fn handle_watch_request(&mut self, req: WatchRequest) {
        if let Some(req) = req.request_union {
            match req {
                RequestUnion::CreateRequest(req) => {
                    self.handle_watch_create(req).await;
                }
                RequestUnion::CancelRequest(req) => {
                    self.handle_watch_cancel(req).await;
                }
                RequestUnion::ProgressRequest(_req) => {
                    panic!("Don't support ProgressRequest yet");
                }
            }
        }
    }

    /// Handle watch event
    async fn handle_watch_event(&mut self, mut event: WatchEvent) {
        let watch_id = event.watch_id();
        let events = event.take_events();
        let response = WatchResponse {
            header: Some(ResponseHeader {
                revision: event.revision(),
                ..ResponseHeader::default()
            }),
            watch_id,
            events,
            ..WatchResponse::default()
        };
        // TODO: handle client closes connection
        assert!(
            self.response_tx.send(Ok(response)).await.is_ok(),
            "Watch client closes connection"
        );
    }
}

#[tonic::async_trait]
impl Watch for WatchServer {
    ///Server streaming response type for the Watch method.
    type WatchStream = ReceiverStream<Result<WatchResponse, tonic::Status>>;

    /// Watch watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watchers and the output
    /// stream sends events. One watch RPC can watch on multiple key ranges, streaming events
    /// for several watches at once. The entire event history can be watched starting from the
    /// last compaction revision.
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        debug!("Receive Watch Connection {:?}", request);
        let req_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        let watcher_clone = Arc::clone(&self.watcher);
        let _hd = tokio::spawn(async move {
            let (event_tx, event_rx) = mpsc::channel(CHANNEL_SIZE);
            let mut watch_handle =
                WatchHandle::new(watcher_clone, tx, req_stream, event_rx, event_tx);
            loop {
                tokio::select! {
                    req = watch_handle.request_rx.next() => {
                        if let Some(req) = req {
                            match req {
                                Ok(req) => {
                                    watch_handle.handle_watch_request(req).await;
                                }
                                Err(e) => {
                                    panic!("Receive WatchRequest error {:?}", e);
                                }
                            }
                        } else {
                            // TODO handle client close connection
                            panic!("Watch client closes connection");
                        }
                    }
                    event = watch_handle.event_rx.recv() => {
                        if let Some(event) = event {
                            watch_handle.handle_watch_event(event).await;
                        } else {
                            panic!("Watch event sender is closed");
                        }
                    }

                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
