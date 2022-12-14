use std::{collections::HashMap, sync::Arc};

use clippy_utilities::OverflowArithmetic;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tracing::{debug, warn};

use super::command::KeyRange;
use crate::{
    rpc::{
        RequestUnion, ResponseHeader, Watch, WatchCancelRequest, WatchCreateRequest, WatchRequest,
        WatchResponse,
    },
    storage::kvwatcher::{KvWatcher, KvWatcherOps, WatchEvent, WatchId, Watcher},
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

    /// bg task for handle watch connection
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) async fn task<S, W>(
        kv_watcher: Arc<W>,
        res_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
        mut req_rx: S,
    ) where
        S: Stream<Item = Result<WatchRequest, tonic::Status>> + Unpin,
        W: KvWatcherOps,
    {
        let (event_tx, event_rx) = mpsc::channel(CHANNEL_SIZE);
        let (stop_tx, stop_rx) = flume::bounded(0);
        let mut watch_handle = WatchHandle::new(kv_watcher, res_tx, event_rx, event_tx, stop_tx);
        loop {
            tokio::select! {
                req = req_rx.next() => {
                    if let Some(req) = req {
                        match req {
                            Ok(req) => {
                                watch_handle.handle_watch_request(req).await;
                            }
                            Err(e) => {
                                warn!("Receive WatchRequest error {:?}", e);
                                break;
                            }
                        }
                    } else {
                        warn!("Watch client closes connection");
                        break;
                    }
                }
                event = watch_handle.event_rx.recv() => {
                    if let Some(event) = event {
                        watch_handle.handle_watch_event(event).await;
                    } else {
                        panic!("Watch event sender is closed");
                    }
                }
                _ = stop_rx.recv_async() => {
                    break;
                }
            }
        }
    }
}

/// Handler for one watch connection
#[derive(Debug)]
struct WatchHandle<W>
where
    W: KvWatcherOps,
{
    /// KV watcher
    kv_watcher: Arc<W>,
    /// `WatchResponse` Sender
    response_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
    /// Event receiver
    event_rx: mpsc::Receiver<WatchEvent>,
    /// Event sender
    event_tx: mpsc::Sender<WatchEvent>,
    /// Watch ID to watcher map
    watcher_map: HashMap<WatchId, Watcher>,
    /// Next available `WatchId`
    next_id: WatchId,
    /// Stop tx
    stop_tx: flume::Sender<()>,
}

impl<W> WatchHandle<W>
where
    W: KvWatcherOps,
{
    /// New `WatchHandle`
    fn new(
        kv_watcher: Arc<W>,
        response_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
        event_rx: mpsc::Receiver<WatchEvent>,
        event_tx: mpsc::Sender<WatchEvent>,
        stop_tx: flume::Sender<()>,
    ) -> Self {
        Self {
            kv_watcher,
            response_tx,
            event_rx,
            event_tx,
            watcher_map: HashMap::new(),
            next_id: 0,
            stop_tx,
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
            if self.response_tx.send(Ok(response)).await.is_err() {
                self.stop_tx.send(()).unwrap_or_else(|e| {
                    warn!("failed to send stop signal: {}", e);
                });
            }
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
                if self.response_tx.send(Ok(event_response)).await.is_err() {
                    self.stop_tx.send(()).unwrap_or_else(|e| {
                        warn!("failed to send stop signal: {}", e);
                    });
                }
            }
        } else {
            let result = Err(tonic::Status::already_exists(format!(
                "Watch ID {} has already been used",
                req.watch_id
            )));
            if self.response_tx.send(result).await.is_err() {
                self.stop_tx.send(()).unwrap_or_else(|e| {
                    warn!("failed to send stop signal: {}", e);
                });
            }
        }
    }

    /// Handle `WatchCancelRequest`
    async fn handle_watch_cancel(&mut self, req: WatchCancelRequest) {
        let watch_id = req.watch_id;
        let result = match self.watcher_map.get(&watch_id) {
            Some(watcher) => {
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
                Ok(response)
            }
            None => Err(tonic::Status::not_found(format!(
                "Watch ID {} doesn't exist",
                req.watch_id
            ))),
        };
        if self.response_tx.send(result).await.is_err() {
            self.stop_tx.send(()).unwrap_or_else(|e| {
                warn!("failed to send stop signal: {}", e);
            });
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
        if self.response_tx.send(Ok(response)).await.is_err() {
            self.stop_tx.send(()).unwrap_or_else(|e| {
                warn!("failed to send stop signal: {}", e);
            });
        }
    }
}

impl<W> Drop for WatchHandle<W>
where
    W: KvWatcherOps,
{
    fn drop(&mut self) {
        for watcher in self.watcher_map.values() {
            let _revision = self.kv_watcher.cancel(watcher);
        }
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
    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        debug!("Receive Watch Connection {:?}", request);
        let req_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        let _hd = tokio::spawn(Self::task(Arc::clone(&self.watcher), tx, req_stream));
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod test {

    use parking_lot::Mutex;

    use super::*;
    use crate::storage::kvwatcher::MockKvWatcherOps;

    #[tokio::test]
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    async fn test_watch_client_closes_connection() -> Result<(), Box<dyn std::error::Error>> {
        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx, mut res_rx) = mpsc::channel(CHANNEL_SIZE);
        let req_stream: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx);

        let mut mock_watcher = MockKvWatcherOps::new();
        let option1 = Arc::new(Mutex::new(None));
        let option2 = Arc::clone(&option1);
        let _ = mock_watcher.expect_watch().times(1).returning(
            move |id, key_range, start_rev, filters, event_tx| {
                let watcher = Watcher::new(key_range, id, start_rev, filters, event_tx);
                *option1.lock() = Some(watcher.clone());
                (watcher, vec![], 0)
            },
        );
        let _ = mock_watcher
            .expect_cancel()
            .times(1)
            .returning(move |watcher| {
                // make sure cancel is called with the same watcher
                assert_eq!(Some(watcher), option2.lock().as_ref());
                0
            });
        let watcher = Arc::new(mock_watcher);

        let handle = tokio::spawn(WatchServer::task(Arc::clone(&watcher), res_tx, req_stream));
        req_tx
            .send(Ok(WatchRequest {
                request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                    key: vec![0],
                    range_end: vec![0],
                    ..Default::default()
                })),
            }))
            .await?;
        if let Some(Ok(res)) = res_rx.recv().await {
            assert!(res.created);
        }
        drop(req_tx);
        tokio::time::timeout(std::time::Duration::from_secs(3), handle).await??;
        Ok(())
    }
}
