use std::{collections::HashSet, sync::Arc};

use event_listener::Event;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tracing::{debug, warn};

use super::command::KeyRange;
use crate::{
    header_gen::HeaderGenerator,
    rpc::{
        RequestUnion, ResponseHeader, Watch, WatchCancelRequest, WatchCreateRequest, WatchRequest,
        WatchResponse,
    },
    storage::{
        kvwatcher::{KvWatcher, KvWatcherOps, WatchEvent, WatchId, WatchIdGenerator},
        storage_api::StorageApi,
    },
};

/// Default channel size
pub(crate) const CHANNEL_SIZE: usize = 128;

/// Watch Server
#[derive(Debug)]
pub(crate) struct WatchServer<S>
where
    S: StorageApi,
{
    /// KV watcher
    watcher: Arc<KvWatcher<S>>,
    /// Watch ID generator
    next_id_gen: Arc<WatchIdGenerator>,
    /// Header Generator
    header_gen: Arc<HeaderGenerator>,
}

impl<S> WatchServer<S>
where
    S: StorageApi,
{
    /// New `WatchServer`
    pub(crate) fn new(watcher: Arc<KvWatcher<S>>, header_gen: Arc<HeaderGenerator>) -> Self {
        Self {
            watcher,
            next_id_gen: Arc::new(WatchIdGenerator::new(1)), // watch_id starts from 1, 0 means auto-generating
            header_gen,
        }
    }

    /// bg task for handle watch connection
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    async fn task<ST, W>(
        next_id_gen: Arc<WatchIdGenerator>,
        kv_watcher: Arc<W>,
        res_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
        mut req_rx: ST,
        header_gen: Arc<HeaderGenerator>,
    ) where
        ST: Stream<Item = Result<WatchRequest, tonic::Status>> + Unpin,
        W: KvWatcherOps,
    {
        let (event_tx, mut event_rx) = mpsc::channel(CHANNEL_SIZE);
        let stop_notify = Arc::new(Event::new());
        let mut watch_handle = WatchHandle::new(
            kv_watcher,
            res_tx,
            event_tx,
            Arc::clone(&stop_notify),
            next_id_gen,
            header_gen,
        );
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
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        watch_handle.handle_watch_event(event).await;
                    } else {
                        panic!("Watch event sender is closed");
                    }
                }
                _ = stop_notify.listen() => {
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
    /// Event sender
    event_tx: mpsc::Sender<WatchEvent>,
    /// Watch ID to watcher map
    active_watch_ids: HashSet<WatchId>,
    /// Next available `WatchId`
    next_id_gen: Arc<WatchIdGenerator>,
    /// Stop Event
    stop_notify: Arc<Event>,
    /// Header Generator
    header_gen: Arc<HeaderGenerator>,
}

impl<W> WatchHandle<W>
where
    W: KvWatcherOps,
{
    /// New `WatchHandle`
    fn new(
        kv_watcher: Arc<W>,
        response_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
        event_tx: mpsc::Sender<WatchEvent>,
        stop_notify: Arc<Event>,
        next_id_gen: Arc<WatchIdGenerator>,
        header_gen: Arc<HeaderGenerator>,
    ) -> Self {
        Self {
            kv_watcher,
            response_tx,
            event_tx,
            active_watch_ids: HashSet::new(),
            next_id_gen,
            stop_notify,
            header_gen,
        }
    }

    /// Validate the given `watch_id`, return None if the given id is not available, will generate a new one if the given one equals 0
    fn validate_watch_id(&mut self, watch_id: WatchId) -> Option<WatchId> {
        // 0 means auto-generate
        if watch_id == 0 {
            loop {
                let next = self.next_id_gen.next();
                if !self.active_watch_ids.contains(&next) {
                    break Some(next);
                }
            }
        } else if self.active_watch_ids.contains(&watch_id) {
            None
        } else {
            Some(watch_id)
        }
    }

    /// Handle `WatchCreateRequest`
    async fn handle_watch_create(&mut self, req: WatchCreateRequest) {
        let Some(watch_id) = self.validate_watch_id(req.watch_id) else {
            let result = Err(tonic::Status::already_exists(format!(
                "Watch ID {} has already been used",
                req.watch_id
            )));
            if self.response_tx.send(result).await.is_err() {
                self.stop_notify.notify(1);
            }
            return;
        };

        let key_range = KeyRange::new(req.key, req.range_end);
        self.kv_watcher.watch(
            watch_id,
            key_range,
            req.start_revision,
            req.filters,
            self.event_tx.clone(),
        );
        assert!(
            self.active_watch_ids.insert(watch_id),
            "WatchId {watch_id} already exists in watcher_map",
        );

        let response = WatchResponse {
            header: Some(self.header_gen.gen_header()),
            watch_id,
            created: true,
            ..WatchResponse::default()
        };
        if self.response_tx.send(Ok(response)).await.is_err() {
            self.stop_notify.notify(1);
        }
    }

    /// Handle `WatchCancelRequest`
    async fn handle_watch_cancel(&mut self, req: WatchCancelRequest) {
        let watch_id = req.watch_id;
        let result = if self.active_watch_ids.remove(&watch_id) {
            let revision = self.kv_watcher.cancel(watch_id);
            let _prev = self.active_watch_ids.remove(&watch_id);
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
        } else {
            Err(tonic::Status::not_found(format!(
                "Watch ID {} doesn't exist",
                req.watch_id
            )))
        };
        if self.response_tx.send(result).await.is_err() {
            self.stop_notify.notify(1);
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
        if events.is_empty() {
            return;
        }
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
            self.stop_notify.notify(1);
        }
    }
}

impl<W> Drop for WatchHandle<W>
where
    W: KvWatcherOps,
{
    fn drop(&mut self) {
        for watch_id in &self.active_watch_ids {
            let _revision = self.kv_watcher.cancel(*watch_id);
        }
    }
}

#[tonic::async_trait]
impl<S> Watch for WatchServer<S>
where
    S: StorageApi,
{
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
        let _hd = tokio::spawn(Self::task(
            Arc::clone(&self.next_id_gen),
            Arc::clone(&self.watcher),
            tx,
            req_stream,
            Arc::clone(&self.header_gen),
        ));
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashMap;

    use parking_lot::Mutex;

    use super::*;
    use crate::storage::{db::DB, kvwatcher::MockKvWatcherOps};

    #[tokio::test]
    async fn test_watch_client_closes_connection() -> Result<(), Box<dyn std::error::Error>> {
        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx, mut res_rx) = mpsc::channel(CHANNEL_SIZE);
        let req_stream: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx);
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let mut mock_watcher = MockKvWatcherOps::new();
        let _ = mock_watcher.expect_watch().times(1).return_const(());
        let _ = mock_watcher.expect_cancel().times(1).returning(move |_| 0);
        let watcher = Arc::new(mock_watcher);
        let next_id = Arc::new(WatchIdGenerator::new(1));
        let handle = tokio::spawn(WatchServer::<DB>::task(
            next_id,
            Arc::clone(&watcher),
            res_tx,
            req_stream,
            header_gen,
        ));
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

    #[tokio::test]
    #[allow(clippy::similar_names)] // use num as suffix
    async fn test_multi_watch_handle() -> Result<(), Box<dyn std::error::Error>> {
        let mut mock_watcher = MockKvWatcherOps::new();
        let collection = Arc::new(Mutex::new(HashMap::new()));
        let collection_c = Arc::clone(&collection);
        let _ = mock_watcher.expect_watch().times(2).returning({
            move |x, _, _, _, _| {
                let mut c = collection_c.lock();
                let e = c.entry(x).or_insert(0);
                *e += 1;
            }
        });
        let _ = mock_watcher.expect_cancel().returning(move |_| 0);
        let kv_watcher = Arc::new(mock_watcher);
        let next_id_gen = Arc::new(WatchIdGenerator::new(1));
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));

        let (req_tx1, req_rx1) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx1, _res_rx1) = mpsc::channel(CHANNEL_SIZE);
        let req_stream1: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx1);
        let handle1 = tokio::spawn(WatchServer::<DB>::task(
            Arc::clone(&next_id_gen),
            Arc::clone(&kv_watcher),
            res_tx1,
            req_stream1,
            Arc::clone(&header_gen),
        ));

        let (req_tx2, req_rx2) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx2, _res_rx2) = mpsc::channel(CHANNEL_SIZE);
        let req_stream2: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx2);
        let handle2 = tokio::spawn(WatchServer::<DB>::task(
            next_id_gen,
            kv_watcher,
            res_tx2,
            req_stream2,
            header_gen,
        ));

        let w_req = WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                key: vec![0],
                range_end: vec![0],
                ..Default::default()
            })),
        };
        req_tx1.send(Ok(w_req.clone())).await?;
        req_tx2.send(Ok(w_req.clone())).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        for count in collection.lock().values() {
            assert_eq!(*count, 1);
        }
        handle1.abort();
        handle2.abort();
        Ok(())
    }
}
