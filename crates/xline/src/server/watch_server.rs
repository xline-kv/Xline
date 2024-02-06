use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use event_listener::Event;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tracing::{debug, warn};
use utils::task_manager::{tasks::TaskName, Listener, TaskManager};
use xlineapi::command::KeyRange;

use crate::{
    header_gen::HeaderGenerator,
    rpc::{
        RequestUnion, ResponseHeader, Watch, WatchCancelRequest, WatchCreateRequest,
        WatchProgressRequest, WatchRequest, WatchResponse,
    },
    storage::{
        kvwatcher::{KvWatcher, KvWatcherOps, WatchEvent, WatchId, WatchIdGenerator},
        storage_api::StorageApi,
    },
};

/// Default channel size
pub(crate) const CHANNEL_SIZE: usize = 1024;

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
    /// Watch progress notify interval
    watch_progress_notify_interval: Duration,
    /// Task manager
    task_manager: Arc<TaskManager>,
}

impl<S> WatchServer<S>
where
    S: StorageApi,
{
    /// New `WatchServer`
    pub(crate) fn new(
        watcher: Arc<KvWatcher<S>>,
        header_gen: Arc<HeaderGenerator>,
        watch_progress_notify_interval: Duration,
        task_manager: Arc<TaskManager>,
    ) -> Self {
        Self {
            watcher,
            next_id_gen: Arc::new(WatchIdGenerator::new(1)), // watch_id starts from 1, 0 means auto-generating
            header_gen,
            watch_progress_notify_interval,
            task_manager,
        }
    }

    /// bg task for handle watch connection
    #[allow(clippy::arithmetic_side_effects)] // Introduced by tokio::select!
    async fn task<ST, W>(
        next_id_gen: Arc<WatchIdGenerator>,
        kv_watcher: Arc<W>,
        res_tx: mpsc::Sender<Result<WatchResponse, tonic::Status>>,
        mut req_rx: ST,
        header_gen: Arc<HeaderGenerator>,
        watch_progress_notify_interval: Duration,
        shutdown_listener: Listener,
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
        let mut ticker = tokio::time::interval(watch_progress_notify_interval);
        let stop_listener = stop_notify.listen();
        tokio::pin!(stop_listener);
        loop {
            tokio::select! {
                _ = shutdown_listener.wait() => break,
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
                _ = ticker.tick() => {
                    watch_handle.handle_tick_progress().await;
                }
                // To ensure that each iteration invokes the same `stop_listener` and keeps
                // events losing due to the cancellation of `stop_listener` at bay.
                _ = &mut stop_listener => {
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
    /// Previous KV status
    prev_kv: HashSet<WatchId>,
    /// Progress status
    ///
    /// `true` means the next tick should be notified
    ///
    /// `false` means the next tick should be skipped
    progress: HashMap<WatchId, bool>,
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
            prev_kv: HashSet::new(),
            progress: HashMap::new(),
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
            Arc::clone(&self.stop_notify),
            self.event_tx.clone(),
        );
        if req.prev_kv {
            assert!(
                self.prev_kv.insert(watch_id),
                "WatchId {watch_id} already exists in prev_kv",
            );
        }
        if req.progress_notify {
            assert!(
                self.progress.insert(watch_id, true).is_none(),
                "WatchId {watch_id} already exists in progress",
            );
        }
        assert!(
            self.active_watch_ids.insert(watch_id),
            "WatchId {watch_id} already exists in active_watch_ids",
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
            self.kv_watcher.cancel(watch_id);
            let _prev = self.active_watch_ids.remove(&watch_id);
            let response = WatchResponse {
                header: Some(self.header_gen.gen_header()),
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
                RequestUnion::ProgressRequest(req) => {
                    self.handle_watch_progress(req).await;
                }
            }
        }
    }

    /// Handle watch event
    async fn handle_watch_event(&mut self, mut watch_event: WatchEvent) {
        let watch_id = watch_event.watch_id();
        let mut response = WatchResponse {
            header: Some(ResponseHeader {
                revision: watch_event.revision(),
                ..ResponseHeader::default()
            }),
            watch_id,
            ..WatchResponse::default()
        };
        if watch_event.compacted() {
            response.compact_revision = self.kv_watcher.compacted_revision();
            response.canceled = true;
        } else {
            let mut events = watch_event.take_events();
            if events.is_empty() {
                return;
            }

            if self.prev_kv.contains(&watch_id) {
                for ev in &mut events {
                    if !ev.is_create() {
                        let kv = ev
                            .kv
                            .as_ref()
                            .unwrap_or_else(|| panic!("event.kv can't be None"));
                        ev.prev_kv = self.kv_watcher.get_prev_kv(kv);
                    }
                }
            }
            response.events = events;
        };

        if self.response_tx.send(Ok(response)).await.is_err() {
            self.stop_notify.notify(1);
        }
        if let Some(progress) = self.progress.get_mut(&watch_id) {
            *progress = false;
        }
    }

    /// Handle progress for request
    async fn handle_watch_progress(&mut self, _req: WatchProgressRequest) {
        if self
            .response_tx
            .send(Ok(WatchResponse {
                header: Some(self.header_gen.gen_header()),
                watch_id: -1,
                ..Default::default()
            }))
            .await
            .is_err()
        {
            self.stop_notify.notify(1);
        }
    }

    /// Handle progress from tick
    async fn handle_tick_progress(&mut self) {
        for (watch_id, progress) in &mut self.progress {
            if *progress {
                if self
                    .response_tx
                    .send(Ok(WatchResponse {
                        header: Some(self.header_gen.gen_header()),
                        watch_id: *watch_id,
                        ..Default::default()
                    }))
                    .await
                    .is_err()
                {
                    self.stop_notify.notify(1);
                }
            } else {
                *progress = true;
            }
        }
    }
}

impl<W> Drop for WatchHandle<W>
where
    W: KvWatcherOps,
{
    fn drop(&mut self) {
        for watch_id in &self.active_watch_ids {
            self.kv_watcher.cancel(*watch_id);
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
        self.task_manager.spawn(TaskName::WatchTask, |n| {
            Self::task(
                Arc::clone(&self.next_id_gen),
                Arc::clone(&self.watcher),
                tx,
                req_stream,
                Arc::clone(&self.header_gen),
                self.watch_progress_notify_interval,
                n,
            )
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicI32, Ordering},
        time::Duration,
    };

    use parking_lot::Mutex;
    use test_macros::abort_on_panic;
    use tokio::{
        sync::mpsc,
        time::{sleep, timeout},
    };
    use utils::config::{default_watch_progress_notify_interval, EngineConfig};
    use xlineapi::RequestWrapper;

    use super::*;
    use crate::{
        rpc::{PutRequest, WatchProgressRequest},
        storage::{
            compact::COMPACT_CHANNEL_SIZE, db::DB, index::Index, kv_store::KvStoreInner,
            kvwatcher::MockKvWatcherOps, lease_store::LeaseCollection, KvStore,
        },
    };

    fn is_progress_notify(wr: &WatchResponse) -> bool {
        wr.events.is_empty()
            && !wr.canceled
            && !wr.created
            && wr.compact_revision == 0
            && wr.header.as_ref().map_or(false, |h| h.revision != 0)
    }

    async fn put(
        store: &KvStore<DB>,
        db: &DB,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        revision: i64,
    ) {
        let req = RequestWrapper::from(PutRequest {
            key: key.into(),
            value: value.into(),
            ..Default::default()
        });
        let (_sync_res, ops) = store.after_sync(&req, revision).await.unwrap();
        let key_revisions = db.flush_ops(ops).unwrap();
        store.insert_index(key_revisions);
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_watch_client_closes_connection() -> Result<(), Box<dyn std::error::Error>> {
        let task_manager = Arc::new(TaskManager::new());
        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx, mut res_rx) = mpsc::channel(CHANNEL_SIZE);
        let req_stream: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx);
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let mut mock_watcher = MockKvWatcherOps::new();
        let _ = mock_watcher.expect_watch().times(1).return_const(());
        let _ = mock_watcher.expect_cancel().times(1).return_const(());
        let _ = mock_watcher
            .expect_compacted_revision()
            .return_const(-1_i64);
        let watcher = Arc::new(mock_watcher);
        let next_id = Arc::new(WatchIdGenerator::new(1));
        let n = task_manager.get_shutdown_listener(TaskName::WatchTask);
        let handle = tokio::spawn(WatchServer::<DB>::task(
            next_id,
            Arc::clone(&watcher),
            res_tx,
            req_stream,
            header_gen,
            default_watch_progress_notify_interval(),
            n,
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
        timeout(Duration::from_secs(3), handle).await??;
        task_manager.shutdown(true).await;
        Ok(())
    }

    #[tokio::test]
    #[abort_on_panic]
    #[allow(clippy::similar_names)] // use num as suffix
    async fn test_multi_watch_handle() -> Result<(), Box<dyn std::error::Error>> {
        let task_manager = Arc::new(TaskManager::new());
        let mut mock_watcher = MockKvWatcherOps::new();
        let collection = Arc::new(Mutex::new(HashMap::new()));
        let collection_c = Arc::clone(&collection);
        let _ = mock_watcher.expect_watch().times(2).returning({
            move |x, _, _, _, _, _| {
                let mut c = collection_c.lock();
                let e = c.entry(x).or_insert(0);
                *e += 1;
            }
        });
        let _ = mock_watcher.expect_cancel().return_const(());
        let _ = mock_watcher
            .expect_compacted_revision()
            .return_const(-1_i64);
        let kv_watcher = Arc::new(mock_watcher);
        let next_id_gen = Arc::new(WatchIdGenerator::new(1));
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));

        let (req_tx1, req_rx1) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx1, _res_rx1) = mpsc::channel(CHANNEL_SIZE);
        let req_stream1: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx1);
        task_manager.spawn(TaskName::WatchTask, |n| {
            WatchServer::<DB>::task(
                Arc::clone(&next_id_gen),
                Arc::clone(&kv_watcher),
                res_tx1,
                req_stream1,
                Arc::clone(&header_gen),
                default_watch_progress_notify_interval(),
                n,
            )
        });

        let (req_tx2, req_rx2) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx2, _res_rx2) = mpsc::channel(CHANNEL_SIZE);
        let req_stream2: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx2);
        task_manager.spawn(TaskName::WatchTask, |n| {
            WatchServer::<DB>::task(
                next_id_gen,
                kv_watcher,
                res_tx2,
                req_stream2,
                header_gen,
                default_watch_progress_notify_interval(),
                n,
            )
        });

        let w_req = WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                key: vec![0],
                range_end: vec![0],
                ..Default::default()
            })),
        };
        req_tx1.send(Ok(w_req.clone())).await?;
        req_tx2.send(Ok(w_req.clone())).await?;
        sleep(Duration::from_secs(1)).await;
        for count in collection.lock().values() {
            assert_eq!(*count, 1);
        }
        task_manager.shutdown(true).await;
        Ok(())
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_watch_prev_kv() {
        let task_manager = Arc::new(TaskManager::new());
        let (compact_tx, _compact_rx) = mpsc::channel(COMPACT_CHANNEL_SIZE);
        let index = Arc::new(Index::new());
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let next_id_gen = Arc::new(WatchIdGenerator::new(1));
        let (kv_update_tx, kv_update_rx) = mpsc::channel(CHANNEL_SIZE);
        let kv_store_inner = Arc::new(KvStoreInner::new(index, Arc::clone(&db)));
        let kv_store = Arc::new(KvStore::new(
            Arc::clone(&kv_store_inner),
            Arc::clone(&header_gen),
            kv_update_tx,
            compact_tx,
            lease_collection,
        ));
        let kv_watcher = KvWatcher::new_arc(
            kv_store_inner,
            kv_update_rx,
            Duration::from_millis(10),
            &task_manager,
        );
        put(&kv_store, &db, "foo", "old_bar", 2).await;
        put(&kv_store, &db, "foo", "bar", 3).await;

        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        let req_stream = ReceiverStream::new(req_rx);
        let create_watch_req = move |watch_id: WatchId, prev_kv: bool| WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                watch_id,
                key: "foo".into(),
                start_revision: 3,
                prev_kv,
                ..Default::default()
            })),
        };
        req_tx.send(Ok(create_watch_req(1, false))).await.unwrap();
        req_tx.send(Ok(create_watch_req(2, true))).await.unwrap();
        let (res_tx, mut res_rx) = mpsc::channel(CHANNEL_SIZE);
        task_manager.spawn(TaskName::WatchTask, |n| {
            WatchServer::<DB>::task(
                Arc::clone(&next_id_gen),
                Arc::clone(&kv_watcher),
                res_tx,
                req_stream,
                Arc::clone(&header_gen),
                default_watch_progress_notify_interval(),
                n,
            )
        });

        for _ in 0..4 {
            let watch_res = res_rx.recv().await.unwrap().unwrap();
            if watch_res.events.is_empty() {
                // WatchCreateResponse
                continue;
            }
            let watch_id = watch_res.watch_id;
            let has_prev = watch_res.events.first().unwrap().prev_kv.is_some();
            if watch_id == 1 {
                assert!(!has_prev);
            } else {
                assert!(has_prev);
            }
        }
        drop(kv_store);
        task_manager.shutdown(true).await;
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_watch_progress() -> Result<(), Box<dyn std::error::Error>> {
        let task_manager = Arc::new(TaskManager::new());
        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx, mut res_rx) = mpsc::channel(CHANNEL_SIZE);
        let req_stream: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx);
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let mut mock_watcher = MockKvWatcherOps::new();
        let _ = mock_watcher.expect_watch().times(1).return_const(());
        let _ = mock_watcher.expect_cancel().times(1).return_const(());
        let _ = mock_watcher
            .expect_compacted_revision()
            .return_const(-1_i64);
        let watcher = Arc::new(mock_watcher);
        let next_id = Arc::new(WatchIdGenerator::new(1));
        task_manager.spawn(TaskName::WatchTask, |n| {
            WatchServer::<DB>::task(
                next_id,
                Arc::clone(&watcher),
                res_tx,
                req_stream,
                header_gen,
                Duration::from_millis(100),
                n,
            )
        });
        req_tx
            .send(Ok(WatchRequest {
                request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                    key: "foo".into(),
                    progress_notify: true,
                    watch_id: 1,
                    ..Default::default()
                })),
            }))
            .await?;
        req_tx
            .send(Ok(WatchRequest {
                request_union: Some(RequestUnion::ProgressRequest(WatchProgressRequest {})),
            }))
            .await?;
        let cnt = Arc::new(AtomicI32::new(0));

        let _ignore = timeout(Duration::from_secs(1), {
            let cnt = Arc::clone(&cnt);
            async move {
                while let Some(Ok(res)) = res_rx.recv().await {
                    if is_progress_notify(&res) {
                        cnt.fetch_add(1, Ordering::Release);
                    }
                }
            }
        })
        .await;
        let c = cnt.load(Ordering::Acquire);
        assert!(c >= 9);
        drop(req_tx);
        task_manager.shutdown(true).await;
        Ok(())
    }

    #[tokio::test]
    async fn watch_task_should_terminate_when_response_tx_closed(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let task_manager = Arc::new(TaskManager::new());
        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        let (res_tx, res_rx) = mpsc::channel(CHANNEL_SIZE);
        let req_stream: ReceiverStream<Result<WatchRequest, tonic::Status>> =
            ReceiverStream::new(req_rx);
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let mut mock_watcher = MockKvWatcherOps::new();
        let _ = mock_watcher.expect_watch().times(1).return_const(());
        let _ = mock_watcher.expect_cancel().times(1).return_const(());
        let _ = mock_watcher
            .expect_compacted_revision()
            .return_const(-1_i64);
        let watcher = Arc::new(mock_watcher);
        let next_id = Arc::new(WatchIdGenerator::new(1));
        let n = task_manager.get_shutdown_listener(TaskName::WatchTask);
        let handle = tokio::spawn(WatchServer::<DB>::task(
            next_id,
            Arc::clone(&watcher),
            res_tx,
            req_stream,
            header_gen,
            Duration::from_millis(100),
            n,
        ));

        req_tx
            .send(Ok(WatchRequest {
                request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                    key: "foo".into(),
                    progress_notify: true,
                    watch_id: 1,
                    ..Default::default()
                })),
            }))
            .await?;

        drop(res_rx);

        req_tx
            .send(Ok(WatchRequest {
                request_union: Some(RequestUnion::ProgressRequest(WatchProgressRequest {})),
            }))
            .await?;

        assert!(timeout(Duration::from_secs(10), handle).await.is_ok());
        task_manager.shutdown(true).await;
        Ok(())
    }

    #[tokio::test]
    async fn watch_compacted_revision_should_fail() {
        let task_manager = Arc::new(TaskManager::new());
        let (compact_tx, _compact_rx) = mpsc::channel(COMPACT_CHANNEL_SIZE);
        let index = Arc::new(Index::new());
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let next_id_gen = Arc::new(WatchIdGenerator::new(1));
        let (kv_update_tx, kv_update_rx) = mpsc::channel(CHANNEL_SIZE);
        let kv_store_inner = Arc::new(KvStoreInner::new(index, Arc::clone(&db)));
        let kv_store = Arc::new(KvStore::new(
            Arc::clone(&kv_store_inner),
            Arc::clone(&header_gen),
            kv_update_tx,
            compact_tx,
            lease_collection,
        ));
        let kv_watcher = KvWatcher::new_arc(
            kv_store_inner,
            kv_update_rx,
            Duration::from_millis(10),
            &task_manager,
        );
        put(&kv_store, &db, "foo", "old_bar", 2).await;
        put(&kv_store, &db, "foo", "bar", 3).await;
        put(&kv_store, &db, "foo", "new_bar", 4).await;

        kv_store.update_compacted_revision(3);

        let (req_tx, req_rx) = mpsc::channel(CHANNEL_SIZE);
        let req_stream = ReceiverStream::new(req_rx);
        let create_watch_req = move |watch_id: WatchId, start_rev: i64| WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                watch_id,
                key: "foo".into(),
                start_revision: start_rev,
                ..Default::default()
            })),
        };
        req_tx.send(Ok(create_watch_req(1, 2))).await.unwrap();
        let (res_tx, mut res_rx) = mpsc::channel(CHANNEL_SIZE);
        task_manager.spawn(TaskName::WatchTask, |n| {
            WatchServer::<DB>::task(
                Arc::clone(&next_id_gen),
                Arc::clone(&kv_watcher),
                res_tx,
                req_stream,
                Arc::clone(&header_gen),
                default_watch_progress_notify_interval(),
                n,
            )
        });

        // It's allowed to create a compacted watch request, but it will immediately cancel. Doing so is for the compatibility with etcdctl
        let watch_create_success_res = res_rx.recv().await.unwrap().unwrap();
        assert!(watch_create_success_res.created);
        assert_eq!(watch_create_success_res.watch_id, 1);
        let watch_cancel_res = res_rx.recv().await.unwrap().unwrap();
        assert!(watch_cancel_res.canceled);
        assert_eq!(watch_cancel_res.compact_revision, 3);

        req_tx.send(Ok(create_watch_req(2, 3))).await.unwrap();
        let watch_create_success_res = res_rx.recv().await.unwrap().unwrap();
        assert!(watch_create_success_res.created);
        assert_eq!(watch_create_success_res.compact_revision, 0);
        assert_eq!(watch_create_success_res.watch_id, 2);

        let watch_event_res = res_rx.recv().await.unwrap().unwrap();
        assert!(!watch_event_res.created);
        assert!(!watch_event_res.canceled);
        assert_eq!(watch_event_res.compact_revision, 0);
        assert_eq!(watch_event_res.watch_id, 2);
        drop(kv_store);
        task_manager.shutdown(true).await;
    }
}
