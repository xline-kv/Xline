use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use clippy_utilities::OverflowArithmetic;
use itertools::Itertools;
use log::warn;
use parking_lot::RwLock;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    time::sleep,
};
use tracing::debug;
use utils::{parking_lot_lock::RwLockMap, shutdown};
use xlineapi::command::KeyRange;

use super::{kv_store::KvStoreInner, storage_api::StorageApi};
use crate::rpc::{Event, KeyValue};

/// Watch ID
pub(crate) type WatchId = i64;

/// Watch ID generator
#[derive(Debug)]
pub(crate) struct WatchIdGenerator(AtomicI64);

impl WatchIdGenerator {
    /// Create a new `WatchIdGenerator`
    pub(crate) fn new(rev: i64) -> Self {
        Self(AtomicI64::new(rev))
    }

    /// Get the next revision number
    pub(crate) fn next(&self) -> i64 {
        self.0.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }
}

/// Watcher
#[derive(Debug)]
struct Watcher {
    /// Key Range
    key_range: KeyRange,
    /// Watch ID
    watch_id: WatchId,
    /// Start revision of this watcher
    start_rev: i64,
    /// Event filters
    filters: Vec<i32>,
    /// Stop notify
    stop_notify: Arc<event_listener::Event>,
    /// Sender of watch event
    event_tx: mpsc::Sender<WatchEvent>,
    /// Compacted flag
    compacted: bool,
}

impl PartialEq for Watcher {
    fn eq(&self, other: &Self) -> bool {
        self.watch_id == other.watch_id
    }
}

impl Eq for Watcher {}

impl Hash for Watcher {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.watch_id.hash(state);
    }
}

impl Watcher {
    /// New `WatcherInner`
    fn new(
        key_range: KeyRange,
        watch_id: WatchId,
        start_rev: i64,
        filters: Vec<i32>,
        stop_notify: Arc<event_listener::Event>,
        event_tx: mpsc::Sender<WatchEvent>,
        compacted: bool,
    ) -> Self {
        Self {
            key_range,
            watch_id,
            start_rev,
            filters,
            stop_notify,
            event_tx,
            compacted,
        }
    }

    /// Get watch id
    fn watch_id(&self) -> i64 {
        self.watch_id
    }

    /// Get key range
    fn key_range(&self) -> &KeyRange {
        &self.key_range
    }

    /// filter out events
    fn filter_events(&self, mut events: Vec<Event>) -> Vec<Event> {
        events.retain(|event| {
            self.filters.iter().all(|filter| filter != &event.r#type)
                && (event
                    .kv
                    .as_ref()
                    .map_or(false, |kv| kv.mod_revision >= self.start_rev))
        });
        events
    }

    /// Notify all passed events, please filter out events before calling this method
    fn notify(
        &mut self,
        (revision, events): (i64, Vec<Event>),
    ) -> Result<(), TrySendError<WatchEvent>> {
        let watch_id = self.watch_id();
        let events = self.filter_events(events);
        let events_len = events.len();
        let watch_event = WatchEvent {
            id: watch_id,
            events,
            revision,
            compacted: self.compacted,
        };
        if !self.compacted {
            if revision < self.start_rev || 0 == events_len {
                return Ok(());
            }
            debug!(watch_id, revision, events_len, "try to send watch response");
        };

        match self.event_tx.try_send(watch_event) {
            Ok(_) => {
                debug!(watch_id, revision, "response sent successfully");
                self.start_rev = revision.overflow_add(1);
                Ok(())
            }
            Err(TrySendError::Closed(_)) => {
                debug!(watch_id, revision, "watcher is closed");
                self.stop_notify.notify(1);
                Ok(())
            }
            Err(TrySendError::Full(watch_event)) => {
                debug!(
                    watch_id,
                    revision, "events channel is full, will try to send later"
                );
                Err(TrySendError::Full(watch_event))
            }
        }
    }
}

/// KV watcher
#[derive(Debug)]
pub(crate) struct KvWatcher<S>
where
    S: StorageApi,
{
    /// KV storage Inner
    kv_store_inner: Arc<KvStoreInner<S>>,
    /// Watch indexes
    watcher_map: Arc<RwLock<WatcherMap>>,
}

/// Store all watchers
#[derive(Debug)]
struct WatcherMap {
    /// Index for watchers
    index: HashMap<KeyRange, HashSet<WatchId>>,
    /// All watchers
    watchers: HashMap<WatchId, Watcher>,
    /// Victims
    victims: HashMap<Watcher, (i64, Vec<Event>)>,
}

impl WatcherMap {
    /// Create a new `WatcherMap`
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            watchers: HashMap::new(),
            victims: HashMap::new(),
        }
    }

    /// Insert a new watcher to the map and create. Internally, it will create a index for this watcher.
    fn register(&mut self, watcher: Watcher) {
        let key_range = watcher.key_range().clone();
        let watch_id = watcher.watch_id();
        assert!(
            self.watchers.insert(watch_id, watcher).is_none(),
            "can't insert a watcher to watchers twice"
        );
        assert!(
            self.index
                .entry(key_range)
                .or_insert_with(HashSet::new)
                .insert(watch_id),
            "can't insert a watcher to index twice"
        );
    }

    /// Move a watcher to victims, the `watch_id` must be valid.
    fn move_to_victim(&mut self, watch_id: WatchId, updates: (i64, Vec<Event>)) {
        debug!(watch_id, "move watcher to victim");
        let Some(watcher) = self.watchers.remove(&watch_id) else {
            unreachable!("watcher should exist")
        };
        let Some(watch_ids) = self.index.get_mut(watcher.key_range()) else {
            unreachable!("watch_ids should exist")
        };
        assert!(
            watch_ids.remove(&watcher.watch_id()),
            "no such watcher in index"
        );
        if watch_ids.is_empty() {
            assert!(
                self.index.remove(&watcher.key_range).is_some(),
                "watch_ids should exist"
            );
        }
        let watch_event = WatchEvent {
            id: watch_id,
            revision: updates.0,
            events: updates.1,
            compacted: false,
        };
        assert!(
            self.victims
                .insert(watcher, (watch_event.revision, watch_event.events))
                .is_none(),
            "can't insert a watcher to victims twice"
        );
    }

    /// Remove a watcher
    fn remove(&mut self, watch_id: WatchId) {
        if let Some(watcher) = self.watchers.remove(&watch_id) {
            let key_range = watcher.key_range();
            let is_empty = {
                let Some(watch_ids) = self.index.get_mut(watcher.key_range()) else {
                    unreachable!("watch_ids should exist")
                };
                assert!(
                    watch_ids.remove(&watcher.watch_id()),
                    "no such watcher in index"
                );
                watch_ids.is_empty()
            };
            if is_empty {
                assert!(
                    self.index.remove(key_range).is_some(),
                    "watch_ids should exist"
                );
            }
        } else {
            self.victims = self
                .victims
                .drain()
                .filter(|pair| pair.0.watch_id() != watch_id)
                .collect();
        };
    }
}

/// Operations of KV watcher
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // Introduced by mockall::automock
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub(crate) trait KvWatcherOps {
    /// Create a watch to KV store
    fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        stop_notify: Arc<event_listener::Event>,
        event_tx: mpsc::Sender<WatchEvent>,
    );

    /// Cancel a watch from KV store
    fn cancel(&self, id: WatchId);

    /// Get Prev `KeyValue` of a `KeyValue`
    fn get_prev_kv(&self, kv: &KeyValue) -> Option<KeyValue>;

    /// Get compacted revision from backend store
    fn compacted_revision(&self) -> i64;
}

#[async_trait::async_trait]
impl<S> KvWatcherOps for KvWatcher<S>
where
    S: StorageApi,
{
    fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        stop_notify: Arc<event_listener::Event>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) {
        let compacted = start_rev != 0 && start_rev < self.compacted_revision();
        let mut watcher = Watcher::new(
            key_range.clone(),
            id,
            start_rev,
            filters,
            stop_notify,
            event_tx,
            compacted,
        );
        let mut watcher_map_w = self.watcher_map.write();
        if compacted {
            debug!("The revision {watcher:?} required has been compacted");
            if let Err(TrySendError::Full(watch_event)) = watcher.notify((0, vec![])) {
                assert!(
                    watcher_map_w
                        .victims
                        .insert(watcher, (watch_event.revision, watch_event.events))
                        .is_none(),
                    "can't insert a watcher to victims twice"
                );
            };
            return;
        }

        let initial_events = if start_rev == 0 {
            vec![]
        } else {
            self.kv_store_inner
                .get_event_from_revision(key_range, start_rev)
                .unwrap_or_else(|e| {
                    warn!("failed to get initial events for watcher: {:?}", e);
                    vec![]
                })
        };
        if !initial_events.is_empty() {
            let last_revision = get_last_revision(&initial_events);
            if let Err(TrySendError::Full(watch_event)) =
                watcher.notify((last_revision, initial_events))
            {
                assert!(
                    watcher_map_w
                        .victims
                        .insert(watcher, (watch_event.revision, watch_event.events))
                        .is_none(),
                    "can't insert a watcher to victims twice"
                );
                return;
            };
        }
        debug!("register watcher: {:?}", watcher);
        watcher_map_w.register(watcher);
    }

    fn cancel(&self, watch_id: WatchId) {
        self.watcher_map.write().remove(watch_id);
    }

    fn get_prev_kv(&self, kv: &KeyValue) -> Option<KeyValue> {
        self.kv_store_inner.get_prev_kv(kv)
    }

    fn compacted_revision(&self) -> i64 {
        self.kv_store_inner.compacted_revision()
    }
}

impl<S> KvWatcher<S>
where
    S: StorageApi,
{
    /// Create a new `Arc<KvWatcher>`
    pub(crate) fn new_arc(
        kv_store_inner: Arc<KvStoreInner<S>>,
        kv_update_rx: mpsc::Receiver<(i64, Vec<Event>)>,
        sync_victims_interval: Duration,
        shutdown_listener: shutdown::Listener,
    ) -> Arc<Self> {
        let watcher_map = Arc::new(RwLock::new(WatcherMap::new()));
        let kv_watcher = Arc::new(Self {
            kv_store_inner,
            watcher_map,
        });
        let _sync_victims_task = tokio::spawn(Self::sync_victims_task(
            Arc::clone(&kv_watcher),
            sync_victims_interval,
            shutdown_listener.clone(),
        ));
        let _kv_updates_task = tokio::spawn(Self::kv_updates_task(
            Arc::clone(&kv_watcher),
            kv_update_rx,
            shutdown_listener,
        ));
        kv_watcher
    }

    /// Background task to handle KV updates
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    async fn kv_updates_task(
        kv_watcher: Arc<KvWatcher<S>>,
        mut kv_update_rx: mpsc::Receiver<(i64, Vec<Event>)>,
        // This task will safely exit when the log_tx is dropped, but we still
        // need to keep the shutdown_listener here to notify the shutdown trigger
        _shutdown_listener: shutdown::Listener,
    ) {
        while let Some(updates) = kv_update_rx.recv().await {
            kv_watcher.handle_kv_updates(updates);
        }
        debug!("kv_update_rx is closed");
    }

    /// Background task to sync victims
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    async fn sync_victims_task(
        kv_watcher: Arc<KvWatcher<S>>,
        sync_victims_interval: Duration,
        mut shutdown_listener: shutdown::Listener,
    ) {
        loop {
            let victims = kv_watcher
                .watcher_map
                .map_write(|mut m| m.victims.drain().collect::<Vec<_>>());
            let mut new_victims = HashMap::new();
            for (mut watcher, res) in victims {
                // needn't to filter updates and get prev_kv, because the watcher is already filtered before inserted into victims
                if let Err(TrySendError::Full(watch_event)) = watcher.notify(res) {
                    assert!(
                        new_victims
                            .insert(watcher, (watch_event.revision, watch_event.events))
                            .is_none(),
                        "can't insert a watcher to new_victims twice"
                    );
                } else {
                    let mut watcher_map_w = kv_watcher.watcher_map.write();
                    let initial_events = kv_watcher
                        .kv_store_inner
                        .get_event_from_revision(watcher.key_range.clone(), watcher.start_rev)
                        .unwrap_or_else(|e| {
                            warn!("failed to get initial events for watcher: {:?}", e);
                            vec![]
                        });
                    if !initial_events.is_empty() {
                        let last_revision = get_last_revision(&initial_events);
                        if let Err(TrySendError::Full(watch_event)) =
                            watcher.notify((last_revision, initial_events))
                        {
                            assert!(
                                new_victims
                                    .insert(watcher, (watch_event.revision, watch_event.events))
                                    .is_none(),
                                "can't insert a watcher to new_victims twice"
                            );
                            break;
                        };
                    }
                    debug!(
                        watch_id = watcher.watch_id(),
                        "watcher synced by sync_victims_task"
                    );
                    if !watcher.compacted {
                        watcher_map_w.register(watcher);
                    }
                }
            }
            if !new_victims.is_empty() {
                kv_watcher.watcher_map.write().victims.extend(new_victims);
            }
            tokio::select! {
                _ = shutdown_listener.wait_self_shutdown() => {
                    debug!("victims sync task is shutdown");
                    break;
                }
                _ = sleep(sync_victims_interval) => {}
            }
        }
    }

    /// Handle KV store updates
    fn handle_kv_updates(&self, (revision, all_events): (i64, Vec<Event>)) {
        self.watcher_map.map_write(|mut watcher_map_w| {
            let mut watcher_events: HashMap<WatchId, Vec<Event>> = HashMap::new();
            for event in all_events {
                let watch_ids = watcher_map_w
                    .index
                    .iter()
                    .filter_map(|(k, v)| {
                        k.contains_key(
                            &event
                                .kv
                                .as_ref()
                                .unwrap_or_else(|| panic!("Receive Event with empty kv"))
                                .key,
                        )
                        .then_some(v)
                    })
                    .flatten()
                    .copied()
                    .collect_vec();
                for watch_id in watch_ids {
                    watcher_events
                        .entry(watch_id)
                        .or_default()
                        .push(event.clone());
                }
            }
            for (watch_id, events) in watcher_events {
                let watcher = watcher_map_w
                    .watchers
                    .get_mut(&watch_id)
                    .unwrap_or_else(|| panic!("watcher index and watchers doesn't match"));
                if let Err(TrySendError::Full(watch_event)) = watcher.notify((revision, events)) {
                    watcher_map_w
                        .move_to_victim(watch_id, (watch_event.revision, watch_event.events));
                }
            }
        });
    }
}

/// Watch Event
#[derive(Debug)]
pub(crate) struct WatchEvent {
    /// Watch ID
    id: WatchId,
    /// Events to be sent
    events: Vec<Event>,
    /// Revision when this event is generated
    revision: i64,
    /// Compacted WatchEvent
    compacted: bool,
}

impl WatchEvent {
    /// Get revision
    pub(crate) fn revision(&self) -> i64 {
        self.revision
    }

    /// Get `WatchId`
    pub(crate) fn watch_id(&self) -> WatchId {
        self.id
    }

    /// Take events
    pub(crate) fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    /// Check whether the `WatchEvent` is a compacted `WatchEvent` or not.
    pub(crate) fn compacted(&self) -> bool {
        self.compacted
    }
}

/// Get the last revision of a event slice
fn get_last_revision(events: &[Event]) -> i64 {
    events
        .last()
        .unwrap_or_else(|| unreachable!("events is not empty"))
        .kv
        .as_ref()
        .unwrap_or_else(|| panic!("event.kv can't be None"))
        .mod_revision
}

#[cfg(test)]
mod test {

    use std::{collections::BTreeMap, time::Duration};

    use clippy_utilities::Cast;
    use test_macros::abort_on_panic;
    use tokio::time::{sleep, timeout};
    use utils::config::EngineConfig;

    use super::*;
    use crate::{
        header_gen::HeaderGenerator,
        rpc::{PutRequest, RequestWithToken},
        storage::{
            compact::COMPACT_CHANNEL_SIZE, db::DB, index::Index, lease_store::LeaseCollection,
            KvStore,
        },
    };

    fn init_empty_store(rx: shutdown::Listener) -> (Arc<KvStore<DB>>, Arc<DB>, Arc<KvWatcher<DB>>) {
        let (compact_tx, _compact_rx) = mpsc::channel(COMPACT_CHANNEL_SIZE);
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let index = Arc::new(Index::new());
        let lease_collection = Arc::new(LeaseCollection::new(0));
        let (kv_update_tx, kv_update_rx) = mpsc::channel(128);
        let kv_store_inner = Arc::new(KvStoreInner::new(index, Arc::clone(&db)));
        let store = Arc::new(KvStore::new(
            Arc::clone(&kv_store_inner),
            header_gen,
            kv_update_tx,
            compact_tx,
            lease_collection,
        ));
        let sync_victims_interval = Duration::from_millis(10);
        let kv_watcher =
            KvWatcher::new_arc(kv_store_inner, kv_update_rx, sync_victims_interval, rx);
        (store, db, kv_watcher)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn watch_should_not_lost_events() {
        let (tx, rx) = shutdown::channel();
        let (store, db, kv_watcher) = init_empty_store(rx);
        let mut map = BTreeMap::new();
        let (event_tx, mut event_rx) = mpsc::channel(128);
        let stop_notify = Arc::new(event_listener::Event::new());
        kv_watcher.watch(
            123,
            KeyRange::new_one_key("foo"),
            10,
            vec![],
            stop_notify,
            event_tx,
        );
        sleep(Duration::from_micros(50)).await;
        let handle = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                for i in 0..100_u8 {
                    put(
                        store.as_ref(),
                        db.as_ref(),
                        "foo",
                        vec![i],
                        i.overflow_add(2).cast(),
                    )
                    .await;
                }
            }
        });

        'outer: while let Some(event_batch) = timeout(Duration::from_secs(3), event_rx.recv())
            .await
            .unwrap()
        {
            for event in event_batch.events {
                let val = event.kv.as_ref().unwrap().value[0];
                debug!(val, "receive event");
                let e = map.entry(val).or_insert(0);
                *e += 1;
                if val == 99 {
                    break 'outer;
                }
            }
        }
        // The length of the map should be 1 + total_val_number - (start_rev - 1) = 1 + 100 - (10 - 1) = 92
        assert_eq!(map.len(), 92);
        for (k, count) in map {
            assert_eq!(count, 1, "key {k} should be notified once");
        }
        handle.abort();
        drop(store);
        tx.self_shutdown_and_wait().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_victim() {
        let (tx, rx) = shutdown::channel();
        let (store, db, kv_watcher) = init_empty_store(rx);
        // response channel with capacity 1, so it will be full easily, then we can trigger victim
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let stop_notify = Arc::new(event_listener::Event::new());

        kv_watcher.watch(
            123,
            KeyRange::new_one_key("foo"),
            0,
            vec![],
            stop_notify,
            event_tx,
        );

        let mut expect = 0;
        let handle = tokio::spawn(async move {
            'outer: while let Some(watch_events) = event_rx.recv().await {
                for event in watch_events.events {
                    let val = event.kv.as_ref().unwrap().value[0];
                    assert_eq!(val, expect);
                    expect += 1;
                    if val == 99 {
                        break 'outer;
                    }
                }
            }
        });

        for i in 0..100_u8 {
            put(store.as_ref(), db.as_ref(), "foo", vec![i], i.cast()).await;
        }
        handle.await.unwrap();
        drop(store);
        tx.self_shutdown_and_wait().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[abort_on_panic]
    async fn test_cancel_watcher() {
        let (tx, rx) = shutdown::channel();
        let (store, _db, kv_watcher) = init_empty_store(rx);
        let (event_tx, _event_rx) = mpsc::channel(1);
        let stop_notify = Arc::new(event_listener::Event::new());
        kv_watcher.watch(
            1,
            KeyRange::new_one_key("foo"),
            0,
            vec![],
            stop_notify,
            event_tx,
        );
        assert!(!kv_watcher.watcher_map.read().index.is_empty());
        assert!(!kv_watcher.watcher_map.read().watchers.is_empty());
        kv_watcher.cancel(1);
        assert!(kv_watcher.watcher_map.read().index.is_empty());
        assert!(kv_watcher.watcher_map.read().watchers.is_empty());
        drop(store);
        tx.self_shutdown_and_wait().await;
    }

    async fn put(
        store: &KvStore<DB>,
        db: &DB,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        revision: i64,
    ) {
        let req = RequestWithToken::new(
            PutRequest {
                key: key.into(),
                value: value.into(),
                ..Default::default()
            }
            .into(),
        );
        let (_sync_res, ops) = store.after_sync(&req, revision).await.unwrap();
        let key_revisions = db.flush_ops(ops).unwrap();
        store.insert_index(key_revisions);
    }
}
