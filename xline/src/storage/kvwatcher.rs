use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use futures::{stream::FuturesUnordered, StreamExt};
use log::warn;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use utils::parking_lot_lock::RwLockMap;

use super::storage_api::StorageApi;
use crate::{rpc::Event, server::command::KeyRange, storage::kv_store::KvStoreBackend};

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
    /// Sender of watch event
    event_tx: mpsc::Sender<WatchEvent>,
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
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> Self {
        Self {
            key_range,
            watch_id,
            start_rev,
            filters,
            event_tx,
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

    /// Get start revision
    fn start_rev(&self) -> i64 {
        self.start_rev
    }

    /// Notify events
    async fn notify(&self, (revision, mut events): (i64, Vec<Event>)) {
        if revision < self.start_rev() {
            return;
        }
        events.retain(|event| self.filters.iter().all(|filter| filter != &event.r#type));
        let watch_event = WatchEvent {
            id: self.watch_id(),
            events,
            revision,
        };
        assert!(
            self.event_tx.send(watch_event).await.is_ok(),
            "WatchEvent receiver is closed"
        );
    }
}

/// KV watcher
#[derive(Debug)]
pub(crate) struct KvWatcher<S>
where
    S: StorageApi,
{
    /// Inner data
    inner: Arc<KvWatcherInner<S>>,
}

/// KV watcher inner data
#[derive(Debug)]
struct KvWatcherInner<S>
where
    S: StorageApi,
{
    /// KV storage
    storage: Arc<KvStoreBackend<S>>,
    /// Watch indexes
    watcher_map: RwLock<WatcherMap>,
}

/// Store all watchers
#[derive(Debug)]
struct WatcherMap {
    /// All watchers
    watchers: HashMap<WatchId, Arc<Watcher>>,
    /// Index for watchers
    index: HashMap<KeyRange, HashSet<Arc<Watcher>>>,
}

impl WatcherMap {
    /// Create a new `WatcherMap`
    fn new() -> Self {
        Self {
            watchers: HashMap::new(),
            index: HashMap::new(),
        }
    }

    /// Insert a new watcher to the map and create. Internally, it will create a index for this watcher.
    fn insert(&mut self, watcher: Arc<Watcher>) {
        let key_range = watcher.key_range().clone();
        let watch_id = watcher.watch_id();
        assert!(
            self.watchers
                .insert(watch_id, Arc::clone(&watcher))
                .is_none(),
            "can't insert a watcher twice"
        );
        assert!(
            self.index
                .entry(key_range)
                .or_insert_with(HashSet::new)
                .insert(watcher),
            "can't insert a watcher twice"
        );
    }

    /// Remove a watcher
    #[allow(clippy::expect_used)] // the logic is managed internally
    fn remove(&mut self, watch_id: WatchId) {
        let watcher = self.watchers.remove(&watch_id).expect("no such watcher");
        let key_range = watcher.key_range();
        let is_empty = {
            let watchers = self
                .index
                .get_mut(key_range)
                .expect("no such watcher in index");
            assert!(watchers.remove(&watcher), "no such watcher in index");
            watchers.is_empty()
        };
        if is_empty {
            assert!(self.index.remove(key_range).is_some());
        }
    }
}

impl<S> KvWatcher<S>
where
    S: StorageApi,
{
    /// New `KvWatcher`
    pub(super) fn new(
        storage: Arc<KvStoreBackend<S>>,
        mut kv_update_rx: mpsc::Receiver<(i64, Vec<Event>)>,
    ) -> Self {
        let inner = Arc::new(KvWatcherInner::new(storage));
        let inner_clone = Arc::clone(&inner);
        let _handle = tokio::spawn(async move {
            while let Some(updates) = kv_update_rx.recv().await {
                inner_clone.handle_kv_updates(updates).await;
            }
        });
        Self { inner }
    }
}

/// Operations of KV watcher
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // Introduced by mockall::automock
#[cfg_attr(test, mockall::automock)]
pub(crate) trait KvWatcherOps {
    /// Create a watch to KV store
    fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> (Vec<Event>, i64);

    /// Cancel a watch from KV store
    fn cancel(&self, id: WatchId) -> i64;
}

impl<S> KvWatcherOps for KvWatcher<S>
where
    S: StorageApi,
{
    /// Create a watch to KV store
    fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> (Vec<Event>, i64) {
        self.inner
            .watch(id, key_range, start_rev, filters, event_tx)
    }

    /// Cancel a watch from KV store
    fn cancel(&self, id: WatchId) -> i64 {
        self.inner.cancel(id)
    }
}

impl<S> KvWatcherInner<S>
where
    S: StorageApi,
{
    /// New `KvWatchInner`
    fn new(storage: Arc<KvStoreBackend<S>>) -> Self {
        Self {
            storage,
            watcher_map: RwLock::new(WatcherMap::new()),
        }
    }

    /// Create a watch to KV store
    fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> (Vec<Event>, i64) {
        let watcher = Watcher::new(key_range.clone(), id, start_rev, filters, event_tx);

        let revision = self.storage.revision();
        // TODO: handle racing that new event is generated before watcher is registered
        let initial_events = if start_rev == 0 {
            vec![]
        } else {
            self.storage
                .get_event_from_revision(key_range, start_rev)
                .unwrap_or_else(|e| {
                    warn!("failed to get initial events for watcher: {:?}", e);
                    vec![]
                })
        };
        // sync a new command
        self.watcher_map.write().insert(Arc::new(watcher));

        (initial_events, revision)
    }

    /// Cancel a watch from KV store
    fn cancel(&self, watch_id: WatchId) -> i64 {
        let revision = self.storage.revision();
        self.watcher_map.write().remove(watch_id);
        revision
    }

    /// Handle KV store updates
    async fn handle_kv_updates(&self, (revision, all_events): (i64, Vec<Event>)) {
        let watcher_events = self.watcher_map.map_read(|watcher_map_r| {
            let mut watcher_events: HashMap<Arc<Watcher>, Vec<Event>> = HashMap::new();
            for event in all_events {
                // get related watchers
                let watchers: HashSet<Arc<Watcher>> = watcher_map_r
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
                        .then(|| v.clone())
                    })
                    .flatten()
                    .collect();
                for watcher in watchers {
                    #[allow(clippy::indexing_slicing)]
                    watcher_events
                        .entry(watcher)
                        .or_default()
                        .push(event.clone());
                }
            }
            watcher_events
        });

        let _ig = watcher_events
            .into_iter()
            .map(|(watcher, events)| async move { watcher.notify((revision, events)).await })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;
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
}
