use std::{
    cmp::Eq,
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::{rpc::Event, server::command::KeyRange, storage::kvstore::KvStoreBackend};

/// Watch ID
pub(crate) type WatchId = i64;

/// Watcher
#[derive(Debug)]
pub(crate) struct Watcher {
    /// Inner data
    inner: Arc<WatcherInner>,
}

/// Watcher inner data
#[derive(Debug)]
pub(crate) struct WatcherInner {
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

impl Hash for Watcher {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::<WatcherInner>::as_ptr(&self.inner).hash(state);
    }
}

impl PartialEq for Watcher {
    fn eq(&self, other: &Self) -> bool {
        Arc::<WatcherInner>::as_ptr(&self.inner) == Arc::<WatcherInner>::as_ptr(&other.inner)
    }
}

impl Eq for Watcher {}

impl Clone for Watcher {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl WatcherInner {
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
}

impl Watcher {
    /// New `Watcher`
    pub(crate) fn new(
        key_range: KeyRange,
        watch_id: WatchId,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> Self {
        Self {
            inner: Arc::new(WatcherInner::new(
                key_range, watch_id, start_rev, filters, event_tx,
            )),
        }
    }

    /// Get watch id
    pub(crate) fn watch_id(&self) -> i64 {
        self.inner.watch_id
    }

    /// Get key range
    pub(crate) fn key_range(&self) -> &KeyRange {
        &self.inner.key_range
    }

    /// Get start revision
    pub(crate) fn start_rev(&self) -> i64 {
        self.inner.start_rev
    }

    /// Filter event
    pub(crate) fn filter_events(&self, events: &[&Event]) -> Vec<Event> {
        events
            .iter()
            .filter_map(|&event| {
                if self.key_range().contains_key(
                    &event
                        .kv
                        .as_ref()
                        .unwrap_or_else(|| panic!("Receive Event with empty kv"))
                        .key,
                ) {
                    if self
                        .inner
                        .filters
                        .iter()
                        .any(|filter| filter == &event.r#type)
                    {
                        None
                    } else {
                        Some(event.clone())
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    /// Notify event
    pub(crate) async fn notify(&self, id: WatchId, revision: i64, events: Vec<Event>) {
        let watch_event = WatchEvent {
            id,
            events,
            revision,
        };
        assert!(
            self.inner.event_tx.send(watch_event).await.is_ok(),
            "WatchEvent receiver is closed"
        );
    }
}

/// KV watcher
#[derive(Debug)]
pub(crate) struct KvWatcher {
    /// Inner data
    inner: Arc<KvWatcherInner>,
}

/// KV watcher inner data
#[derive(Debug)]
pub(crate) struct KvWatcherInner {
    /// KV storage
    storage: Arc<KvStoreBackend>,
    /// watchers map
    /// TODO: change to more effecient data structure
    watcher_map: Mutex<HashMap<KeyRange, HashSet<Watcher>>>,
}

impl KvWatcher {
    /// New `KvWatcher`
    pub(crate) fn new(
        storage: Arc<KvStoreBackend>,
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
#[async_trait::async_trait]
pub(crate) trait KvWatcherOps {
    /// Create a watch to KV store
    async fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> (Watcher, Vec<Event>, i64);

    /// Cancel a watch from KV store
    fn cancel(&self, watcher: &Watcher) -> i64;
}

#[async_trait::async_trait]
impl KvWatcherOps for KvWatcher {
    /// Create a watch to KV store
    async fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> (Watcher, Vec<Event>, i64) {
        self.inner
            .watch(id, key_range, start_rev, filters, event_tx)
            .await
    }

    /// Cancel a watch from KV store
    fn cancel(&self, watcher: &Watcher) -> i64 {
        self.inner.cancel(watcher)
    }
}

impl KvWatcherInner {
    /// New `KvWatchInner`
    fn new(storage: Arc<KvStoreBackend>) -> Self {
        Self {
            storage,
            watcher_map: Mutex::new(HashMap::new()),
        }
    }

    /// Create a watch to KV store
    async fn watch(
        &self,
        id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> (Watcher, Vec<Event>, i64) {
        let watcher = Watcher::new(key_range.clone(), id, start_rev, filters, event_tx);
        let revision = self.storage.revision();
        // TODO handle racing that new event is generated before watcher is registered
        let events = if start_rev == 0 {
            vec![]
        } else {
            self.storage
                .get_event_from_revision(key_range.clone(), start_rev)
        };

        let _prev = self
            .watcher_map
            .lock()
            .entry(key_range)
            .or_insert_with(HashSet::new)
            .insert(watcher.clone());
        (watcher, events, revision)
    }

    /// Cancel a watch from KV store
    fn cancel(&self, watcher: &Watcher) -> i64 {
        let revision = self.storage.revision();
        let key_range = watcher.key_range().clone();
        let _prev = self.watcher_map.lock().entry(key_range).and_modify(|set| {
            let _ = set.remove(watcher);
        });

        revision
    }

    /// Handle KV store updates
    async fn handle_kv_updates(&self, updates: (i64, Vec<Event>)) {
        let (revision, updates) = updates;
        let mut watchers = HashSet::new();
        let events: Vec<_> = {
            let watcher_map = self.watcher_map.lock();
            updates
                .iter()
                .filter(|e| {
                    let mut watched = false;
                    watcher_map.iter().for_each(|(k, v)| {
                        if k.contains_key(
                            &e.kv
                                .as_ref()
                                .unwrap_or_else(|| panic!("Receive Event with empty kv"))
                                .key,
                        ) {
                            watchers.extend(v.iter().cloned());
                            watched = true;
                        }
                    });
                    watched
                })
                .collect()
        };

        for watcher in watchers {
            if revision < watcher.start_rev() {
                continue;
            }
            let filtered_event = watcher.filter_events(&events);
            if !filtered_event.is_empty() {
                watcher
                    .notify(watcher.watch_id(), revision, filtered_event)
                    .await;
            }
        }
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
