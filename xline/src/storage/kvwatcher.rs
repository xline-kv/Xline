use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use clippy_utilities::OverflowArithmetic;
use futures::{stream::FuturesUnordered, StreamExt};
use log::warn;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use utils::parking_lot_lock::RwLockMap;

use super::storage_api::StorageApi;
use crate::{rpc::Event, server::command::KeyRange, storage::kv_store::KvStoreBackend};

/// Watch ID
pub(crate) type WatchId = i64;

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
    /// Syncing flag
    syncing: AtomicBool,
    /// Syncing notify
    syncing_notify: Arc<event_listener::Event>,
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

    /// Mark as syncing done and notify
    fn sync_done(&self);
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

    /// Mark as syncing done and notify
    fn sync_done(&self) {
        self.inner.sync_done();
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
            syncing: AtomicBool::new(false),
            syncing_notify: Arc::new(event_listener::Event::new()),
        }
    }

    /// Is there a watcher that is syncing
    fn is_syncing(&self) -> bool {
        self.syncing.load(Ordering::SeqCst)
    }

    /// Mark current state as syncing
    fn start_syncing(&self) {
        self.syncing.store(true, Ordering::SeqCst);
    }

    /// Mark as syncing done and notify
    fn sync_done(&self) {
        self.syncing.store(false, Ordering::SeqCst);
        self.syncing_notify.notify(1);
    }

    /// If current state is syncing, wait until it is done
    async fn wait_syncing(&self) {
        while self.is_syncing() {
            self.syncing_notify.listen().await;
        }
    }

    /// Create a watch to KV store
    fn watch(
        &self,
        watch_id: WatchId,
        key_range: KeyRange,
        start_rev: i64,
        filters: Vec<i32>,
        event_tx: mpsc::Sender<WatchEvent>,
    ) -> (Vec<Event>, i64) {
        self.start_syncing();
        let mut revision = self.storage.revision();
        let initial_events = if start_rev == 0 {
            vec![]
        } else {
            let (rev, events) = self
                .storage
                .get_events_from_revision(key_range.clone(), start_rev)
                .unwrap_or_else(|e| {
                    warn!("failed to get initial events for watcher: {:?}", e);
                    (0, vec![])
                });
            if !events.is_empty() {
                revision = rev.overflow_add(1);
            }
            events
        };
        let watcher = Watcher::new(key_range, watch_id, revision, filters, event_tx);
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
        self.wait_syncing().await;
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

#[cfg(test)]
mod test {

    use std::{collections::BTreeMap, time::Duration};

    use tokio::time::timeout;
    use utils::config::StorageConfig;

    use crate::{
        header_gen::HeaderGenerator,
        rpc::{PutRequest, RequestWithToken},
        storage::{db::DBProxy, index::Index, lease_store::LeaseMessage, KvStore},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn watcher_should_get_all_events() {
        let (store, db) = init_empty_store();
        let mut map = BTreeMap::new();
        let handle = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                for i in 0..100_u8 {
                    let req = RequestWithToken::new(
                        PutRequest {
                            key: "foo".into(),
                            value: vec![i],
                            ..Default::default()
                        }
                        .into(),
                    );
                    let (sync_res, ops) = store.after_sync(&req).await.unwrap();
                    db.flush_ops(ops).unwrap();
                    store.mark_index_available(sync_res.revision());
                }
            }
        });
        tokio::time::sleep(std::time::Duration::from_micros(500)).await;
        let watcher = store.kv_watcher();
        let (event_tx, mut event_rx) = mpsc::channel(128);
        let (events, _revision) =
            watcher.watch(123, KeyRange::new_one_key("foo"), 1, vec![], event_tx);
        // send events to client
        watcher.sync_done();
        for event in events {
            let val = event.kv.as_ref().unwrap().value[0];
            let e = map.entry(val).or_insert(0);
            *e += 1;
        }

        while let Some(event) = timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
        {
            let val = event.events.first().unwrap().kv.as_ref().unwrap().value[0];
            let e = map.entry(val).or_insert(0);
            *e += 1;
            if val == 99 {
                break;
            }
        }

        assert_eq!(map.len(), 100);
        for count in map.values() {
            assert_eq!(*count, 1);
        }
        handle.abort();
    }

    fn init_empty_store() -> (Arc<KvStore<DBProxy>>, Arc<DBProxy>) {
        let db = DBProxy::open(&StorageConfig::Memory).unwrap();
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let (lease_cmd_tx, mut lease_cmd_rx) = mpsc::channel(128);
        let index = Arc::new(Index::new());
        let _handle = tokio::spawn(async move {
            while let Some(LeaseMessage::GetLease(tx, _)) = lease_cmd_rx.recv().await {
                assert!(tx.send(0).is_ok());
            }
        });
        (
            Arc::new(KvStore::new(
                lease_cmd_tx,
                header_gen,
                Arc::clone(&db),
                index,
            )),
            db,
        )
    }
}
