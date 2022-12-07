use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{cmd::ProposeId, error::ExecuteError};
use log::debug;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::warn;
use uuid::Uuid;

use super::{
    db::DB,
    index::{Index, IndexOperate},
    kvwatcher::KvWatcher,
    leasestore::DeleteMessage,
    LeaseStore,
};
use crate::{
    header_gen::HeaderGenerator,
    rpc::{
        Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse, Event,
        EventType, KeyValue, PutRequest, PutResponse, RangeRequest, RangeResponse, Request,
        RequestOp, RequestWithToken, RequestWrapper, ResponseWrapper, SortOrder, SortTarget,
        TargetUnion, TxnRequest, TxnResponse,
    },
    server::command::{CommandResponse, KeyRange, SyncResponse},
    storage::req_ctx::RequestCtx,
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

/// KV store
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct KvStore {
    /// KV store Backend
    inner: Arc<KvStoreBackend>,
    /// KV watcher
    kv_watcher: Arc<KvWatcher>,
}

/// KV store inner
#[derive(Debug)]
pub(crate) struct KvStoreBackend {
    /// Key Index
    index: Index,
    /// DB to store key value
    db: DB,
    /// Revision
    revision: Arc<Mutex<i64>>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<ProposeId, Vec<RequestCtx>>>,
    /// KV update sender
    kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
    /// Lease storage
    lease_storage: Arc<LeaseStore>,
}

impl KvStore {
    /// New `KvStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new(
        lease_storage: Arc<LeaseStore>,
        del_rx: mpsc::Receiver<DeleteMessage>,
        header_gen: Arc<HeaderGenerator>,
    ) -> Self {
        let (kv_update_tx, kv_update_rx) = mpsc::channel(CHANNEL_SIZE);
        let inner = Arc::new(KvStoreBackend::new(kv_update_tx, lease_storage, header_gen));
        let kv_watcher = Arc::new(KvWatcher::new(Arc::clone(&inner), kv_update_rx));
        let _del_task = tokio::spawn({
            let inner = Arc::clone(&inner);
            Self::del_task(del_rx, inner)
        });

        Self { inner, kv_watcher }
    }

    /// Receive keys from lease storage and delete them
    async fn del_task(mut del_rx: mpsc::Receiver<DeleteMessage>, inner: Arc<KvStoreBackend>) {
        while let Some(msg) = del_rx.recv().await {
            let (keys, tx) = msg.unpack();
            debug!("Delete keys: {:?} by lease revoked", keys);
            println!("Delete keys: {:?} by lease revoked", keys);

            let id = ProposeId::new(Uuid::new_v4().to_string());
            let del_reqs = keys
                .into_iter()
                .map(|key| RequestOp {
                    request: Some(Request::RequestDeleteRange(DeleteRangeRequest {
                        key,
                        range_end: vec![],
                        prev_kv: false,
                    })),
                })
                .collect();
            let txn_req = TxnRequest {
                compare: vec![],
                success: del_reqs,
                failure: vec![],
            };
            if let Err(e) = inner.handle_kv_requests(id.clone(), txn_req.into()) {
                warn!("Delete keys by lease revoked failed: {:?}", e);
            }
            let _revision = inner.sync_requests(&id).await;

            if tx.send(()).is_err() {
                warn!("receiver dropped");
            }
        }
    }

    /// execute a kv request
    pub(crate) fn execute(
        &self,
        id: ProposeId,
        request: RequestWithToken,
    ) -> Result<CommandResponse, ExecuteError> {
        self.inner
            .handle_kv_requests(id, request.request)
            .map(CommandResponse::new)
    }

    /// sync a kv request
    pub(crate) async fn after_sync(&self, id: ProposeId) -> SyncResponse {
        SyncResponse::new(self.inner.sync_requests(&id).await)
    }

    /// Get KV watcher
    pub(crate) fn kv_watcher(&self) -> Arc<KvWatcher> {
        Arc::clone(&self.kv_watcher)
    }
}

impl KvStoreBackend {
    /// New `KvStoreBackend`
    pub(crate) fn new(
        kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
        lease_storage: Arc<LeaseStore>,
        header_gen: Arc<HeaderGenerator>,
    ) -> Self {
        Self {
            index: Index::new(),
            db: DB::new(),
            revision: header_gen.revision_arc(),
            header_gen,
            sp_exec_pool: Mutex::new(HashMap::new()),
            kv_update_tx,
            lease_storage,
        }
    }

    /// Get revision of KV store
    pub(crate) fn revision(&self) -> i64 {
        *self.revision.lock()
    }

    /// Notify KV changes to KV watcher
    async fn notify_updates(&self, revision: i64, updates: Vec<Event>) {
        assert!(
            self.kv_update_tx.send((revision, updates)).await.is_ok(),
            "Failed to send updates to KV watchter"
        );
    }

    /// Handle kv requests
    fn handle_kv_requests(
        &self,
        id: ProposeId,
        wrapper: RequestWrapper,
    ) -> Result<ResponseWrapper, ExecuteError> {
        debug!("Receive request {:?}", wrapper);
        #[allow(clippy::wildcard_enum_match_arm)]
        let res = match wrapper {
            RequestWrapper::RangeRequest(ref req) => {
                debug!("Receive RangeRequest {:?}", req);
                Ok(self.handle_range_request(req).into())
            }
            RequestWrapper::PutRequest(ref req) => {
                debug!("Receive PutRequest {:?}", req);
                self.handle_put_request(req).map(Into::into)
            }
            RequestWrapper::DeleteRangeRequest(ref req) => {
                debug!("Receive DeleteRangeRequest {:?}", req);
                Ok(self.handle_delete_range_request(req).into())
            }
            RequestWrapper::TxnRequest(ref req) => {
                debug!("Receive TxnRequest {:?}", req);
                self.handle_txn_request(&id, req).map(Into::into)
            }
            _ => unreachable!("Other request should not be sent to this store"),
        };
        if !matches!(wrapper, RequestWrapper::TxnRequest(_)) {
            let ctx = RequestCtx::new(wrapper, res.is_err());
            self.sp_exec_pool
                .lock()
                .entry(id)
                .or_insert_with(Vec::new)
                .push(ctx);
        }
        res
    }

    /// Get `KeyValue` of a range
    ///
    /// If `range_end` is `&[]`, this function will return one or zero `KeyValue`.
    fn get_range(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<KeyValue> {
        let revisions = self.index.get(key, range_end, revision);
        self.db.get_values(&revisions)
    }

    /// Get `KeyValue` start from a revision and convert to `Event`
    pub(crate) fn get_event_from_revision(&self, key_range: KeyRange, revision: i64) -> Vec<Event> {
        let key = key_range.start.as_slice();
        let range_end = key_range.end.as_slice();
        let revisions = self.index.get_from_rev(key, range_end, revision);
        let values = self.db.get_values(&revisions);
        values
            .into_iter()
            .map(|kv| {
                // Delete
                #[allow(clippy::as_conversions)] // This cast is always valid
                let event_type = if kv.version == 0 && kv.create_revision == 0 {
                    EventType::Delete
                } else {
                    EventType::Put
                };
                let mut event = Event {
                    kv: Some(kv),
                    prev_kv: None,
                    ..Default::default()
                };
                event.set_type(event_type);
                event
            })
            .collect()
    }

    /// Handle `RangeRequest`
    fn handle_range_request(&self, req: &RangeRequest) -> RangeResponse {
        let mut kvs = self.get_range(&req.key, &req.range_end, req.revision);
        debug!("handle_range_request kvs {:?}", kvs);
        let mut response = RangeResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            count: kvs.len().cast(),
            ..RangeResponse::default()
        };
        if !req.count_only {
            match (req.sort_target(), req.sort_order()) {
                (SortTarget::Key, SortOrder::None) => {}
                (SortTarget::Key, SortOrder::Ascend) => {
                    kvs.sort_by(|a, b| a.key.cmp(&b.key));
                }
                (SortTarget::Key, SortOrder::Descend) => {
                    kvs.sort_by(|a, b| b.key.cmp(&a.key));
                }
                (SortTarget::Version, SortOrder::Ascend | SortOrder::None) => {
                    kvs.sort_by(|a, b| a.version.cmp(&b.version));
                }
                (SortTarget::Version, SortOrder::Descend) => {
                    kvs.sort_by(|a, b| b.version.cmp(&a.version));
                }
                (SortTarget::Create, SortOrder::Ascend | SortOrder::None) => {
                    kvs.sort_by(|a, b| a.create_revision.cmp(&b.create_revision));
                }
                (SortTarget::Create, SortOrder::Descend) => {
                    kvs.sort_by(|a, b| b.create_revision.cmp(&a.create_revision));
                }
                (SortTarget::Mod, SortOrder::Ascend | SortOrder::None) => {
                    kvs.sort_by(|a, b| a.mod_revision.cmp(&b.mod_revision));
                }
                (SortTarget::Mod, SortOrder::Descend) => {
                    kvs.sort_by(|a, b| b.mod_revision.cmp(&a.mod_revision));
                }
                (SortTarget::Value, SortOrder::Ascend | SortOrder::None) => {
                    kvs.sort_by(|a, b| a.value.cmp(&b.value));
                }
                (SortTarget::Value, SortOrder::Descend) => {
                    kvs.sort_by(|a, b| b.value.cmp(&a.value));
                }
            }
            if (req.limit > 0) && (kvs.len() > req.limit.cast()) {
                response.more = true;
                kvs.truncate(req.limit.cast());
            }
            response.kvs = kvs;
        }
        response
    }

    /// Handle `PutRequest`
    fn handle_put_request(&self, req: &PutRequest) -> Result<PutResponse, ExecuteError> {
        debug!("handle_put_request");
        let mut response = PutResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            ..Default::default()
        };
        if req.prev_kv || req.ignore_lease || req.ignore_value {
            let prev_kv = self.get_range(&req.key, &[], 0).pop();
            if prev_kv.is_none() && (req.ignore_lease || req.ignore_value) {
                return Err(ExecuteError::InvalidCommand(
                    "ignore_lease or ignore_value is set but there is no previous value".to_owned(),
                ));
            }
            if req.prev_kv {
                response.prev_kv = prev_kv;
            }
        };
        Ok(response)
    }

    /// Handle `DeleteRangeRequest`
    fn handle_delete_range_request(&self, req: &DeleteRangeRequest) -> DeleteRangeResponse {
        let prev_kvs = self.get_range(&req.key, &req.range_end, 0);
        debug!("handle_delete_range_request prev_kvs {:?}", prev_kvs);
        let mut response = DeleteRangeResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            ..DeleteRangeResponse::default()
        };
        response.deleted = prev_kvs.len().cast();
        if req.prev_kv {
            response.prev_kvs = prev_kvs;
        }
        response
    }

    /// Compare i64
    fn compare_i64(val: i64, target: i64) -> CompareResult {
        match val.cmp(&target) {
            Ordering::Greater => CompareResult::Greater,
            Ordering::Less => CompareResult::Less,
            Ordering::Equal => CompareResult::Equal,
        }
    }

    /// Compare vec<u8>
    fn compare_vec_u8(val: &[u8], target: &[u8]) -> CompareResult {
        match val.cmp(target) {
            Ordering::Greater => CompareResult::Greater,
            Ordering::Less => CompareResult::Less,
            Ordering::Equal => CompareResult::Equal,
        }
    }

    /// Check one `KeyValue` with `Compare`
    fn compare_kv(cmp: &Compare, kv: &KeyValue) -> bool {
        let result = match cmp.target() {
            CompareTarget::Version => {
                let rev = if let Some(TargetUnion::Version(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.version, rev)
            }
            CompareTarget::Create => {
                let rev = if let Some(TargetUnion::CreateRevision(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.create_revision, rev)
            }
            CompareTarget::Mod => {
                let rev = if let Some(TargetUnion::ModRevision(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.mod_revision, rev)
            }
            CompareTarget::Value => {
                let empty = vec![];
                let val = if let Some(TargetUnion::Value(ref v)) = cmp.target_union {
                    v
                } else {
                    &empty
                };
                Self::compare_vec_u8(&kv.value, val)
            }
            CompareTarget::Lease => {
                let les = if let Some(TargetUnion::Lease(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.mod_revision, les)
            }
        };

        match cmp.result() {
            CompareResult::Equal => result == CompareResult::Equal,
            CompareResult::Greater => result == CompareResult::Greater,
            CompareResult::Less => result == CompareResult::Less,
            CompareResult::NotEqual => result != CompareResult::Equal,
        }
    }

    /// Check result of a `Compare`
    fn check_compare(&self, cmp: &Compare) -> bool {
        let kvs = self.get_range(&cmp.key, &cmp.range_end, 0);
        if kvs.is_empty() {
            if let Some(TargetUnion::Value(_)) = cmp.target_union {
                false
            } else {
                Self::compare_kv(cmp, &KeyValue::default())
            }
        } else {
            kvs.iter().all(|kv| Self::compare_kv(cmp, kv))
        }
    }

    /// Handle `TxnRequest`
    fn handle_txn_request(
        &self,
        id: &ProposeId,
        req: &TxnRequest,
    ) -> Result<TxnResponse, ExecuteError> {
        let success = req
            .compare
            .iter()
            .all(|compare| self.check_compare(compare));
        let requests = if success {
            req.success.iter()
        } else {
            req.failure.iter()
        };
        let mut responses = Vec::with_capacity(requests.len());
        for request_op in requests {
            let response = self.handle_kv_requests(id.clone(), request_op.clone().into())?;
            responses.push(response.into());
        }
        Ok(TxnResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            succeeded: success,
            responses,
        })
    }

    /// Sync a vec of requests
    async fn sync_requests(&self, id: &ProposeId) -> i64 {
        let ctxes = self.sp_exec_pool.lock().remove(id).unwrap_or_else(|| {
            panic!("Failed to get speculative execution propose id {:?}", id);
        });
        if ctxes.iter().any(RequestCtx::met_err) {
            return self.revision();
        };
        let requests: Vec<RequestWrapper> = ctxes.into_iter().map(RequestCtx::req).collect();
        let revision = self.revision();
        let next_revision = revision.overflow_add(1);
        let mut sub_revision = 0;
        let mut all_events = vec![];
        for request in requests {
            let mut events = self.sync_request(request, next_revision, sub_revision);
            sub_revision = sub_revision.overflow_add(events.len().cast());
            all_events.append(&mut events);
        }
        if all_events.is_empty() {
            revision
        } else {
            self.notify_updates(next_revision, all_events).await;
            *self.revision.lock() = next_revision;
            next_revision
        }
    }

    /// Sync one `Request`
    fn sync_request(&self, req: RequestWrapper, revision: i64, sub_revision: i64) -> Vec<Event> {
        #[allow(clippy::wildcard_enum_match_arm)]
        match req {
            RequestWrapper::RangeRequest(req) => {
                debug!("Sync RequestRange {:?}", req);
                Self::sync_range_request(&req)
            }
            RequestWrapper::PutRequest(req) => {
                debug!("Sync RequestPut {:?}", req);
                self.sync_put_request(req, revision, sub_revision)
            }
            RequestWrapper::DeleteRangeRequest(req) => {
                debug!("Sync DeleteRequest {:?}", req);
                self.sync_delete_range_request(req, revision, sub_revision)
            }
            RequestWrapper::TxnRequest(req) => {
                debug!("Sync TxnRequest {:?}", req);
                panic!("Sync for TxnRequest is impossible");
            }
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        }
    }

    /// Sync `RangeRequest` and return of kvstore is changed
    fn sync_range_request(_req: &RangeRequest) -> Vec<Event> {
        Vec::new()
    }

    /// Sync `PutRequest` and return if kvstore is changed
    fn sync_put_request(&self, req: PutRequest, revision: i64, sub_revision: i64) -> Vec<Event> {
        let prev_kv = self.get_range(&req.key, &[], 0).pop();
        let new_rev = self
            .index
            .insert_or_update_revision(&req.key, revision, sub_revision);
        let mut kv = KeyValue {
            key: req.key,
            value: req.value,
            create_revision: new_rev.create_revision,
            mod_revision: new_rev.mod_revision,
            version: new_rev.version,
            lease: req.lease,
        };

        if req.ignore_lease || req.ignore_value {
            #[allow(clippy::unwrap_used)] // checked when execute cmd
            let prev = prev_kv.as_ref().unwrap();
            if req.ignore_lease {
                kv.lease = prev.lease;
            }
            if req.ignore_value {
                kv.value = prev.value.clone();
            }
        }

        let _prev = self.db.insert(new_rev.as_revision(), kv.clone());
        let old_lease = self.lease_storage.get_lease(&kv.key);
        if old_lease != 0 {
            self.lease_storage
                .detach(old_lease, &kv.key)
                .unwrap_or_else(|e| warn!("Failed to detach lease from a key, error: {:?}", e));
        }
        if req.lease != 0 {
            self.lease_storage
                .attach(req.lease, kv.key.clone()) // already checked, lease is not 0
                .unwrap_or_else(|e| panic!("unexpected error from lease Attach: {}", e));
        }
        let event = Event {
            #[allow(clippy::as_conversions)] // This cast is always valid
            r#type: EventType::Put as i32,
            kv: Some(kv),
            prev_kv,
        };
        vec![event]
    }

    /// create events for a deletion
    fn new_deletion_events(revision: i64, prev_kvs: Vec<KeyValue>) -> Vec<Event> {
        prev_kvs
            .into_iter()
            .map(|prev| {
                let kv = KeyValue {
                    key: prev.key.clone(),
                    mod_revision: revision,
                    ..Default::default()
                };
                Event {
                    #[allow(clippy::as_conversions)] // This cast is always valid
                    r#type: EventType::Delete as i32,
                    kv: Some(kv),
                    prev_kv: Some(prev),
                }
            })
            .collect()
    }

    /// Sync `DeleteRangeRequest` and return if kvstore is changed
    fn sync_delete_range_request(
        &self,
        req: DeleteRangeRequest,
        revision: i64,
        sub_revision: i64,
    ) -> Vec<Event> {
        let key = req.key;
        let range_end = req.range_end;
        let revisions = self.index.delete(&key, &range_end, revision, sub_revision);
        debug!("sync_delete_range_request: revisions {:?}", revisions);
        let prev_kv = self.db.mark_deletions(&revisions);
        for kv in &prev_kv {
            let lease_id = self.lease_storage.get_lease(&kv.key);
            self.lease_storage
                .detach(lease_id, &kv.key)
                .unwrap_or_else(|e| warn!("Failed to detach lease from a key, error: {:?}", e));
        }
        Self::new_deletion_events(revision, prev_kv)
    }
}
