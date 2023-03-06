use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use clippy_utilities::{Cast, OverflowArithmetic};
use curp::cmd::ProposeId;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use uuid::Uuid;

use super::{
    index::{Index, IndexOperate},
    kvwatcher::KvWatcher,
    lease_store::{DeleteMessage, LeaseMessage},
    storage_api::StorageApi,
    Revision,
};
use crate::{
    header_gen::HeaderGenerator,
    revision_number::RevisionNumber,
    rpc::{
        Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse, Event,
        EventType, KeyValue, PutRequest, PutResponse, RangeRequest, RangeResponse, Request,
        RequestOp, RequestWithToken, RequestWrapper, ResponseWrapper, SortOrder, SortTarget,
        TargetUnion, TxnRequest, TxnResponse,
    },
    server::command::{CommandResponse, KeyRange, SyncResponse},
    storage::{ExecuteError, RequestCtx},
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

/// KV store
#[derive(Debug)]
pub(crate) struct KvStore<DB>
where
    DB: StorageApi,
{
    /// KV store Backend
    inner: Arc<KvStoreBackend<DB>>,
    /// KV watcher
    kv_watcher: Arc<KvWatcher<DB>>,
}

/// KV store inner
#[derive(Debug)]
pub(crate) struct KvStoreBackend<DB>
where
    DB: StorageApi,
{
    /// Table name
    table: String,
    /// Key Index
    index: Index,
    /// DB to store key value
    db: Arc<DB>,
    /// Revision
    revision: Arc<RevisionNumber>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<ProposeId, Vec<RequestCtx>>>,
    /// KV update sender
    kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
    /// Lease command sender
    lease_cmd_tx: mpsc::Sender<LeaseMessage>,
}

impl<DB> KvStore<DB>
where
    DB: StorageApi,
{
    /// New `KvStore`
    pub(crate) fn new(
        lease_cmd_tx: mpsc::Sender<LeaseMessage>,
        del_rx: mpsc::Receiver<DeleteMessage>,
        header_gen: Arc<HeaderGenerator>,
        storage: Arc<DB>,
    ) -> Self {
        let (kv_update_tx, kv_update_rx) = mpsc::channel(CHANNEL_SIZE);
        let inner = Arc::new(KvStoreBackend::new(
            kv_update_tx,
            lease_cmd_tx,
            header_gen,
            storage,
        ));
        let kv_watcher = Arc::new(KvWatcher::new(Arc::clone(&inner), kv_update_rx));
        let _del_task = tokio::spawn({
            let inner = Arc::clone(&inner);
            Self::del_task(del_rx, inner)
        });
        Self { inner, kv_watcher }
    }

    /// Receive keys from lease storage and delete them
    async fn del_task(mut del_rx: mpsc::Receiver<DeleteMessage>, inner: Arc<KvStoreBackend<DB>>) {
        while let Some(msg) = del_rx.recv().await {
            let (keys, tx) = msg.unpack();
            debug!("Delete keys: {:?} by lease revoked", keys);
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
    pub(crate) async fn after_sync(&self, id: ProposeId) -> Result<SyncResponse, ExecuteError> {
        self.inner.sync_requests(&id).await.map(SyncResponse::new)
    }

    /// Get KV watcher
    pub(crate) fn kv_watcher(&self) -> Arc<KvWatcher<DB>> {
        Arc::clone(&self.kv_watcher)
    }
}

impl<DB> KvStoreBackend<DB>
where
    DB: StorageApi,
{
    /// New `KvStoreBackend`
    pub(crate) fn new(
        kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
        lease_cmd_tx: mpsc::Sender<LeaseMessage>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
    ) -> Self {
        Self {
            table: "kv".to_owned(),
            index: Index::new(),
            db,
            revision: header_gen.revision_arc(),
            header_gen,
            sp_exec_pool: Mutex::new(HashMap::new()),
            kv_update_tx,
            lease_cmd_tx,
        }
    }

    /// Get revision of KV store
    pub(crate) fn revision(&self) -> i64 {
        self.revision.get()
    }

    /// Notify KV changes to KV watcher
    async fn notify_updates(&self, revision: i64, updates: Vec<Event>) {
        assert!(
            self.kv_update_tx.send((revision, updates)).await.is_ok(),
            "Failed to send updates to KV watcher"
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
                self.handle_range_request(req).map(Into::into)
            }
            RequestWrapper::PutRequest(ref req) => {
                debug!("Receive PutRequest {:?}", req);
                self.handle_put_request(req).map(Into::into)
            }
            RequestWrapper::DeleteRangeRequest(ref req) => {
                debug!("Receive DeleteRangeRequest {:?}", req);
                self.handle_delete_range_request(req).map(Into::into)
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

    /// Get `KeyValue` from the `KvStoreBackend`
    fn get_values(&self, revisions: &[Revision]) -> Result<Vec<KeyValue>, ExecuteError> {
        let revisions = revisions
            .iter()
            .map(Revision::encode_to_vec)
            .collect::<Vec<Vec<u8>>>();
        let values = self.db.get_values(&self.table, &revisions)?;
        let kvs = values
            .into_iter()
            .map(|v| KeyValue::decode(v.as_slice()))
            .collect::<Result<_, _>>()
            .map_err(|e| {
                ExecuteError::DbError(format!("Failed to decode key-value from DB, error: {e}"))
            })?;
        Ok(kvs)
    }

    /// Get `KeyValue` of a range
    ///
    /// If `range_end` is `&[]`, this function will return one or zero `KeyValue`.
    fn get_range(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
    ) -> Result<Vec<KeyValue>, ExecuteError> {
        let revisions = self.index.get(key, range_end, revision);
        self.get_values(&revisions)
    }

    /// Get `KeyValue` of a range with limit and count only, return kvs and total count
    fn get_range_with_opts(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        limit: usize,
        count_only: bool,
    ) -> Result<(Vec<KeyValue>, usize), ExecuteError> {
        let mut revisions = self.index.get(key, range_end, revision);
        let total = revisions.len();
        if count_only {
            return Ok((vec![], total));
        }
        if limit != 0 {
            revisions.truncate(limit);
        }
        let kvs = self.get_values(&revisions)?;
        Ok((kvs, total))
    }

    /// Get `KeyValue` start from a revision and convert to `Event`
    pub(crate) fn get_event_from_revision(
        &self,
        key_range: KeyRange,
        revision: i64,
    ) -> Result<Vec<Event>, ExecuteError> {
        let key = key_range.start.as_slice();
        let range_end = key_range.end.as_slice();
        let revisions = self.index.get_from_rev(key, range_end, revision);
        let events = self
            .get_values(&revisions)?
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
            .collect();
        Ok(events)
    }

    /// Sort kvs by sort target and order
    fn sort_kvs(kvs: &mut [KeyValue], sort_order: SortOrder, sort_target: SortTarget) {
        match (sort_target, sort_order) {
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
        };
    }

    /// filter kvs by `{max,min}_{mod,create}_revision`
    fn filter_kvs(
        kvs: &mut Vec<KeyValue>,
        max_mod_revision: i64,
        min_mod_revision: i64,
        max_create_revision: i64,
        min_create_revision: i64,
    ) {
        if max_mod_revision > 0 {
            kvs.retain(|kv| kv.mod_revision <= max_mod_revision);
        }
        if min_mod_revision > 0 {
            kvs.retain(|kv| kv.mod_revision >= min_mod_revision);
        }
        if max_create_revision > 0 {
            kvs.retain(|kv| kv.create_revision <= max_create_revision);
        }
        if min_create_revision > 0 {
            kvs.retain(|kv| kv.create_revision >= min_create_revision);
        }
    }

    /// Handle `RangeRequest`
    fn handle_range_request(&self, req: &RangeRequest) -> Result<RangeResponse, ExecuteError> {
        debug!("handle_range_request kvs");
        let storage_fetch_limit = if (req.sort_order() != SortOrder::None)
            || (req.max_mod_revision != 0)
            || (req.min_mod_revision != 0)
            || (req.max_create_revision != 0)
            || (req.min_create_revision != 0)
            || (req.limit == 0)
        {
            0 // get all from storage then sort and filter
        } else {
            req.limit.overflow_add(1) // get one extra for "more" flag
        };
        let (mut kvs, total) = self.get_range_with_opts(
            &req.key,
            &req.range_end,
            req.revision,
            storage_fetch_limit.cast(),
            req.count_only,
        )?;
        let mut response = RangeResponse {
            header: Some(self.header_gen.gen_header()),
            count: total.cast(),
            ..RangeResponse::default()
        };
        if kvs.is_empty() {
            return Ok(response);
        }

        Self::filter_kvs(
            &mut kvs,
            req.max_mod_revision,
            req.min_mod_revision,
            req.max_create_revision,
            req.min_create_revision,
        );
        Self::sort_kvs(&mut kvs, req.sort_order(), req.sort_target());

        if (req.limit > 0) && (kvs.len() > req.limit.cast()) {
            response.more = true;
            kvs.truncate(req.limit.cast());
        }
        if req.keys_only {
            kvs.iter_mut().for_each(|kv| kv.value.clear());
        }
        response.kvs = kvs;
        Ok(response)
    }

    /// Handle `PutRequest`
    fn handle_put_request(&self, req: &PutRequest) -> Result<PutResponse, ExecuteError> {
        debug!("handle_put_request");
        let mut response = PutResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            ..Default::default()
        };
        if req.prev_kv || req.ignore_lease || req.ignore_value {
            let prev_kv = self.get_range(&req.key, &[], 0)?.pop();
            if prev_kv.is_none() && (req.ignore_lease || req.ignore_value) {
                return Err(ExecuteError::key_not_found());
            }
            if req.prev_kv {
                response.prev_kv = prev_kv;
            }
        };
        Ok(response)
    }

    /// Handle `DeleteRangeRequest`
    fn handle_delete_range_request(
        &self,
        req: &DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, ExecuteError> {
        let prev_kvs = self.get_range(&req.key, &req.range_end, 0)?;
        debug!("handle_delete_range_request prev_kvs {:?}", prev_kvs);
        let mut response = DeleteRangeResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            ..DeleteRangeResponse::default()
        };
        response.deleted = prev_kvs.len().cast();
        if req.prev_kv {
            response.prev_kvs = prev_kvs;
        }
        Ok(response)
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
        let kvs = self
            .get_range(&cmp.key, &cmp.range_end, 0)
            .unwrap_or_default();
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
    async fn sync_requests(&self, id: &ProposeId) -> Result<i64, ExecuteError> {
        let ctxes = self.sp_exec_pool.lock().remove(id).unwrap_or_else(|| {
            panic!("Failed to get speculative execution propose id {id:?}");
        });
        if ctxes.iter().any(RequestCtx::met_err) {
            return Ok(self.revision());
        };
        let requests: Vec<RequestWrapper> = ctxes.into_iter().map(RequestCtx::req).collect();
        let rev = if requests.is_empty() {
            self.revision()
        } else {
            let next_revision = self.revision.next();
            let mut sub_revision = 0;
            let mut all_events = Vec::new();
            for request in requests {
                let mut events = self
                    .sync_request(request, next_revision, sub_revision)
                    .await?;
                sub_revision = sub_revision.overflow_add(events.len().cast());
                all_events.append(&mut events);
            }
            self.notify_updates(next_revision, all_events).await;
            next_revision
        };

        Ok(rev)
    }

    /// Sync one `Request`
    async fn sync_request(
        &self,
        req: RequestWrapper,
        revision: i64,
        sub_revision: i64,
    ) -> Result<Vec<Event>, ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        match req {
            RequestWrapper::RangeRequest(req) => {
                debug!("Sync RequestRange {:?}", req);
                Ok(Vec::new())
            }
            RequestWrapper::PutRequest(req) => {
                debug!("Sync RequestPut {:?}", req);
                self.sync_put_request(req, revision, sub_revision).await
            }
            RequestWrapper::DeleteRangeRequest(req) => {
                debug!("Sync DeleteRequest {:?}", req);
                self.sync_delete_range_request(req, revision, sub_revision)
                    .await
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

    /// Insert a `KeyValue`
    fn insert(&self, revision: Revision, kv: &KeyValue) -> Result<(), ExecuteError> {
        let key = revision.encode_to_vec();
        let value = kv.encode_to_vec();
        self.db.insert(&self.table, key, value, false)
    }

    /// Sync `PutRequest` and return if kvstore is changed
    async fn sync_put_request(
        &self,
        req: PutRequest,
        revision: i64,
        sub_revision: i64,
    ) -> Result<Vec<Event>, ExecuteError> {
        let prev_kv = self.get_range(&req.key, &[], 0)?.pop();
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
            let prev = prev_kv.as_ref().ok_or_else(ExecuteError::key_not_found)?;
            if req.ignore_lease {
                kv.lease = prev.lease;
            }
            if req.ignore_value {
                kv.value = prev.value.clone();
            }
        }

        let _prev = self.insert(new_rev.as_revision(), &kv);
        let old_lease = self.get_lease(&kv.key).await;
        if old_lease != 0 {
            self.detach(old_lease, kv.key.as_slice())
                .await
                .unwrap_or_else(|e| warn!("Failed to detach lease from a key, error: {:?}", e));
        }
        if req.lease != 0 {
            self.attach(req.lease, kv.key.as_slice())
                .await // already checked, lease is not 0
                .unwrap_or_else(|e| panic!("unexpected error from lease Attach: {e}"));
        }
        let event = Event {
            #[allow(clippy::as_conversions)] // This cast is always valid
            r#type: EventType::Put as i32,
            kv: Some(kv),
            prev_kv,
        };
        Ok(vec![event])
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

    /// Mark deletion for keys
    fn mark_deletions(
        &self,
        revisions: &[(Revision, Revision)],
    ) -> Result<Vec<KeyValue>, ExecuteError> {
        let prev_revisions = revisions
            .iter()
            .map(|&(prev_rev, _)| prev_rev)
            .collect::<Vec<_>>();
        let prev_kvs = self.get_values(&prev_revisions)?;
        assert!(
            prev_kvs.len() == revisions.len(),
            "Index doesn't match with DB"
        );
        prev_kvs
            .iter()
            .zip(revisions.iter())
            .try_for_each(|(kv, &(_, new_rev))| {
                let del_kv = KeyValue {
                    key: kv.key.clone(),
                    mod_revision: new_rev.revision(),
                    ..KeyValue::default()
                };
                let key = new_rev.encode_to_vec();
                let value = del_kv.encode_to_vec();
                self.db.insert(&self.table, key, value, false)
            })?;
        Ok(prev_kvs)
    }

    /// Sync `DeleteRangeRequest` and return if kvstore is changed
    async fn sync_delete_range_request(
        &self,
        req: DeleteRangeRequest,
        revision: i64,
        sub_revision: i64,
    ) -> Result<Vec<Event>, ExecuteError> {
        let key = req.key;
        let range_end = req.range_end;
        let revisions = self.index.delete(&key, &range_end, revision, sub_revision);
        debug!("sync_delete_range_request: revisions {:?}", revisions);
        let prev_kv = self.mark_deletions(&revisions)?;
        for kv in &prev_kv {
            let lease_id = self.get_lease(&kv.key).await;
            self.detach(lease_id, kv.key.as_slice())
                .await
                .unwrap_or_else(|e| warn!("Failed to detach lease from a key, error: {:?}", e));
        }
        Ok(Self::new_deletion_events(revision, prev_kv))
    }

    /// Send get lease to lease store
    async fn get_lease(&self, key: &[u8]) -> i64 {
        let (get_lease, rx) = LeaseMessage::get_lease(key);
        assert!(
            self.lease_cmd_tx.send(get_lease).await.is_ok(),
            "lease_cmd_rx is closed"
        );
        rx.await.unwrap_or_else(|_e| panic!("res sender is closed"))
    }

    /// Send detach to lease store
    async fn detach(&self, lease_id: i64, key: impl Into<Vec<u8>>) -> Result<(), ExecuteError> {
        let (detach, rx) = LeaseMessage::detach(lease_id, key);
        assert!(
            self.lease_cmd_tx.send(detach).await.is_ok(),
            "lease_cmd_rx is closed"
        );
        rx.await.unwrap_or_else(|_e| panic!("res sender is closed"))
    }

    /// Send attach to lease store
    async fn attach(&self, lease_id: i64, key: impl Into<Vec<u8>>) -> Result<(), ExecuteError> {
        let (attach, rx) = LeaseMessage::attach(lease_id, key);
        assert!(
            self.lease_cmd_tx.send(attach).await.is_ok(),
            "lease_cmd_rx is closed"
        );
        rx.await.unwrap_or_else(|_e| panic!("res sender is closed"))
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use utils::config::StorageConfig;

    use super::*;
    use crate::storage::db::DBProxy;

    #[tokio::test]
    async fn test_keys_only() -> Result<(), Box<dyn Error>> {
        let store = init_store().await?;

        let request = RangeRequest {
            key: vec![0],
            range_end: vec![0],
            keys_only: true,
            ..Default::default()
        };
        let response = store.inner.handle_range_request(&request)?;
        assert_eq!(response.kvs.len(), 5);
        for kv in response.kvs {
            assert!(kv.value.is_empty());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_range_empty() -> Result<(), Box<dyn Error>> {
        let store = init_store().await?;

        let request = RangeRequest {
            key: "x".into(),
            range_end: "y".into(),
            keys_only: true,
            ..Default::default()
        };
        let response = store.inner.handle_range_request(&request)?;
        assert_eq!(response.kvs.len(), 0);
        assert_eq!(response.count, 0);

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::indexing_slicing)] // Checked before use index
    async fn test_range_filter() -> Result<(), Box<dyn Error>> {
        let store = init_store().await?;

        let request = RangeRequest {
            key: vec![0],
            range_end: vec![0],
            max_create_revision: 3,
            min_create_revision: 2,
            max_mod_revision: 3,
            min_mod_revision: 2,
            ..Default::default()
        };
        let response = store.inner.handle_range_request(&request)?;
        assert_eq!(response.count, 5);
        assert_eq!(response.kvs.len(), 2);
        assert_eq!(response.kvs[0].create_revision, 2);
        assert_eq!(response.kvs[1].create_revision, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_range_sort() -> Result<(), Box<dyn Error>> {
        let store = init_store().await?;
        let keys = ["a", "b", "c", "d", "e"];
        let reversed_keys = ["e", "d", "c", "b", "a"];

        for order in [SortOrder::Ascend, SortOrder::Descend, SortOrder::None] {
            for target in [
                SortTarget::Key,
                SortTarget::Create,
                SortTarget::Mod,
                SortTarget::Value,
            ] {
                let response = store.inner.handle_range_request(&sort_req(order, target))?;
                assert_eq!(response.count, 5);
                assert_eq!(response.kvs.len(), 5);
                let expected = match order {
                    SortOrder::Descend => reversed_keys,
                    SortOrder::Ascend | SortOrder::None => keys,
                };
                let is_identical = response
                    .kvs
                    .iter()
                    .zip(expected.iter())
                    .all(|(kv, want)| kv.key == want.as_bytes());
                assert!(is_identical);
            }
        }
        for order in [SortOrder::Ascend, SortOrder::Descend, SortOrder::None] {
            let response = store
                .inner
                .handle_range_request(&sort_req(order, SortTarget::Version))?;
            assert_eq!(response.count, 5);
            assert_eq!(response.kvs.len(), 5);
            let is_identical = response
                .kvs
                .iter()
                .zip(keys.iter())
                .all(|(kv, want)| kv.key == want.as_bytes());
            assert!(is_identical);
        }
        Ok(())
    }

    fn sort_req(sort_order: SortOrder, sort_target: SortTarget) -> RangeRequest {
        #[allow(clippy::as_conversions)] // this cast is always safe
        RangeRequest {
            key: vec![0],
            range_end: vec![0],
            sort_order: sort_order as i32,
            sort_target: sort_target as i32,
            ..Default::default()
        }
    }

    async fn init_store() -> Result<KvStore<DBProxy>, Box<dyn Error>> {
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let (_, del_rx) = mpsc::channel(128);
        let (lease_cmd_tx, mut lease_cmd_rx) = mpsc::channel(128);
        let _handle = tokio::spawn(async move {
            while let Some(LeaseMessage::GetLease(tx, _)) = lease_cmd_rx.recv().await {
                assert!(tx.send(0).is_ok());
            }
        });
        let kv_db = DBProxy::open(&StorageConfig::Memory)?;
        let store = KvStore::new(lease_cmd_tx, del_rx, header_gen, kv_db);
        let keys = vec!["a", "b", "c", "d", "e"];
        let vals = vec!["a", "b", "c", "d", "e"];
        for (key, val) in keys.into_iter().zip(vals.into_iter()) {
            let req = RequestWithToken::new(
                PutRequest {
                    key: key.into(),
                    value: val.into(),
                    ..Default::default()
                }
                .into(),
            );
            let id = ProposeId::new("test-id".to_owned());
            let _cmd_res = store.execute(id.clone(), req)?;
            let _sync_res = store.after_sync(id).await;
        }
        Ok(store)
    }
}
