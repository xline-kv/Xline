use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use clippy_utilities::{Cast, OverflowArithmetic};
use curp::cmd::ProposeId;
use log::debug;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::{mpsc, oneshot};

use super::index::IndexOperate;
use super::{db::DB, index::Index, kvwatcher::KvWatcher};
use crate::rpc::{
    Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse, Event,
    EventType, KeyValue, PutRequest, PutResponse, RangeRequest, RangeResponse, Request, RequestOp,
    Response, ResponseHeader, ResponseOp, SortOrder, SortTarget, TargetUnion, TxnRequest,
    TxnResponse,
};
use crate::server::command::{
    Command, CommandResponse, ExecutionRequest, KeyRange, SyncRequest, SyncResponse,
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

/// KV store
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct KvStore {
    /// KV store Backend
    inner: Arc<KvStoreBackend>,
    /// TODO: check if this can be moved into Inner
    /// Sender to send command
    exec_tx: mpsc::Sender<ExecutionRequest>,
    /// Sender to send sync request
    sync_tx: mpsc::Sender<SyncRequest>,
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
    revision: Mutex<i64>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<ProposeId, Vec<Request>>>,
    /// KV update sender
    kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>,
}

impl KvStore {
    /// New `KvStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new() -> Self {
        let (exec_tx, mut exec_rx) = mpsc::channel(CHANNEL_SIZE);
        let (sync_tx, mut sync_rx) = mpsc::channel(CHANNEL_SIZE);
        let (kv_update_tx, kv_update_rx) = mpsc::channel(CHANNEL_SIZE);
        let inner = Arc::new(KvStoreBackend::new(kv_update_tx));
        let kv_watcher = Arc::new(KvWatcher::new(Arc::clone(&inner), kv_update_rx));
        let inner_clone = Arc::clone(&inner);
        let _handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    cmd_req = exec_rx.recv() => {
                        if let Some(req) = cmd_req {
                            inner.speculative_exec(req);
                        }
                    }
                    sync_req = sync_rx.recv() => {
                        if let Some(req) = sync_req {
                            inner.sync_cmd(req).await;
                        }
                    }
                }
            }
        });

        Self {
            inner: inner_clone,
            exec_tx,
            sync_tx,
            kv_watcher,
        }
    }

    /// Send execution request to KV store
    pub(crate) async fn send_req(&self, cmd: Command) -> oneshot::Receiver<CommandResponse> {
        let (req, receiver) = ExecutionRequest::new(cmd);
        assert!(
            self.exec_tx.send(req).await.is_ok(),
            "Command receiver dropped"
        );
        receiver
    }

    /// Send sync request to KV store
    pub(crate) async fn send_sync(&self, propose_id: ProposeId) -> oneshot::Receiver<SyncResponse> {
        let (req, receiver) = SyncRequest::new(propose_id);
        assert!(
            self.sync_tx.send(req).await.is_ok(),
            "Command receiver dropped"
        );
        receiver
    }

    /// Get KV watcher
    pub(crate) fn kv_watcher(&self) -> Arc<KvWatcher> {
        Arc::clone(&self.kv_watcher)
    }
}

impl KvStoreBackend {
    /// New `KvStoreBackend`
    pub(crate) fn new(kv_update_tx: mpsc::Sender<(i64, Vec<Event>)>) -> Self {
        Self {
            index: Index::new(),
            db: DB::new(),
            revision: Mutex::new(1),
            sp_exec_pool: Mutex::new(HashMap::new()),
            kv_update_tx,
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

    /// speculative execute command
    pub(crate) fn speculative_exec(&self, execution_req: ExecutionRequest) {
        debug!("Receive Execution Request {:?}", execution_req);
        let (cmd, res_sender) = execution_req.unpack();
        let (key, request_data, id) = cmd.unpack();
        let request_op = RequestOp::decode(request_data.as_slice()).unwrap_or_else(|e| {
            panic!(
                "Failed to decode request, key is {:?}, error is {:?}",
                key, e
            )
        });
        let response = self.handle_request_op(&id, request_op);
        assert!(
            res_sender.send(CommandResponse::new(&response)).is_ok(),
            "Failed to send response"
        );
    }

    /// Handle `RequestOp`
    fn handle_request_op(&self, id: &ProposeId, request_op: RequestOp) -> ResponseOp {
        let request = request_op
            .request
            .unwrap_or_else(|| panic!("Received empty request"));
        debug!("Receive request {:?}", request);
        let (response, is_txn) = match request {
            Request::RequestRange(ref req) => {
                debug!("Receive RangeRequest {:?}", req);
                (
                    ResponseOp {
                        response: Some(Response::ResponseRange(self.handle_range_request(req))),
                    },
                    false,
                )
            }
            Request::RequestPut(ref req) => {
                debug!("Receive PutRequest {:?}", req);
                (
                    ResponseOp {
                        response: Some(Response::ResponsePut(self.handle_put_request(req))),
                    },
                    false,
                )
            }
            Request::RequestDeleteRange(ref req) => {
                debug!("Receive DeleteRequest {:?}", req);
                (
                    ResponseOp {
                        response: Some(Response::ResponseDeleteRange(
                            self.handle_delete_range_request(req),
                        )),
                    },
                    false,
                )
            }
            Request::RequestTxn(ref req) => {
                debug!("Receive TxnRequest {:?}", req);
                (
                    ResponseOp {
                        response: Some(Response::ResponseTxn(self.handle_txn_request(id, req))),
                    },
                    true,
                )
            }
        };
        if is_txn {
            let _prev = self.sp_exec_pool.lock().entry(id.clone()).or_insert(vec![]);
        } else {
            let _prev = self
                .sp_exec_pool
                .lock()
                .entry(id.clone())
                .and_modify(|req| req.push(request.clone()))
                .or_insert_with(|| vec![request.clone()]);
        }
        response
    }

    /// Get `KeyValue` of a range
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
        let key = &req.key;
        let range_end = &req.range_end;
        let mut kvs = self.get_range(key, range_end, req.revision);
        debug!("handle_range_request kvs {:?}", kvs);
        let mut response = RangeResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
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
    fn handle_put_request(&self, req: &PutRequest) -> PutResponse {
        let mut prev_kvs = self.get_range(&req.key, &[], 0);
        debug!("handle_put_request prev_kvs {:?}", prev_kvs);
        let prev = if prev_kvs.len() == 1 {
            Some(prev_kvs.swap_remove(0))
        } else if prev_kvs.is_empty() {
            None
        } else {
            panic!(
                "Get more than one KeyValue {:?} for req {:?}",
                prev_kvs, req
            );
        };
        let mut response = PutResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            ..PutResponse::default()
        };
        if req.prev_kv {
            response.prev_kv = prev;
        }
        response
    }

    /// Handle `DeleteRangeRequest`
    fn handle_delete_range_request(&self, req: &DeleteRangeRequest) -> DeleteRangeResponse {
        let mut response = DeleteRangeResponse::default();
        let prev_kvs = self.get_range(&req.key, &req.range_end, 0);
        debug!("handle_delete_range_request prev_kvs {:?}", prev_kvs);
        response.deleted = prev_kvs.len().cast();
        if req.prev_kv {
            response.prev_kvs = prev_kvs;
        }
        response.header = Some(ResponseHeader {
            revision: -1,
            ..ResponseHeader::default()
        });
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
    fn handle_txn_request(&self, id: &ProposeId, req: &TxnRequest) -> TxnResponse {
        let success = req
            .compare
            .iter()
            .all(|compare| self.check_compare(compare));

        let responses = if success {
            req.success
                .iter()
                .map(|op| self.handle_request_op(id, op.clone()))
                .collect()
        } else {
            req.failure
                .iter()
                .map(|op| self.handle_request_op(id, op.clone()))
                .collect()
        };
        TxnResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            succeeded: success,
            responses,
        }
    }

    /// Sync a Command to storage and generate revision for Command.
    async fn sync_cmd(&self, sync_req: SyncRequest) {
        debug!("Receive SyncRequest {:?}", sync_req);
        let (propose_id, res_sender) = sync_req.unpack();
        let requests = self
            .sp_exec_pool
            .lock()
            .remove(&propose_id)
            .unwrap_or_else(|| {
                panic!(
                    "Failed to get speculative execution propose id {:?}",
                    propose_id
                );
            });
        let (revision, events) = self.sync_requests(requests.clone());
        assert!(
            res_sender.send(SyncResponse::new(revision)).is_ok(),
            "Failed to send response"
        );
        if let Some(events) = events {
            self.notify_updates(revision, events).await;
        }
    }

    /// Sync a vec of requests
    fn sync_requests(&self, requests: Vec<Request>) -> (i64, Option<Vec<Event>>) {
        let revision = *self.revision.lock();
        let next_revision = revision.overflow_add(1);
        let mut sub_revision = 0;
        let mut modify = false;
        let mut all_events = vec![];

        for request in requests {
            let mut events = self.sync_request(request, next_revision, sub_revision);
            modify = modify || !events.is_empty();
            sub_revision = sub_revision.overflow_add(events.len().cast());
            all_events.append(&mut events);
        }

        if modify {
            *self.revision.lock() = next_revision;
            (next_revision, Some(all_events))
        } else {
            (revision, None)
        }
    }

    /// Sync one `Request`
    fn sync_request(&self, req: Request, revision: i64, sub_revision: i64) -> Vec<Event> {
        match req {
            Request::RequestRange(req) => {
                debug!("Sync RequestRange {:?}", req);
                Self::sync_range_request(&req)
            }
            Request::RequestPut(req) => {
                debug!("Sync RequestPut {:?}", req);
                self.sync_put_request(req, revision, sub_revision)
            }
            Request::RequestDeleteRange(req) => {
                debug!("Receive DeleteRequest {:?}", req);
                self.sync_delete_range_request(req, revision, sub_revision)
            }
            Request::RequestTxn(req) => {
                debug!("Receive TxnRequest {:?}", req);
                panic!("Sync for TxnRequest is impossible");
            }
        }
    }

    /// Sync `RangeRequest` and return of kvstore is changed
    fn sync_range_request(_req: &RangeRequest) -> Vec<Event> {
        Vec::new()
    }

    /// Sync `PutRequest` and return if kvstore is changed
    fn sync_put_request(&self, req: PutRequest, revision: i64, sub_revision: i64) -> Vec<Event> {
        let new_rev = self
            .index
            .insert_or_update_revision(&req.key, revision, sub_revision);

        let kv = KeyValue {
            key: req.key,
            value: req.value,
            create_revision: new_rev.create_revision,
            mod_revision: new_rev.mod_revision,
            version: new_rev.version,
            ..KeyValue::default()
        };
        let prev = self.db.insert(new_rev.as_revision(), kv.clone());
        let event = Event {
            #[allow(clippy::as_conversions)] // This cast is always valid
            r#type: EventType::Put as i32,
            kv: Some(kv),
            prev_kv: prev,
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
        Self::new_deletion_events(revision, prev_kv)
    }
}

#[cfg(test)]
mod test {
    #[tokio::test(flavor = "multi_thread")]
    //#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_all() {}
}
