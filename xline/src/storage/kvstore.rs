use std::cmp::Ordering;
use std::{collections::HashMap, sync::Arc};

use clippy_utilities::NumericCast;
use clippy_utilities::OverflowArithmetic;
use curp::cmd::ProposeId;
use log::debug;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::{mpsc, oneshot};

use super::{db::DB, index::Index};
use crate::rpc::{
    Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse, KeyValue,
    PutRequest, PutResponse, RangeRequest, RangeResponse, Request, RequestOp, Response,
    ResponseHeader, ResponseOp, TargetUnion, TxnRequest, TxnResponse,
};
use crate::server::command::{
    Command, CommandResponse, ExecutionRequest, KeyRange, SyncRequest, SyncResponse,
};

/// Default channel size
const CHANNEL_SIZE: usize = 100;
/// Range end to get all keys
const ALL_KEYS: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];

/// KV store
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct KvStore {
    /// KV store inner
    inner: Arc<KvStoreInner>,
    /// TODO: check if this can be moved into Inner
    /// Sender to send command
    exec_tx: mpsc::Sender<ExecutionRequest>,
    /// Sender to send sync request
    sync_tx: mpsc::Sender<SyncRequest>,
}

/// KV store inner
#[derive(Debug)]
struct KvStoreInner {
    /// Key Index
    index: Index,
    /// DB to store key value
    db: DB,
    /// Revision
    revision: Mutex<i64>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<ProposeId, Vec<Request>>>,
}

impl KvStore {
    /// New `KvStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new() -> Self {
        let (exec_tx, mut exec_rx) = mpsc::channel(CHANNEL_SIZE);
        let (sync_tx, mut sync_rx) = mpsc::channel(CHANNEL_SIZE);
        let inner = Arc::new(KvStoreInner::new());
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
                            inner.sync_cmd(req);
                        }
                    }
                }
            }
        });

        Self {
            inner: inner_clone,
            exec_tx,
            sync_tx,
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
}

impl KvStoreInner {
    /// New `KvStoreInner`
    pub(crate) fn new() -> Self {
        Self {
            index: Index::new(),
            db: DB::new(),
            revision: Mutex::new(1),
            sp_exec_pool: Mutex::new(HashMap::new()),
        }
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
    fn get_range(&self, key: &[u8], range_end: &[u8]) -> Vec<KeyValue> {
        let mut kvs = vec![];
        match range_end {
            ONE_KEY => {
                if let Some(index) = self.index.get_one(key) {
                    if let Some(kv) = self.db.get(&index) {
                        kvs.push(kv);
                    }
                }
            }
            ALL_KEYS => {
                let revisions = self.index.get_all();
                let mut values = self.db.get_values(&revisions);
                kvs.append(&mut values);
            }
            _ => {
                let range = KeyRange {
                    start: key.to_vec(),
                    end: range_end.to_vec(),
                };
                let revisions = self.index.get_range(range);
                let mut values = self.db.get_values(&revisions);
                kvs.append(&mut values);
            }
        }
        kvs
    }

    /// Handle `RangeRequest`
    fn handle_range_request(&self, req: &RangeRequest) -> RangeResponse {
        //let revision = self.revision.lock();

        let key = &req.key;
        let range_end = &req.range_end;
        let kvs = self.get_range(key, range_end);
        debug!("handle_range_request kvs {:?}", kvs);
        let mut response = RangeResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            count: kvs.len().numeric_cast(),
            ..RangeResponse::default()
        };
        response.kvs = kvs;
        response
    }

    /// Handle `PutRequest`
    fn handle_put_request(&self, req: &PutRequest) -> PutResponse {
        let mut prev_kvs = self.get_range(&req.key, &[]);
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
        let prev_kvs = self.get_range(&req.key, &req.range_end);
        debug!("handle_delete_range_request prev_kvs {:?}", prev_kvs);
        response.deleted = prev_kvs.len().numeric_cast();
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
        let result = match CompareTarget::from_i32(cmp.target) {
            Some(CompareTarget::Version) => {
                let rev = if let Some(TargetUnion::Version(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.version, rev)
            }
            Some(CompareTarget::Create) => {
                let rev = if let Some(TargetUnion::CreateRevision(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.create_revision, rev)
            }
            Some(CompareTarget::Mod) => {
                let rev = if let Some(TargetUnion::ModRevision(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.mod_revision, rev)
            }
            Some(CompareTarget::Value) => {
                let empty = vec![];
                let val = if let Some(TargetUnion::Value(ref v)) = cmp.target_union {
                    v
                } else {
                    &empty
                };
                Self::compare_vec_u8(&kv.value, val)
            }
            Some(CompareTarget::Lease) => {
                let les = if let Some(TargetUnion::Lease(v)) = cmp.target_union {
                    v
                } else {
                    0
                };
                Self::compare_i64(kv.mod_revision, les)
            }
            None => panic!("Invalid CompareTarget {:?}", cmp.target),
        };

        match CompareResult::from_i32(cmp.result) {
            Some(CompareResult::Equal) => result == CompareResult::Equal,
            Some(CompareResult::Greater) => result == CompareResult::Greater,
            Some(CompareResult::Less) => result == CompareResult::Less,
            Some(CompareResult::NotEqual) => result != CompareResult::Equal,
            None => panic!("Invalid CompareResult {:?}", cmp.result),
        }
    }

    /// Check result of a `Compare`
    fn check_compare(&self, cmp: &Compare) -> bool {
        let kvs = self.get_range(&cmp.key, &cmp.range_end);
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
    fn sync_cmd(&self, sync_req: SyncRequest) {
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
        let revision = self.sync_requests(requests);
        assert!(
            res_sender.send(SyncResponse::new(revision)).is_ok(),
            "Failed to send response"
        );
    }

    /// Sync a vec of requests
    fn sync_requests(&self, requests: Vec<Request>) -> i64 {
        let revision = *self.revision.lock();
        let next_revision = revision.overflow_add(1);
        let mut sub_revision = 0;
        let mut modify = false;

        for request in requests {
            let res = self.sync_request(request, next_revision, &mut sub_revision);
            modify = modify || res;
        }

        if modify {
            *self.revision.lock() = next_revision;
            next_revision
        } else {
            revision
        }
    }

    /// Sync one `Request`
    fn sync_request(&self, req: Request, revision: i64, sub_revision: &mut i64) -> bool {
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
    fn sync_range_request(_req: &RangeRequest) -> bool {
        false
    }

    /// Sync `PutRequest` and return if kvstore is changed
    fn sync_put_request(&self, req: PutRequest, revision: i64, sub_revision: &mut i64) -> bool {
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
        let _prev = self.db.insert(new_rev.as_revision(), kv);
        true
    }

    /// Sync `DeleteRangeRequest` and return if kvstore is changed
    fn sync_delete_range_request(
        &self,
        req: DeleteRangeRequest,
        revision: i64,
        sub_revision: &mut i64,
    ) -> bool {
        let key = &req.key;
        let range_end = &req.range_end;
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(rev) = self.index.delete_one(key, revision, sub_revision) {
                    debug!("sync_delete_range_request delete one: revisions {:?}", rev);
                    let _kv = self.db.mark_deletions(&[rev]);
                    true
                } else {
                    false
                }
            }
            ALL_KEYS => {
                let revisions = self.index.delete_all(revision, sub_revision);
                debug!(
                    "sync_delete_range_request delete all: revisions {:?}",
                    revisions
                );
                let _kv = self.db.mark_deletions(&revisions);
                !revisions.is_empty()
            }
            _ => {
                let range = KeyRange {
                    start: req.key,
                    end: req.range_end,
                };
                let revisions = self.index.delete_range(range, revision, sub_revision);
                debug!(
                    "sync_delete_range_request delete range: revisions {:?}",
                    revisions
                );
                let _kv = self.db.mark_deletions(&revisions);
                !revisions.is_empty()
            }
        }
    }
}

#[cfg(test)]
mod test {
    #[tokio::test(flavor = "multi_thread")]
    //#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_all() {}
}
