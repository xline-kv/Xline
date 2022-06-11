use std::{collections::HashMap, ops::Range, sync::Arc};

use clippy_utilities::Cast;
use clippy_utilities::OverflowArithmetic;
use log::debug;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::{mpsc, oneshot};

use super::{db::DB, index::Index};
use crate::rpc::{
    DeleteRangeRequest, DeleteRangeResponse, KeyValue, PutRequest, PutResponse, RangeRequest,
    RangeResponse, Request, RequestOp, Response, ResponseHeader, ResponseOp, TxnRequest,
    TxnResponse,
};
use crate::server::{Command, CommandResponse};

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
    /// Mapping from propose id to sender of revision notifier
    rev_notifier: Mutex<HashMap<String, oneshot::Sender<i64>>>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<String, Request>>,
}

/// Execution Request
#[derive(Debug)]
struct ExecutionRequest {
    /// Command to execute
    cmd: Command,
    /// Command response sender
    res_sender: oneshot::Sender<CommandResponse>,
}

impl ExecutionRequest {
    /// New `ExectionRequest`
    fn new(cmd: Command) -> (Self, oneshot::Receiver<CommandResponse>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd,
                res_sender: tx,
            },
            rx,
        )
    }

    /// Consume `ExecutionRequest` and get ownership of each field
    fn unpack(self) -> (Command, oneshot::Sender<CommandResponse>) {
        let Self { cmd, res_sender } = self;
        (cmd, res_sender)
    }
}

/// Sync Request
#[derive(Debug)]
pub(crate) struct SyncRequest {
    /// Propose id to sync
    propose_id: String,
    /// Command response sender
    res_sender: oneshot::Sender<SyncResponse>,
}

impl SyncRequest {
    /// New `SyncRequest`
    #[allow(dead_code)]
    fn new(propose_id: String) -> (Self, oneshot::Receiver<SyncResponse>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                propose_id,
                res_sender: tx,
            },
            rx,
        )
    }

    /// Consume `ExecutionRequest` and get ownership of each field
    fn unpack(self) -> (String, oneshot::Sender<SyncResponse>) {
        let Self {
            propose_id,
            res_sender,
        } = self;
        (propose_id, res_sender)
    }
}

/// Sync Response
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct SyncResponse {
    /// Revision of this request
    revision: i64,
}
impl SyncResponse {
    /// New `SyncRequest`
    fn new(revision: i64) -> Self {
        Self { revision }
    }
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

    /// Register notifier for one `Command`
    /// Client will be notified when the revision of a command is confirmed
    pub(crate) fn register_rev_notifier(&self, propose_id: String) -> oneshot::Receiver<i64> {
        self.inner.register_rev_notifier(propose_id)
    }

    /// Send sync request to KV store
    pub(crate) async fn send_sync(&self, propose_id: String) -> oneshot::Receiver<SyncResponse> {
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
            rev_notifier: Mutex::new(HashMap::new()),
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
        let request = request_op
            .request
            .unwrap_or_else(|| panic!("Received empty request, key is {:?}", key));
        debug!("Receive request {:?}", request);
        let _prev = self.sp_exec_pool.lock().insert(id, request.clone());
        let response = match request {
            Request::RequestRange(req) => {
                debug!("Receive RangeRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponseRange(self.handle_range_request(&req))),
                }
            }
            Request::RequestPut(req) => {
                debug!("Receive PutRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponsePut(self.handle_put_request(&req))),
                }
            }
            Request::RequestDeleteRange(req) => {
                debug!("Receive DeleteRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponseDeleteRange(
                        self.handle_delete_range_request(&req),
                    )),
                }
            }
            Request::RequestTxn(req) => {
                debug!("Receive TxnRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponseTxn(Self::handle_txn_request(&req))),
                }
            }
        };
        assert!(
            res_sender.send(CommandResponse::new(&response)).is_ok(),
            "Failed to send response"
        );
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
                // TODO: reduce clone
                let range = Range {
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

    #[allow(clippy::pattern_type_mismatch)]
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
            count: kvs.len().cast(),
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

    #[allow(clippy::pattern_type_mismatch)]
    /// Handle `DeleteRangeRequest`
    fn handle_delete_range_request(&self, req: &DeleteRangeRequest) -> DeleteRangeResponse {
        let mut response = DeleteRangeResponse::default();
        let prev_kvs = self.get_range(&req.key, &req.range_end);
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
    /// Handle `TxnRequest`
    fn handle_txn_request(_req: &TxnRequest) -> TxnResponse {
        TxnResponse::default()
    }

    /// Sync a Command to storage.
    fn sync_cmd(&self, sync_req: SyncRequest) {
        debug!("Receive SyncRequest {:?}", sync_req);
        let (propose_id, res_sender) = sync_req.unpack();
        let request = self
            .sp_exec_pool
            .lock()
            .remove(&propose_id)
            .unwrap_or_else(|| {
                panic!(
                    "Failed to get speculative execution propose id {:?}",
                    propose_id
                );
            });
        let revision = match request {
            Request::RequestRange(req) => {
                debug!("Sync RequestRange {:?}", req);
                self.sync_range_request(&req)
            }
            Request::RequestPut(req) => {
                debug!("Sync RequestPut {:?}", req);
                self.sync_put_request(req)
            }
            Request::RequestDeleteRange(req) => {
                debug!("Receive DeleteRequest {:?}", req);
                self.sync_delete_range_request(req)
            }
            Request::RequestTxn(req) => {
                debug!("Receive TxnRequest {:?}", req);
                panic!("Unsupported")
            }
        };
        assert!(
            res_sender.send(SyncResponse::new(revision)).is_ok(),
            "Failed to send response"
        );
        self.notify_revision(&propose_id, revision);
    }

    /// Sync range request
    fn sync_range_request(&self, _req: &RangeRequest) -> i64 {
        *self.revision.lock()
    }

    /// Sync put request
    fn sync_put_request(&self, req: PutRequest) -> i64 {
        let mut revision = self.revision.lock();
        *revision = (*revision).overflow_add(1);

        let new_rev = self.index.insert_or_update_revision(&req.key, *revision);

        let kv = KeyValue {
            key: req.key,
            value: req.value,
            create_revision: new_rev.create_revision,
            mod_revision: new_rev.mod_revision,
            version: new_rev.version,
            ..KeyValue::default()
        };
        let _prev = self.db.insert(new_rev.as_revision(), kv);
        *revision
    }

    /// Sync put request
    fn sync_delete_range_request(&self, req: DeleteRangeRequest) -> i64 {
        let mut revision = self.revision.lock();

        let key = &req.key;
        let range_end = &req.range_end;
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(rev) = self.index.delete_one(key, *revision) {
                    debug!("sync_delete_range_request delete one: revisions {:?}", rev);
                    *revision = (*revision).overflow_add(1);
                    let _kv = self.db.mark_deletions(&[rev]);
                }
            }
            ALL_KEYS => {
                let revisions = self.index.delete_all(*revision);
                debug!(
                    "sync_delete_range_request delete all: revisions {:?}",
                    revisions
                );
                if !revisions.is_empty() {
                    *revision = (*revision).overflow_add(1);
                }
                let _kv = self.db.mark_deletions(&revisions);
            }
            _ => {
                let range = Range {
                    start: req.key,
                    end: req.range_end,
                };
                let revisions = self.index.delete_range(*revision, range);
                debug!(
                    "sync_delete_range_request delete range: revisions {:?}",
                    revisions
                );
                if !revisions.is_empty() {
                    *revision = (*revision).overflow_add(1);
                }
                let _kv = self.db.mark_deletions(&revisions);
            }
        };
        *revision
    }

    /// Register notifier for one proposal
    /// Client will be notified when the revision of a command is confirmed
    pub(crate) fn register_rev_notifier(&self, propose_id: String) -> oneshot::Receiver<i64> {
        let (tx, rx) = oneshot::channel();
        let _prev = self.rev_notifier.lock().insert(propose_id, tx);
        rx
    }

    /// Notify revision of one proposal
    pub(crate) fn notify_revision(&self, propose_id: &String, revision: i64) {
        if let Some(sender) = self.rev_notifier.lock().remove(propose_id) {
            assert!(
                sender.send(revision).is_ok(),
                "Failed to notify revision of propose id {:?}",
                propose_id
            );
        }
    }
}

#[cfg(test)]
mod test {
    #[tokio::test(flavor = "multi_thread")]
    //#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_all() {}
}
