use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use clippy_utilities::Cast;
use log::debug;
use prost::Message;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
//use tokio::sync::{Mutex, MutexGuard};
use clippy_utilities::OverflowArithmetic;
use parking_lot::Mutex;

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
    /// Sender to send command
    request_tx: mpsc::Sender<ExecutionRequest>,
}

/// KV store inner
#[derive(Debug)]
struct KvStoreInner {
    /// Key Index
    index: Mutex<BTreeMap<Vec<u8>, Vec<KeyRevision>>>,
    /// DB to store key value
    db: Mutex<HashMap<Revision, KeyValue>>,
    /// Revision
    revision: Mutex<i64>,
}

/// Revison of a key
#[derive(Debug, Copy, Clone)]
struct KeyRevision {
    /// Last creation revision
    create_revision: i64,
    /// Number of modification since last creation
    version: i64,
    /// Last modification revision
    mod_revision: i64,
    /// Sub revision in one transaction
    sub_revision: i64,
}

/// Revision
#[derive(Debug, Eq, Hash, PartialEq)]
struct Revision {
    /// Main revision
    revision: i64,
    /// Sub revision in one transaction
    sub_revision: i64,
}
impl Revision {
    /// New `Revision`
    fn new(revision: i64, sub_revision: i64) -> Self {
        Self {
            revision,
            sub_revision,
        }
    }
}

impl KeyRevision {
    /// New `Revision`
    fn new(create_revision: i64, version: i64, mod_revision: i64, sub_revision: i64) -> Self {
        Self {
            create_revision,
            version,
            mod_revision,
            sub_revision,
        }
    }
    /// New a revision to represent deletion
    fn new_delete(mod_revision: i64) -> Self {
        Self {
            create_revision: 0,
            version: 0,
            mod_revision,
            sub_revision: 0,
        }
    }

    /// If current revision represent deletion
    fn is_deleted(&self) -> bool {
        self.create_revision == 0 && self.version == 0 && self.sub_revision == 0
    }

    /// Create `Revision`
    fn as_revision(&self) -> Revision {
        Revision::new(self.mod_revision, self.sub_revision)
    }
}

/// Execution Request
#[derive(Debug)]
struct ExecutionRequest {
    /// Command to execute
    cmd: Command,
    /// Command request sender
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

impl KvStore {
    /// New `KvStore`
    pub(crate) fn new() -> Self {
        let (request_tx, mut request_rx) = mpsc::channel(CHANNEL_SIZE);
        let inner = Arc::new(KvStoreInner::new());
        let inner_clone = Arc::clone(&inner);
        let _handle = tokio::spawn(async move {
            while let Some(req) = request_rx.recv().await {
                inner.dispatch(req);
                debug!("Current KvStoreInner {:?}", inner);
            }
        });

        Self {
            inner: inner_clone,
            request_tx,
        }
    }

    /// Send execution request to KV store
    pub(crate) async fn send_req(&self, cmd: Command) -> oneshot::Receiver<CommandResponse> {
        let (req, receiver) = ExecutionRequest::new(cmd);
        assert!(
            self.request_tx.send(req).await.is_ok(),
            "Command receiver dropped"
        );
        receiver
    }
}

impl KvStoreInner {
    /// New `KvStoreInner`
    pub(crate) fn new() -> Self {
        Self {
            index: Mutex::new(BTreeMap::new()),
            db: Mutex::new(HashMap::new()),
            revision: Mutex::new(1),
        }
    }
    /// Dispatch and handle command
    pub(crate) fn dispatch(&self, execution_req: ExecutionRequest) {
        debug!("Receive Execution Request {:?}", execution_req);
        let (cmd, res_sender) = execution_req.unpack();
        let (key, request_data) = cmd.unpack();
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
        let response = match request {
            Request::RequestRange(req) => {
                debug!("Receive RangeRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponseRange(self.handle_range_request(req))),
                }
            }
            Request::RequestPut(req) => {
                debug!("Receive PutRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponsePut(self.handle_put_request(req))),
                }
            }
            Request::RequestDeleteRange(req) => {
                debug!("Receive DeleteRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponseDeleteRange(
                        self.handle_delete_range_request(req),
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

    /// Get a `KeyRevision` if the key is not deleted
    fn get_index(indexes: &[KeyRevision]) -> Option<KeyRevision> {
        indexes.last().filter(|index| !index.is_deleted()).copied()
    }

    #[allow(clippy::pattern_type_mismatch)]
    /// Handle `RangeRequest`
    fn handle_range_request(&self, req: RangeRequest) -> RangeResponse {
        let key_index = self.index.lock();
        let db = self.db.lock();
        let revision = self.revision.lock();

        let key = &req.key;
        let range_end = &req.range_end;
        let mut kvs = vec![];
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(indexes) = key_index.get(key) {
                    if let Some(index) = Self::get_index(indexes) {
                        if let Some(kv) = db.get(&index.as_revision()) {
                            kvs.push(kv.clone());
                        }
                    }
                }
            }
            ALL_KEYS => {
                //let mut values: Vec<KeyValue> = btree.values().map(Clone::clone).collect();
                let mut values: Vec<KeyValue> = key_index
                    .values()
                    .filter_map(|indexes| {
                        Self::get_index(indexes)
                            .and_then(|index| db.get(&index.as_revision()).cloned())
                    })
                    .collect();
                kvs.append(&mut values);
            }
            _ => {
                let range = Range {
                    start: req.key,
                    end: req.range_end,
                };
                let mut values: Vec<KeyValue> = key_index
                    .range(range)
                    .filter_map(|(_k, indexes)| {
                        Self::get_index(indexes)
                            .and_then(|index| db.get(&index.as_revision()).cloned())
                    })
                    .collect();
                kvs.append(&mut values);
            }
        }
        let mut response = RangeResponse {
            header: Some(ResponseHeader {
                revision: *revision,
                ..ResponseHeader::default()
            }),
            count: kvs.len().cast(),
            ..RangeResponse::default()
        };
        response.kvs = kvs;
        response
    }

    /// Handle `PutRequest`
    fn handle_put_request(&self, req: PutRequest) -> PutResponse {
        let mut key_index = self.index.lock();
        let mut db = self.db.lock();
        let mut revision = self.revision.lock();
        *revision = (*revision).overflow_add(1);

        let key_revision = if let Some(indexes) = key_index.get_mut(&req.key) {
            let kv_rev = if let Some(index) = Self::get_index(indexes) {
                KeyRevision::new(
                    index.create_revision,
                    index.version.overflow_add(1),
                    *revision,
                    0,
                )
            } else {
                KeyRevision::new(*revision, 1, *revision, 0)
            };
            indexes.push(kv_rev);
            kv_rev
        } else {
            let kv_rev = KeyRevision::new(*revision, 1, *revision, 0);
            let _idx = key_index.insert(req.key.clone(), vec![kv_rev]);
            kv_rev
        };
        let kv = KeyValue {
            key: req.key.clone(),
            value: req.value,
            create_revision: key_revision.create_revision,
            mod_revision: key_revision.mod_revision,
            version: key_revision.version,
            ..KeyValue::default()
        };
        let prev = db.insert(key_revision.as_revision(), kv);

        let mut response = PutResponse {
            header: Some(ResponseHeader {
                revision: *revision,
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
    fn handle_delete_range_request(&self, req: DeleteRangeRequest) -> DeleteRangeResponse {
        let mut key_index = self.index.lock();
        let db = self.db.lock();
        let mut revision = self.revision.lock();

        let key = &req.key;
        let range_end = &req.range_end;
        let mut prev_kvs = vec![];
        let mut response = DeleteRangeResponse::default();
        let del_revision = KeyRevision::new_delete((*revision).overflow_add(1));
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(indexes) = key_index.get_mut(key) {
                    if let Some(index) = Self::get_index(indexes) {
                        let prev_kv = db.get(&index.as_revision()).cloned().unwrap_or_else(|| {
                            panic!("Failed to get key {:?} index {:?} from DB", key, index)
                        });
                        indexes.push(del_revision);
                        *revision = (*revision).overflow_add(1);
                        prev_kvs.push(prev_kv);
                        response.deleted = 1;
                    }
                }
            }
            ALL_KEYS => {
                let mut kvs: Vec<KeyValue> = key_index
                    .values_mut()
                    .filter_map(|indexes| {
                        if let Some(kv_rev) = Self::get_index(indexes) {
                            indexes.push(del_revision);
                            db.get(&kv_rev.as_revision()).cloned()
                        } else {
                            None
                        }
                    })
                    .collect();
                if !kvs.is_empty() {
                    *revision = (*revision).overflow_add(1);
                }
                response.deleted = kvs.len().cast();
                prev_kvs.append(&mut kvs);
            }
            _ => {
                let range = Range {
                    start: req.key,
                    end: req.range_end,
                };
                let mut kvs: Vec<KeyValue> = key_index
                    .range_mut(range)
                    .filter_map(|(_k, indexes)| {
                        if let Some(kv_rev) = Self::get_index(indexes) {
                            indexes.push(del_revision);
                            db.get(&kv_rev.as_revision()).cloned()
                        } else {
                            None
                        }
                    })
                    .collect();
                if !kvs.is_empty() {
                    *revision = (*revision).overflow_add(1);
                }
                prev_kvs.append(&mut kvs);
                response.deleted = kvs.len().cast();
            }
        }
        if req.prev_kv {
            response.prev_kvs = prev_kvs;
        }
        response.header = Some(ResponseHeader {
            revision: *revision,
            ..ResponseHeader::default()
        });
        response
    }
    /// Handle `TxnRequest`
    fn handle_txn_request(_req: &TxnRequest) -> TxnResponse {
        TxnResponse::default()
    }
}

#[cfg(test)]
mod test {
    #[tokio::test(flavor = "multi_thread")]
    //#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_all() {}
}
