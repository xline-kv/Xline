use std::{ops::Range, sync::Arc};

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
    /// Sender to send command
    request_tx: mpsc::Sender<ExecutionRequest>,
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
            index: Index::new(),
            db: DB::new(),
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

    #[allow(clippy::pattern_type_mismatch)]
    /// Handle `RangeRequest`
    fn handle_range_request(&self, req: RangeRequest) -> RangeResponse {
        let revision = self.revision.lock();

        let key = &req.key;
        let range_end = &req.range_end;
        let mut kvs = vec![];
        match range_end.as_slice() {
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
                let range = Range {
                    start: req.key,
                    end: req.range_end,
                };
                let revisions = self.index.get_range(range);
                let mut values = self.db.get_values(&revisions);
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
        let mut revision = self.revision.lock();
        *revision = (*revision).overflow_add(1);

        let new_rev = self.index.insert_or_update_revision(&req.key, *revision);

        let kv = KeyValue {
            key: req.key.clone(),
            value: req.value,
            create_revision: new_rev.create_revision,
            mod_revision: new_rev.mod_revision,
            version: new_rev.version,
            ..KeyValue::default()
        };
        let prev = self.db.insert(new_rev.as_revision(), kv);

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
        let mut revision = self.revision.lock();

        let key = &req.key;
        let range_end = &req.range_end;
        let mut response = DeleteRangeResponse::default();
        let prev_kvs = match range_end.as_slice() {
            ONE_KEY => {
                if let Some(rev) = self.index.delete_one(key, *revision) {
                    *revision = (*revision).overflow_add(1);
                    response.deleted = 1;
                    self.db.mark_deletions(&[rev])
                } else {
                    vec![]
                }
            }
            ALL_KEYS => {
                let revisions = self.index.delete_all(*revision);
                if !revisions.is_empty() {
                    *revision = (*revision).overflow_add(1);
                }
                self.db.mark_deletions(&revisions)
            }
            _ => {
                let range = Range {
                    start: req.key,
                    end: req.range_end,
                };
                let revisions = self.index.delete_range(*revision, range);
                if !revisions.is_empty() {
                    *revision = (*revision).overflow_add(1);
                }
                self.db.mark_deletions(&revisions)
            }
        };
        response.deleted = prev_kvs.len().cast();
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
