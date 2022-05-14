use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use clippy_utilities::Cast;
use log::debug;
use prost::Message;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::{Mutex, MutexGuard};

use crate::rpc::{
    DeleteRangeRequest, DeleteRangeResponse, KeyValue, PutRequest, PutResponse, RangeRequest,
    RangeResponse, Request, RequestOp, Response, ResponseOp, TxnRequest, TxnResponse,
};
use crate::server::ExecutionRequset;

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
    request_tx: Sender<ExecutionRequset>,
}

/// KV store inner
#[derive(Debug)]
struct KvStoreInner {
    /// B-tree map
    btree: Mutex<BTreeMap<Vec<u8>, KeyValue>>,
}

impl KvStore {
    /// New `KvStore`
    pub(crate) fn new() -> Self {
        let (request_tx, mut request_rx) = channel(CHANNEL_SIZE);
        let inner = Arc::new(KvStoreInner::new());
        let inner_clone = Arc::clone(&inner);
        let _handle = tokio::spawn(async move {
            let mut btree = inner.btree.lock().await;
            while let Some(req) = request_rx.recv().await {
                KvStoreInner::dispatch(&mut btree, req);
            }
        });

        Self {
            inner: inner_clone,
            request_tx,
        }
    }

    /// Send execution request to KV store
    pub(crate) async fn send_req(&self, req: ExecutionRequset) {
        if self.request_tx.send(req).await.is_err() {
            panic!("Command receiver dropped");
        }
    }
}

impl KvStoreInner {
    /// New `KvStoreInner`
    pub(crate) fn new() -> Self {
        Self {
            btree: Mutex::new(BTreeMap::new()),
        }
    }
    /// Dispatch and handle command
    pub(crate) fn dispatch(
        btree: &mut MutexGuard<BTreeMap<Vec<u8>, KeyValue>>,
        execution_req: ExecutionRequset,
    ) {
        debug!("Receive Execution Request {:?}", execution_req);
        let (cmd, notifier) = execution_req.unpack();
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
                    response: Some(Response::ResponseRange(Self::handle_range_request(
                        btree, req,
                    ))),
                }
            }
            Request::RequestPut(req) => {
                debug!("Receive PutRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponsePut(Self::handle_put_request(btree, req))),
                }
            }
            Request::RequestDeleteRange(req) => {
                debug!("Receive DeleteRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponseDeleteRange(
                        Self::handle_delete_range_request(btree, req),
                    )),
                }
            }
            Request::RequestTxn(req) => {
                debug!("Receive TxnRequest {:?}", req);
                ResponseOp {
                    response: Some(Response::ResponseTxn(Self::handle_txn_request(btree, &req))),
                }
            }
        };
        if notifier.send(response).is_err() {
            panic!("Failed to send response");
        }
    }

    #[allow(clippy::pattern_type_mismatch)]
    /// Handle `RangeRequest`
    fn handle_range_request(
        btree: &MutexGuard<BTreeMap<Vec<u8>, KeyValue>>,
        req: RangeRequest,
    ) -> RangeResponse {
        let key = &req.key;
        let range_end = &req.range_end;
        let mut kvs = vec![];
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(kv) = btree.get(key) {
                    kvs.push(kv.clone());
                }
            }
            ALL_KEYS => {
                //let mut values: Vec<KeyValue> = btree.values().map(Clone::clone).collect();
                let mut values: Vec<KeyValue> = btree.values().cloned().collect();
                kvs.append(&mut values);
            }
            _ => {
                let range = Range {
                    start: req.key,
                    end: req.range_end,
                };
                let mut values: Vec<KeyValue> =
                    btree.range(range).map(|(_k, v)| v.clone()).collect();
                kvs.append(&mut values);
            }
        }
        let mut response = RangeResponse {
            count: kvs.len().cast(),
            ..RangeResponse::default()
        };
        response.kvs = kvs;
        response
    }
    /// Handle `PutRequest`
    fn handle_put_request(
        btree: &mut MutexGuard<BTreeMap<Vec<u8>, KeyValue>>,
        req: PutRequest,
    ) -> PutResponse {
        let kv = KeyValue {
            key: req.key.clone(),
            value: req.value,
            ..KeyValue::default()
        };
        let prev = btree.insert(req.key, kv);
        let mut response = PutResponse::default();
        if req.prev_kv {
            response.prev_kv = prev;
        }
        response
    }

    #[allow(clippy::pattern_type_mismatch)]
    /// Handle `DeleteRangeRequest`
    fn handle_delete_range_request(
        btree: &mut MutexGuard<BTreeMap<Vec<u8>, KeyValue>>,
        req: DeleteRangeRequest,
    ) -> DeleteRangeResponse {
        let key = &req.key;
        let range_end = &req.range_end;
        let mut prev_kvs = vec![];
        let mut response = DeleteRangeResponse::default();
        match range_end.as_slice() {
            ONE_KEY => {
                if let Some(kv) = btree.remove(key) {
                    prev_kvs.push(kv);
                    response.deleted = 1;
                }
            }
            ALL_KEYS => {
                let mut kvs: Vec<KeyValue> = btree.values().map(Clone::clone).collect();
                response.deleted = kvs.len().cast();
                prev_kvs.append(&mut kvs);
                btree.clear();
            }
            _ => {
                let range = Range {
                    start: req.key,
                    end: req.range_end,
                };
                let keys: Vec<Vec<u8>> = btree.range(range).map(|(k, _v)| k.clone()).collect();
                response.deleted = keys.len().cast();
                for k in keys {
                    if let Some(kv) = btree.remove(&k) {
                        prev_kvs.push(kv);
                    }
                }
            }
        }
        if req.prev_kv {
            response.prev_kvs = prev_kvs;
        }
        response
    }
    /// Handle `TxnRequest`
    fn handle_txn_request(
        _btree: &MutexGuard<BTreeMap<Vec<u8>, KeyValue>>,
        _req: &TxnRequest,
    ) -> TxnResponse {
        TxnResponse::default()
    }
}

#[cfg(test)]
mod test {
    #[tokio::test(flavor = "multi_thread")]
    //#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_all() {}
}
