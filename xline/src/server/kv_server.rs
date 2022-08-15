use std::sync::Arc;

use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use log::debug;
use prost::Message;
use uuid::Uuid;

use super::command::{Command, CommandResponse, KeyRange, SyncResponse};
use crate::{
    rpc::{
        CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, Kv,
        PutRequest, PutResponse, RangeRequest, RangeResponse, Request, RequestOp, Response,
        ResponseOp, TxnRequest, TxnResponse,
    },
    storage::KvStore,
};

/// KV Server
#[derive(Debug)]
#[allow(dead_code)] // Remove this after feature is completed
pub(crate) struct KvServer {
    /// KV storage
    storage: Arc<KvStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
}

impl KvServer {
    /// New `KvServer`
    pub(crate) fn new(storage: Arc<KvStore>, client: Arc<Client<Command>>, name: String) -> Self {
        Self {
            storage,
            client,
            name,
        }
    }
    /// Parse `ResponseOp`
    pub(crate) fn parse_response_op(response_op: ResponseOp) -> Response {
        if let Some(response) = response_op.response {
            response
        } else {
            panic!("Receive empty ResponseOp");
        }
    }

    /// Generate `Command` proposal from `Request`
    fn command_from_request(propose_id: ProposeId, request: Request) -> Command {
        let key_ranges = match request {
            Request::RequestRange(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            }],
            Request::RequestPut(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: vec![],
            }],
            Request::RequestDeleteRange(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            }],
            Request::RequestTxn(ref req) => req
                .compare
                .iter()
                .map(|cmp| KeyRange {
                    start: cmp.key.clone(),
                    end: cmp.range_end.clone(),
                })
                .collect(),
        };
        let range_request_op = RequestOp {
            request: Some(request),
        };
        Command::new(key_ranges, range_request_op.encode_to_vec(), propose_id)
    }

    /// Propose request and get result with slow path
    async fn propose_slow_path(
        &self,
        propose_id: ProposeId,
        request: Request,
    ) -> Result<(CommandResponse, SyncResponse), ProposeError> {
        let cmd = Self::command_from_request(propose_id, request);
        self.client.propose_indexed(cmd).await
    }

    /// Propose request and get result with fast path
    async fn propose_fast_path(
        &self,
        propose_id: ProposeId,
        request: Request,
    ) -> Result<CommandResponse, ProposeError> {
        let cmd = Self::command_from_request(propose_id, request);
        self.client.propose(cmd).await
    }

    /// Update revision of `ResponseHeader`
    pub(crate) fn update_header_revision(response: &mut Response, revision: i64) {
        match *response {
            Response::ResponseRange(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
            }
            Response::ResponsePut(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
            }
            Response::ResponseDeleteRange(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
            }
            Response::ResponseTxn(ref mut res) => {
                if let Some(header) = res.header.as_mut() {
                    header.revision = revision;
                }
                for resp in &mut res.responses {
                    if let Some(re) = resp.response.as_mut() {
                        Self::update_header_revision(re, revision);
                    }
                }
            }
        };
    }

    /// Generate propose id
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }
}

#[tonic::async_trait]
impl Kv for KvServer {
    /// Range gets the keys in the range from the key-value store.
    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        debug!("Receive RangeRequest {:?}", request);

        let range_request = request.into_inner();
        let propose_id = self.generate_propose_id();
        let is_fast_path = true;
        let (res_op, sync_res) = if is_fast_path {
            let res_op = self
                .propose_fast_path(propose_id.clone(), Request::RequestRange(range_request))
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, None)
        } else {
            let (res_op, sync_res) = self
                .propose_slow_path(propose_id.clone(), Request::RequestRange(range_request))
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, Some(sync_res))
        };

        let mut res = Self::parse_response_op(res_op.decode());
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for RangeRequest", revision);
            Self::update_header_revision(&mut res, revision);
        }
        if let Response::ResponseRange(response) = res {
            Ok(tonic::Response::new(response))
        } else {
            panic!("Receive wrong response {:?} for RangeRequest", res);
        }
    }

    /// Put puts the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        debug!("Receive PutRequest {:?}", request);
        let put_request = request.into_inner();
        let propose_id = self.generate_propose_id();
        let is_fast_path = true;
        let (res_op, sync_res) = if is_fast_path {
            let res_op = self
                .propose_fast_path(propose_id.clone(), Request::RequestPut(put_request))
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, None)
        } else {
            let (res_op, sync_res) = self
                .propose_slow_path(propose_id.clone(), Request::RequestPut(put_request))
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, Some(sync_res))
        };

        let mut res = Self::parse_response_op(res_op.decode());
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for PutRequest", revision);
            Self::update_header_revision(&mut res, revision);
        }
        if let Response::ResponsePut(response) = res {
            Ok(tonic::Response::new(response))
        } else {
            panic!("Receive wrong response {:?} for PutRequest", res);
        }
    }

    /// DeleteRange deletes the given range from the key-value store.
    /// A delete request increments the revision of the key-value store
    /// and generates a delete event in the event history for every deleted key.
    async fn delete_range(
        &self,
        request: tonic::Request<DeleteRangeRequest>,
    ) -> Result<tonic::Response<DeleteRangeResponse>, tonic::Status> {
        debug!("Receive DeleteRangeRequest {:?}", request);
        let delete_range_request = request.into_inner();
        let propose_id = self.generate_propose_id();
        let is_fast_path = true;
        let (res_op, sync_res) = if is_fast_path {
            let res_op = self
                .propose_fast_path(
                    propose_id.clone(),
                    Request::RequestDeleteRange(delete_range_request),
                )
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, None)
        } else {
            let (res_op, sync_res) = self
                .propose_slow_path(
                    propose_id.clone(),
                    Request::RequestDeleteRange(delete_range_request),
                )
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, Some(sync_res))
        };

        let mut res = Self::parse_response_op(res_op.decode());
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for DeleteRangeRequest", revision);
            Self::update_header_revision(&mut res, revision);
        }
        if let Response::ResponseDeleteRange(response) = res {
            Ok(tonic::Response::new(response))
        } else {
            panic!("Receive wrong response {:?} for DeleteRangeRequest", res);
        }
    }

    /// Txn processes multiple requests in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed request.
    /// It is not allowed to modify the same key several times within one txn.
    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        debug!("Receive TxnRequest {:?}", request);
        let txn_request = request.into_inner();
        let propose_id = self.generate_propose_id();
        let is_fast_path = true;
        let (res_op, sync_res) = if is_fast_path {
            let res_op = self
                .propose_fast_path(propose_id.clone(), Request::RequestTxn(txn_request))
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, None)
        } else {
            let (res_op, sync_res) = self
                .propose_slow_path(propose_id.clone(), Request::RequestTxn(txn_request))
                .await
                .unwrap_or_else(|e| panic!("failed to receive response from kv storage, {e}"));
            (res_op, Some(sync_res))
        };

        let mut res = Self::parse_response_op(res_op.decode());
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for TxnRequest", revision);
            Self::update_header_revision(&mut res, revision);
        }
        if let Response::ResponseTxn(response) = res {
            Ok(tonic::Response::new(response))
        } else {
            panic!("Receive wrong response {:?} for TxnRequest", res);
        }
    }

    /// Compact compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    async fn compact(
        &self,
        request: tonic::Request<CompactionRequest>,
    ) -> Result<tonic::Response<CompactionResponse>, tonic::Status> {
        debug!("Receive CompactionRequest {:?}", request);
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Not Implemented".to_owned(),
        ))
    }
}
