use std::{collections::HashSet, fmt::Debug, sync::Arc};

use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use log::debug;
use tracing::instrument;
use uuid::Uuid;

use super::{
    auth_server::get_token,
    command::{Command, CommandResponse, KeyRange, SyncResponse},
};
use crate::{
    rpc::{
        CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, Kv,
        PutRequest, PutResponse, RangeRequest, RangeResponse, Request, RequestOp, RequestWithToken,
        RequestWrapper, Response, ResponseOp, SortOrder, SortTarget, TxnRequest, TxnResponse,
    },
    storage::KvStore,
};

/// Default max txn ops
const DEFAULT_MAX_TXN_OPS: usize = 128;

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

    /// Generate `Command` proposal from `RequestWrapper`
    fn command_from_request_wrapper(propose_id: ProposeId, wrapper: RequestWithToken) -> Command {
        #[allow(clippy::wildcard_enum_match_arm)]
        let key_ranges = match wrapper.request {
            RequestWrapper::RangeRequest(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            }],
            RequestWrapper::PutRequest(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: vec![],
            }],
            RequestWrapper::DeleteRangeRequest(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            }],
            RequestWrapper::TxnRequest(ref req) => req
                .compare
                .iter()
                .map(|cmp| KeyRange {
                    start: cmp.key.clone(),
                    end: cmp.range_end.clone(),
                })
                .collect(),
            _ => unreachable!("Other request should not be sent to this store"),
        };
        Command::new(key_ranges, wrapper, propose_id)
    }

    /// Propose request and get result with fast/slow path
    #[instrument(skip(self))]
    async fn propose<T>(
        &self,
        request: tonic::Request<T>,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>), tonic::Status>
    where
        T: Into<RequestWrapper> + Debug,
    {
        let wrapper = match get_token(request.metadata()) {
            Some(token) => RequestWithToken::new_with_token(request.into_inner().into(), token),
            None => RequestWithToken::new(request.into_inner().into()),
        };
        let propose_id = self.generate_propose_id();
        let cmd = Self::command_from_request_wrapper(propose_id, wrapper);
        if use_fast_path {
            let cmd_res = self.client.propose(cmd).await.map_err(|err| {
                if let ProposeError::ExecutionError(e) = err {
                    tonic::Status::invalid_argument(e)
                } else {
                    panic!("propose err {err:?}")
                }
            })?;
            Ok((cmd_res, None))
        } else {
            let (cmd_res, sync_res) = self.client.propose_indexed(cmd).await.map_err(|err| {
                if let ProposeError::ExecutionError(e) = err {
                    tonic::Status::invalid_argument(e)
                } else {
                    panic!("propose err {err:?}")
                }
            })?;
            Ok((cmd_res, Some(sync_res)))
        }
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

    /// Validate range request before handle
    fn check_range_request(req: &RangeRequest) -> Result<(), tonic::Status> {
        if req.serializable {
            return Err(tonic::Status::unimplemented(
                "serializable is unimplemented",
            ));
        }
        if req.keys_only {
            return Err(tonic::Status::unimplemented("keys_only is unimplemented"));
        }
        if req.min_mod_revision != 0
            || req.max_mod_revision != 0
            || req.min_create_revision != 0
            || req.max_create_revision != 0
        {
            return Err(tonic::Status::unimplemented(
                "min/max mod/create revision is unimplemented",
            ));
        }
        // TODO: Remove the above errors after implementation

        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument("key is not provided"));
        }
        if !SortOrder::is_valid(req.sort_order) || !SortTarget::is_valid(req.sort_target) {
            return Err(tonic::Status::invalid_argument("invalid sort option"));
        }

        Ok(())
    }

    /// Validate put request before handle
    fn check_put_request(req: &PutRequest) -> Result<(), tonic::Status> {
        if req.lease != 0 {
            return Err(tonic::Status::unimplemented("lease is unimplemented"));
        }
        // TODO: Remove the above errors after implementation
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument("key is not provided"));
        }
        if req.ignore_value && !req.value.is_empty() {
            return Err(tonic::Status::invalid_argument("value is provided"));
        }
        if req.ignore_lease && req.lease != 0 {
            return Err(tonic::Status::invalid_argument("lease is provided"));
        }

        Ok(())
    }

    /// Validate delete range request before handle
    fn check_delete_range_request(req: &DeleteRangeRequest) -> Result<(), tonic::Status> {
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument("key is not provided"));
        }

        Ok(())
    }

    /// Validate txn request before handle
    fn check_txn_request(req: &TxnRequest) -> Result<(), tonic::Status> {
        let opc = req
            .compare
            .len()
            .max(req.success.len())
            .max(req.failure.len());
        if opc > DEFAULT_MAX_TXN_OPS {
            return Err(tonic::Status::invalid_argument(
                "too many operations in txn request",
            ));
        }
        for c in &req.compare {
            if c.key.is_empty() {
                return Err(tonic::Status::invalid_argument("key is not provided"));
            }
        }
        for op in req.success.iter().chain(req.failure.iter()) {
            if let Some(ref request) = op.request {
                match *request {
                    Request::RequestRange(ref r) => Self::check_range_request(r),
                    Request::RequestPut(ref r) => Self::check_put_request(r),
                    Request::RequestDeleteRange(ref r) => Self::check_delete_range_request(r),
                    Request::RequestTxn(ref r) => Self::check_txn_request(r),
                }?;
            } else {
                return Err(tonic::Status::invalid_argument("key not found"));
            }
        }

        let _ignore_success = Self::check_intervals(&req.success)?;
        let _ignore_failure = Self::check_intervals(&req.failure)?;

        Ok(())
    }
    /// Check if puts and deletes overlap
    fn check_intervals(
        ops: &[RequestOp],
    ) -> Result<(HashSet<&[u8]>, Vec<KeyRange>), tonic::Status> {
        // TODO: use interval tree is better?

        let mut dels = Vec::new();

        for op in ops {
            if let Some(Request::RequestDeleteRange(ref req)) = op.request {
                // collect dels
                let del = KeyRange {
                    start: req.key.clone(),
                    end: req.range_end.clone(),
                };
                dels.push(del);
            }
        }

        let mut puts: HashSet<&[u8]> = HashSet::new();

        for op in ops {
            if let Some(Request::RequestTxn(ref req)) = op.request {
                // handle child txn request
                let (success_puts, mut success_dels) = Self::check_intervals(&req.success)?;
                let (failure_puts, mut failure_dels) = Self::check_intervals(&req.failure)?;

                for k in &success_puts {
                    if !puts.insert(k) {
                        return Err(tonic::Status::invalid_argument(
                            "duplicate key given in txn request",
                        ));
                    }
                    if dels.iter().any(|del| del.contains_key(k)) {
                        return Err(tonic::Status::invalid_argument(
                            "duplicate key given in txn request",
                        ));
                    }
                }

                for k in failure_puts {
                    if !puts.insert(k) && !success_puts.contains(k) {
                        // only keys in the puts and not in the success_puts is overlap
                        return Err(tonic::Status::invalid_argument(
                            "duplicate key given in txn request",
                        ));
                    }
                    if dels.iter().any(|del| del.contains_key(k)) {
                        return Err(tonic::Status::invalid_argument(
                            "duplicate key given in txn request",
                        ));
                    }
                }

                dels.append(&mut success_dels);
                dels.append(&mut failure_dels);
            }
        }

        for op in ops {
            if let Some(Request::RequestPut(ref req)) = op.request {
                // check puts in this level
                if !puts.insert(&req.key) {
                    return Err(tonic::Status::invalid_argument(
                        "duplicate key given in txn request",
                    ));
                }
                if dels.iter().any(|del| del.contains_key(&req.key)) {
                    return Err(tonic::Status::invalid_argument(
                        "duplicate key given in txn request",
                    ));
                }
            }
        }
        Ok((puts, dels))
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
        Self::check_range_request(request.get_ref())?;
        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res = Self::parse_response_op(cmd_res.decode().into());
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
    #[instrument(skip(self))]
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        debug!("Receive PutRequest {:?}", request);
        Self::check_put_request(request.get_ref())?;
        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res = Self::parse_response_op(cmd_res.decode().into());
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
        Self::check_delete_range_request(request.get_ref())?;
        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res = Self::parse_response_op(cmd_res.decode().into());
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
        Self::check_txn_request(request.get_ref())?;
        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res = Self::parse_response_op(cmd_res.decode().into());
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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn txn_check() {
        let txn_req = TxnRequest {
            compare: vec![],
            success: vec![
                RequestOp {
                    request: Some(Request::RequestDeleteRange(DeleteRangeRequest {
                        key: b"foo1".to_vec(),
                        range_end: vec![],
                        prev_kv: false,
                    })),
                },
                RequestOp {
                    request: Some(Request::RequestTxn(TxnRequest {
                        compare: vec![],
                        success: vec![RequestOp {
                            request: Some(Request::RequestPut(PutRequest {
                                key: b"foo".to_vec(),
                                value: b"bar".to_vec(),
                                lease: 0,
                                prev_kv: false,
                                ignore_value: false,
                                ignore_lease: false,
                            })),
                        }],
                        failure: vec![RequestOp {
                            request: Some(Request::RequestPut(PutRequest {
                                key: b"foo".to_vec(),
                                value: b"bar".to_vec(),
                                lease: 0,
                                prev_kv: false,
                                ignore_value: false,
                                ignore_lease: false,
                            })),
                        }],
                    })),
                },
            ],
            failure: vec![],
        };
        let result = KvServer::check_txn_request(&txn_req);
        assert!(result.is_ok());
    }
}
