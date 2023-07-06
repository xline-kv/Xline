use std::{collections::HashSet, fmt::Debug, sync::Arc, time::Duration};

use curp::{
    client::{Client, ReadState},
    cmd::ProposeId,
    error::ProposeError,
};
use futures::future::join_all;
use tokio::time::timeout;
use tracing::{debug, instrument};
use uuid::Uuid;
use xlineapi::{
    CompactionResponse, DeleteRangeResponse, Kv, PutResponse, RangeResponse, ResponseWrapper,
    TxnResponse,
};

use super::{
    auth_server::get_token,
    barriers::{IdBarrier, IndexBarrier},
    command::{Command, CommandResponse, KeyRange, SyncResponse},
};
use crate::{
    rpc::{
        CompactionRequest, DeleteRangeRequest, PutRequest, RangeRequest, Request, RequestOp,
        RequestWithToken, RequestWrapper, Response, ResponseOp, SortOrder, SortTarget, TxnRequest,
    },
    storage::{storage_api::StorageApi, AuthStore, KvStore},
};

/// Default max txn ops
const DEFAULT_MAX_TXN_OPS: usize = 128;

/// KV Server
#[derive(Debug)]
pub(crate) struct KvServer<S>
where
    S: StorageApi,
{
    /// KV storage
    kv_storage: Arc<KvStore<S>>,
    /// Auth storage
    auth_storage: Arc<AuthStore<S>>,
    /// Barrier for applied index
    index_barrier: Arc<IndexBarrier>,
    /// Barrier for propose id
    id_barrier: Arc<IdBarrier>,
    /// Range request retry timeout
    range_retry_timeout: Duration,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
}

impl<S> KvServer<S>
where
    S: StorageApi,
{
    /// New `KvServer`
    pub(crate) fn new(
        kv_storage: Arc<KvStore<S>>,
        auth_storage: Arc<AuthStore<S>>,
        index_barrier: Arc<IndexBarrier>,
        id_barrier: Arc<IdBarrier>,
        range_retry_timeout: Duration,
        client: Arc<Client<Command>>,
        name: String,
    ) -> Self {
        Self {
            kv_storage,
            auth_storage,
            index_barrier,
            id_barrier,
            range_retry_timeout,
            client,
            name,
        }
    }

    /// Parse `ResponseOp`
    pub(crate) fn parse_response_op(response_op: ResponseOp) -> Response {
        if let Some(response) = response_op.response {
            response
        } else {
            unreachable!("Receive empty ResponseOp");
        }
    }

    /// Generate `Command` proposal from `RequestWrapper`
    fn command_from_request_wrapper(propose_id: ProposeId, wrapper: RequestWithToken) -> Command {
        #[allow(clippy::wildcard_enum_match_arm)]
        let key_ranges = match wrapper.request {
            RequestWrapper::RangeRequest(ref req) => {
                vec![KeyRange::new(req.key.as_slice(), req.range_end.as_slice())]
            }
            RequestWrapper::PutRequest(ref req) => vec![KeyRange::new_one_key(req.key.as_slice())],
            RequestWrapper::DeleteRangeRequest(ref req) => {
                vec![KeyRange::new(req.key.as_slice(), req.range_end.as_slice())]
            }
            RequestWrapper::TxnRequest(ref req) => req
                .compare
                .iter()
                .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
                .collect(),
            RequestWrapper::CompactionRequest(ref _req) => Vec::new(),
            _ => unreachable!("Other request should not be sent to this store"),
        };
        Command::new(key_ranges, wrapper, propose_id)
    }

    /// serializable execute request in current node
    fn do_serializable(&self, wrapper: &RequestWithToken) -> Result<Response, tonic::Status> {
        self.auth_storage
            .check_permission(wrapper)
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
        let cmd_res = self
            .kv_storage
            .execute(wrapper)
            .map_err(|e| tonic::Status::internal(format!("Execute failed: {e:?}")))?;
        Ok(Self::parse_response_op(cmd_res.decode().into()))
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: tonic::Request<T>,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>), tonic::Status>
    where
        T: Into<RequestWrapper> + Debug,
    {
        let token = get_token(request.metadata());
        let wrapper = RequestWithToken::new_with_token(request.into_inner().into(), token);
        let propose_id = self.generate_propose_id();
        let cmd = Self::command_from_request_wrapper(propose_id, wrapper);
        #[allow(clippy::wildcard_enum_match_arm)]
        if use_fast_path {
            let cmd_res = self.client.propose(cmd).await.map_err(|err| match err {
                ProposeError::ExecutionError(e) => tonic::Status::invalid_argument(e),
                _ => unreachable!("propose err {err:?}"),
            })?;
            Ok((cmd_res, None))
        } else {
            let (cmd_res, sync_res) =
                self.client
                    .propose_indexed(cmd)
                    .await
                    .map_err(|err| match err {
                        ProposeError::ExecutionError(e) => tonic::Status::invalid_argument(e),
                        ProposeError::SyncedError(e) => tonic::Status::unknown(e),
                        _ => unreachable!("propose err {err:?}"),
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
    fn check_range_request(
        req: &RangeRequest,
        compacted_revision: i64,
        current_revision: i64,
    ) -> Result<(), tonic::Status> {
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument("key is not provided"));
        }
        if !SortOrder::is_valid(req.sort_order) || !SortTarget::is_valid(req.sort_target) {
            return Err(tonic::Status::invalid_argument("invalid sort option"));
        }
        if req.revision > current_revision {
            Err(tonic::Status::invalid_argument(format!(
                "required revision {} is higher than current revision {}",
                req.revision, current_revision
            )))
        } else {
            Self::check_range_compacted(req.revision, compacted_revision)
        }
    }

    /// check whether the required revision is compacted or not
    fn check_range_compacted(
        range_revision: i64,
        compacted_revision: i64,
    ) -> Result<(), tonic::Status> {
        (range_revision <= 0 || range_revision >= compacted_revision)
            .then_some(())
            .ok_or(tonic::Status::invalid_argument(format!(
                "required revision {range_revision} has been compacted, compacted revision is {compacted_revision}"
            )))
    }

    /// Validate compact request before handle
    fn check_compact_request(
        req: &CompactionRequest,
        compacted_revision: i64,
        current_revision: i64,
    ) -> Result<(), tonic::Status> {
        debug_assert!(
            compacted_revision <= current_revision,
            "compacted revision should not larger than current revision"
        );
        if req.revision <= compacted_revision {
            Err(tonic::Status::invalid_argument(format!(
                "required revision {} has been compacted, compacted revision is {}",
                req.revision, compacted_revision
            )))
        } else if req.revision > current_revision {
            Err(tonic::Status::invalid_argument(format!(
                "required revision {} is higher than current revision {}",
                req.revision, current_revision
            )))
        } else {
            Ok(())
        }
    }

    /// Validate put request before handle
    fn check_put_request(req: &PutRequest) -> Result<(), tonic::Status> {
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
    fn check_txn_request(
        req: &TxnRequest,
        compacted_revision: i64,
        current_revision: i64,
    ) -> Result<(), tonic::Status> {
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
                    Request::RequestRange(ref r) => {
                        Self::check_range_request(r, compacted_revision, current_revision)
                    }
                    Request::RequestPut(ref r) => Self::check_put_request(r),
                    Request::RequestDeleteRange(ref r) => Self::check_delete_range_request(r),
                    Request::RequestTxn(ref r) => {
                        Self::check_txn_request(r, compacted_revision, current_revision)
                    }
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
                let del = KeyRange::new(req.key.as_slice(), req.range_end.as_slice());
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

    /// Wait current node's state machine apply the conflict commands
    async fn wait_read_state(&self, cmd: &Command) -> Result<(), tonic::Status> {
        loop {
            let rd_state = self
                .client
                .fetch_read_state(cmd)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;
            let wait_future = async move {
                match rd_state {
                    ReadState::Ids(ids) => {
                        debug!(?ids, "Range wait for command ids");
                        let fus = ids
                            .into_iter()
                            .map(|id| self.id_barrier.wait(id))
                            .collect::<Vec<_>>();
                        let _ignore = join_all(fus).await;
                    }
                    ReadState::CommitIndex(index) => {
                        debug!(?index, "Range wait for commit index");
                        self.index_barrier.wait(index).await;
                    }
                    _ => unreachable!(),
                }
            };
            if timeout(self.range_retry_timeout, wait_future).await.is_ok() {
                break;
            };
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl<S> Kv for KvServer<S>
where
    S: StorageApi,
{
    /// Range gets the keys in the range from the key-value store.
    #[instrument(skip_all)]
    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        let range_req = request.get_ref();
        debug!("Receive grpc request: {:?}", range_req);
        Self::check_range_request(
            range_req,
            self.kv_storage.compacted_revision(),
            self.kv_storage.revision(),
        )?;
        let range_required_revision = range_req.revision;
        let is_serializable = range_req.serializable;
        let token = get_token(request.metadata());
        let wrapper = RequestWithToken::new_with_token(request.into_inner().into(), token);
        let propose_id = self.generate_propose_id();
        let cmd = Self::command_from_request_wrapper(propose_id, wrapper);
        if !is_serializable {
            self.wait_read_state(&cmd).await?;
            // Double check whether the range request is compacted or not since the compaction request
            // may be executed during the process of `wait_read_state` which results in the result of
            // previous `check_range_request` outdated.
            Self::check_range_compacted(
                range_required_revision,
                self.kv_storage.compacted_revision(),
            )?;
        }
        let res = self.do_serializable(cmd.request())?;
        if let Response::ResponseRange(response) = res {
            Ok(tonic::Response::new(response))
        } else {
            unreachable!("Receive wrong response {res:?} for RangeRequest");
        }
    }

    /// Put puts the given key into the key-value store.
    /// A put request increments the revision of the key-value store
    /// and generates one event in the event history.
    #[instrument(skip_all)]
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        let put_req: &PutRequest = request.get_ref();
        debug!("Receive grpc request: {:?}", put_req);
        Self::check_put_request(put_req)?;
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
            unreachable!("Receive wrong response {res:?} for PutRequest");
        }
    }

    /// DeleteRange deletes the given range from the key-value store.
    /// A delete request increments the revision of the key-value store
    /// and generates a delete event in the event history for every deleted key.
    #[instrument(skip_all)]
    async fn delete_range(
        &self,
        request: tonic::Request<DeleteRangeRequest>,
    ) -> Result<tonic::Response<DeleteRangeResponse>, tonic::Status> {
        let delete_range_req = request.get_ref();
        debug!("Receive grpc request: {:?}", delete_range_req);
        Self::check_delete_range_request(delete_range_req)?;
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
            unreachable!("Receive wrong response {res:?} for DeleteRangeRequest");
        }
    }

    /// Txn processes multiple requests in a single transaction.
    /// A txn request increments the revision of the key-value store
    /// and generates events with the same revision for every completed request.
    /// It is not allowed to modify the same key several times within one txn.
    #[instrument(skip_all)]
    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        let txn_req = request.get_ref();
        debug!("Receive grpc request: {:?}", txn_req);
        Self::check_txn_request(
            txn_req,
            self.kv_storage.compacted_revision(),
            self.kv_storage.revision(),
        )?;

        let res = if txn_req.is_read_only() {
            debug!("TxnRequest is read only");
            let is_serializable = txn_req.is_serializable();
            let token = get_token(request.metadata());
            let wrapper = RequestWithToken::new_with_token(request.into_inner().into(), token);
            let propose_id = self.generate_propose_id();
            let cmd = Self::command_from_request_wrapper(propose_id, wrapper);
            if !is_serializable {
                self.wait_read_state(&cmd).await?;
            }
            self.do_serializable(cmd.request())?
        } else {
            let is_fast_path = false; // lock need revision of txn
            let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

            let mut res = Self::parse_response_op(cmd_res.decode().into());
            if let Some(sync_res) = sync_res {
                let revision = sync_res.revision();
                debug!("Get revision {:?} for TxnRequest", revision);
                Self::update_header_revision(&mut res, revision);
            }
            res
        };
        if let Response::ResponseTxn(response) = res {
            Ok(tonic::Response::new(response))
        } else {
            unreachable!("Receive wrong response {res:?} for TxnRequest");
        }
    }

    /// Compact compacts the event history in the etcd key-value store. The key-value
    /// store should be periodically compacted or the event history will continue to grow
    /// indefinitely.
    #[instrument(skip_all)]
    async fn compact(
        &self,
        request: tonic::Request<CompactionRequest>,
    ) -> Result<tonic::Response<CompactionResponse>, tonic::Status> {
        debug!("Receive CompactionRequest {:?}", request);
        let compacted_revision = self.kv_storage.compacted_revision();
        let current_revision = self.kv_storage.revision();
        let req = request.get_ref();
        Self::check_compact_request(req, compacted_revision, current_revision)?;

        let is_fast_path = !req.physical;
        let (cmd_res, _sync_res) = self.propose(request, is_fast_path).await?;
        let resp = cmd_res.decode();

        if let ResponseWrapper::CompactionResponse(response) = resp {
            Ok(tonic::Response::new(response))
        } else {
            panic!("Receive wrong response {resp:?} for CompactionRequest");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::db::DB;

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
        let result = KvServer::<DB>::check_txn_request(&txn_req, 1, 0);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_range_invalid_revision() {
        let current_revision = 10;
        let compacted_revision = 5;
        let range_request_with_future_rev = RangeRequest {
            key: b"foo".to_vec(),
            revision: 20,
            ..Default::default()
        };

        let expected_err_message = tonic::Status::invalid_argument(format!(
            "required revision {} is higher than current revision {}",
            range_request_with_future_rev.revision, current_revision
        ))
        .to_string();
        let message = KvServer::<DB>::check_range_request(
            &range_request_with_future_rev,
            compacted_revision,
            current_revision,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(message, expected_err_message);

        let range_request_with_compacted_rev = RangeRequest {
            key: b"foo".to_vec(),
            revision: 2,
            ..Default::default()
        };

        let expected_err_message = tonic::Status::invalid_argument(format!(
            "required revision {} has been compacted, compacted revision is {}",
            range_request_with_compacted_rev.revision, compacted_revision
        ))
        .to_string();

        let message = KvServer::<DB>::check_range_request(
            &range_request_with_compacted_rev,
            compacted_revision,
            current_revision,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(message, expected_err_message);
    }

    #[tokio::test]
    async fn test_txn_invalid_revision() {
        let current_revision = 10;
        let compacted_revision = 5;
        let txn_request_with_future_revision = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest {
                    key: b"foo".to_vec(),
                    revision: 20,
                    ..Default::default()
                })),
            }],
            failure: vec![],
        };

        let expected_err_message = tonic::Status::invalid_argument(format!(
            "required revision {} is higher than current revision {}",
            20, current_revision
        ))
        .to_string();

        let message = KvServer::<DB>::check_txn_request(
            &txn_request_with_future_revision,
            compacted_revision,
            current_revision,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(message, expected_err_message);

        let txn_request_with_compacted_revision = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest {
                    key: b"foo".to_vec(),
                    revision: 3,
                    ..Default::default()
                })),
            }],
            failure: vec![],
        };

        let expected_err_message = tonic::Status::invalid_argument(format!(
            "required revision {} has been compacted, compacted revision is {}",
            3, compacted_revision
        ))
        .to_string();

        let message = KvServer::<DB>::check_txn_request(
            &txn_request_with_compacted_revision,
            compacted_revision,
            current_revision,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(message, expected_err_message);
    }

    #[tokio::test]
    async fn test_compact_invalid_revision() {
        let compact_request = CompactionRequest {
            revision: 10,
            ..Default::default()
        };

        let expected_err_message = tonic::Status::invalid_argument(format!(
            "required revision {} is higher than current revision {}",
            compact_request.revision, 8
        ))
        .to_string();

        let message = KvServer::<DB>::check_compact_request(&compact_request, 3, 8)
            .unwrap_err()
            .to_string();
        assert_eq!(message, expected_err_message);

        let expected_err_message = tonic::Status::invalid_argument(format!(
            "required revision {} has been compacted, compacted revision is {}",
            compact_request.revision, 13
        ))
        .to_string();

        let message = KvServer::<DB>::check_compact_request(&compact_request, 13, 18)
            .unwrap_err()
            .to_string();
        assert_eq!(message, expected_err_message);
    }
}
