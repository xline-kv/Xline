use std::{fmt::Debug, sync::Arc, time::Duration};

use curp::{
    client::{Client, ReadState},
    cmd::generate_propose_id,
};
use futures::future::join_all;
use tokio::time::timeout;
use tracing::{debug, instrument};
use xlineapi::ResponseWrapper;

use super::{
    auth_server::get_token,
    barriers::{IdBarrier, IndexBarrier},
    command::{propose_err_to_status, Command, CommandResponse, SyncResponse},
};
use crate::{
    request_validation::RequestValidator,
    revision_check::RevisionCheck,
    rpc::{
        CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, Kv,
        PutRequest, PutResponse, RangeRequest, RangeResponse, RequestWithToken, RequestWrapper,
        Response, ResponseOp, TxnRequest, TxnResponse,
    },
    server::command::command_from_request_wrapper,
    storage::{storage_api::StorageApi, AuthStore, ExecuteError, KvStore},
};

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

    /// serializable execute request in current node
    fn do_serializable(&self, wrapper: &RequestWithToken) -> Result<Response, tonic::Status> {
        self.auth_storage.check_permission(wrapper)?;
        let cmd_res = self.kv_storage.execute(wrapper)?;

        Ok(Self::parse_response_op(cmd_res.into_inner().into()))
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
        let cmd = command_from_request_wrapper::<S>(generate_propose_id(&self.name), wrapper, None);

        self.client
            .propose(cmd, use_fast_path)
            .await
            .map_err(propose_err_to_status)
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

    /// check whether the required revision is compacted or not
    fn check_range_compacted(
        range_revision: i64,
        compacted_revision: i64,
    ) -> Result<(), tonic::Status> {
        (range_revision <= 0 || range_revision >= compacted_revision)
            .then_some(())
            .ok_or(ExecuteError::RevisionCompacted(range_revision, compacted_revision).into())
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
        range_req.validation()?;
        debug!("Receive grpc request: {:?}", range_req);
        range_req.check_revision(
            self.kv_storage.compacted_revision(),
            self.kv_storage.revision(),
        )?;
        let range_required_revision = range_req.revision;
        let is_serializable = range_req.serializable;
        let token = get_token(request.metadata());
        let wrapper = RequestWithToken::new_with_token(request.into_inner().into(), token);
        let propose_id = generate_propose_id(&self.name);
        let cmd = command_from_request_wrapper::<S>(propose_id, wrapper, None);
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
        put_req.validation()?;
        debug!("Receive grpc request: {:?}", put_req);
        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res = Self::parse_response_op(cmd_res.into_inner().into());
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
        delete_range_req.validation()?;
        debug!("Receive grpc request: {:?}", delete_range_req);
        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res = Self::parse_response_op(cmd_res.into_inner().into());
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
        txn_req.validation()?;
        debug!("Receive grpc request: {:?}", txn_req);
        txn_req.check_revision(
            self.kv_storage.compacted_revision(),
            self.kv_storage.revision(),
        )?;

        let res = if txn_req.is_read_only() {
            debug!("TxnRequest is read only");
            let is_serializable = txn_req.is_serializable();
            let token = get_token(request.metadata());
            let wrapper = RequestWithToken::new_with_token(request.into_inner().into(), token);
            let propose_id = generate_propose_id(&self.name);
            let cmd = command_from_request_wrapper::<S>(propose_id, wrapper, None);
            if !is_serializable {
                self.wait_read_state(&cmd).await?;
            }
            self.do_serializable(cmd.request())?
        } else {
            let is_fast_path = true;
            let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

            let mut res = Self::parse_response_op(cmd_res.into_inner().into());
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
        req.check_revision(compacted_revision, current_revision)?;

        let is_fast_path = !req.physical;
        let (cmd_res, _sync_res) = self.propose(request, is_fast_path).await?;
        let resp = cmd_res.into_inner();

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
    use crate::rpc::{Request, RequestOp};

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
        assert!(txn_req.validation().is_ok());
        assert!(txn_req.check_revision(1, 2).is_ok());
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

        let expected_tonic_status = tonic::Status::from(
            range_request_with_future_rev
                .check_revision(compacted_revision, current_revision)
                .unwrap_err(),
        );
        assert_eq!(expected_tonic_status.code(), tonic::Code::OutOfRange);

        let range_request_with_compacted_rev = RangeRequest {
            key: b"foo".to_vec(),
            revision: 2,
            ..Default::default()
        };

        let expected_tonic_status = tonic::Status::from(
            range_request_with_compacted_rev
                .check_revision(compacted_revision, current_revision)
                .unwrap_err(),
        );

        assert_eq!(expected_tonic_status.code(), tonic::Code::OutOfRange);
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

        let expected_tonic_status = tonic::Status::from(
            txn_request_with_future_revision
                .check_revision(compacted_revision, current_revision)
                .unwrap_err(),
        );

        assert_eq!(expected_tonic_status.code(), tonic::Code::OutOfRange);

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

        let expected_tonic_status = tonic::Status::from(
            txn_request_with_compacted_revision
                .check_revision(compacted_revision, current_revision)
                .unwrap_err(),
        );

        assert_eq!(expected_tonic_status.code(), tonic::Code::OutOfRange);
    }

    #[tokio::test]
    async fn test_compact_invalid_revision() {
        let compact_request = CompactionRequest {
            revision: 10,
            ..Default::default()
        };

        let expected_tonic_status =
            tonic::Status::from(compact_request.check_revision(3, 8).unwrap_err());
        assert_eq!(expected_tonic_status.code(), tonic::Code::OutOfRange);

        let expected_tonic_status =
            tonic::Status::from(compact_request.check_revision(13, 18).unwrap_err());
        assert_eq!(expected_tonic_status.code(), tonic::Code::OutOfRange);
    }
}
