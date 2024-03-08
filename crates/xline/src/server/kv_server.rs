use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use curp::rpc::ReadState;
use dashmap::DashMap;
use event_listener::Event;
use futures::future::{join_all, Either};
use tokio::time::timeout;
use tracing::{debug, instrument};
use xlineapi::{
    command::{Command, CommandResponse, CurpClient, SyncResponse},
    execute_error::ExecuteError,
    request_validation::RequestValidator,
    AuthInfo, ResponseWrapper,
};

use super::barriers::{IdBarrier, IndexBarrier};
use crate::{
    metrics,
    revision_check::RevisionCheck,
    rpc::{
        CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, Kv,
        PutRequest, PutResponse, RangeRequest, RangeResponse, RequestWrapper, Response, ResponseOp,
        TxnRequest, TxnResponse,
    },
    storage::{storage_api::StorageApi, AuthStore, KvStore},
};

/// KV Server
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
    /// Compact timeout
    compact_timeout: Duration,
    /// Consensus client
    client: Arc<CurpClient>,
    /// Compact events
    compact_events: Arc<DashMap<u64, Arc<Event>>>,
    /// Next compact_id
    next_compact_id: AtomicU64,
}

impl<S> KvServer<S>
where
    S: StorageApi,
{
    /// New `KvServer`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kv_storage: Arc<KvStore<S>>,
        auth_storage: Arc<AuthStore<S>>,
        index_barrier: Arc<IndexBarrier>,
        id_barrier: Arc<IdBarrier>,
        range_retry_timeout: Duration,
        compact_timeout: Duration,
        client: Arc<CurpClient>,
        compact_events: Arc<DashMap<u64, Arc<Event>>>,
    ) -> Self {
        Self {
            kv_storage,
            auth_storage,
            index_barrier,
            id_barrier,
            range_retry_timeout,
            compact_timeout,
            client,
            compact_events,
            next_compact_id: AtomicU64::new(0),
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
    fn do_serializable(&self, command: &Command) -> Result<Response, tonic::Status> {
        self.auth_storage
            .check_permission(command.request(), command.auth_info())?;
        let cmd_res = self.kv_storage.execute(command.request())?;
        Ok(Self::parse_response_op(cmd_res.into_inner().into()))
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: T,
        auth_info: Option<AuthInfo>,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>), tonic::Status>
    where
        T: Into<RequestWrapper>,
    {
        let request = request.into();
        let cmd = Command::new_with_auth_info(request.keys(), request, auth_info);
        let res = self.client.propose(&cmd, None, use_fast_path).await??;
        Ok(res)
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
            let rd_state = self.client.fetch_read_state(cmd).await.map_err(|e| {
                metrics::get().read_indexes_failed_total.add(1, &[]);
                e
            })?;
            let wait_future = async move {
                match rd_state {
                    ReadState::Ids(id_set) => {
                        debug!(?id_set, "Range wait for command ids");
                        let fus = id_set
                            .inflight_ids
                            .into_iter()
                            .map(|id| self.id_barrier.wait(id))
                            .collect::<Vec<_>>();
                        let _ignore = join_all(fus).await;
                    }
                    ReadState::CommitIndex(index) => {
                        debug!(?index, "Range wait for commit index");
                        self.index_barrier.wait(index).await;
                    }
                }
            };
            if timeout(self.range_retry_timeout, wait_future).await.is_ok() {
                break;
            }
            metrics::get().slow_read_indexes_total.add(1, &[]);
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
        debug!("Receive grpc request: {}", range_req);
        range_req.check_revision(
            self.kv_storage.compacted_revision(),
            self.kv_storage.revision(),
        )?;
        let auth_info = self.auth_storage.try_get_auth_info_from_request(&request)?;
        let range_required_revision = range_req.revision;
        let is_serializable = range_req.serializable;
        let request = RequestWrapper::from(request.into_inner());
        let cmd = Command::new_with_auth_info(request.keys(), request, auth_info);
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

        let res = self.do_serializable(&cmd)?;
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
        debug!("Receive grpc request: {}", put_req);
        let auth_info = self.auth_storage.try_get_auth_info_from_request(&request)?;
        let is_fast_path = true;
        let (cmd_res, sync_res) = self
            .propose(request.into_inner(), auth_info, is_fast_path)
            .await?;
        let mut res = Self::parse_response_op(cmd_res.into_inner().into());
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {} for PutRequest", revision);
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
        debug!("Receive grpc request: {}", delete_range_req);
        let auth_info = self.auth_storage.try_get_auth_info_from_request(&request)?;
        let is_fast_path = true;
        let (cmd_res, sync_res) = self
            .propose(request.into_inner(), auth_info, is_fast_path)
            .await?;
        let mut res = Self::parse_response_op(cmd_res.into_inner().into());
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {} for DeleteRangeRequest", revision);
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
        debug!("Receive grpc request: {}", txn_req);
        txn_req.check_revision(
            self.kv_storage.compacted_revision(),
            self.kv_storage.revision(),
        )?;
        let auth_info = self.auth_storage.try_get_auth_info_from_request(&request)?;
        let res = if txn_req.is_read_only() {
            debug!("TxnRequest is read only");
            let is_serializable = txn_req.is_serializable();
            let request = RequestWrapper::from(request.into_inner());
            let cmd = Command::new_with_auth_info(request.keys(), request, auth_info);
            if !is_serializable {
                self.wait_read_state(&cmd).await?;
            }
            self.do_serializable(&cmd)?
        } else {
            let is_fast_path = true;
            let (cmd_res, sync_res) = self
                .propose(request.into_inner(), auth_info, is_fast_path)
                .await?;
            let mut res = Self::parse_response_op(cmd_res.into_inner().into());
            if let Some(sync_res) = sync_res {
                let revision = sync_res.revision();
                debug!("Get revision {} for TxnRequest", revision);
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
        let auth_info = self.auth_storage.try_get_auth_info_from_request(&request)?;
        let physical = req.physical;
        let request = RequestWrapper::from(request.into_inner());
        let cmd = Command::new_with_auth_info(request.keys(), request, auth_info);
        let compact_id = self.next_compact_id.fetch_add(1, Ordering::Relaxed);
        let compact_physical_fut = if physical {
            let event = Arc::new(Event::new());
            _ = self.compact_events.insert(compact_id, Arc::clone(&event));
            let listener = event.listen();
            Either::Left(listener)
        } else {
            Either::Right(async {})
        };
        let (cmd_res, _sync_res) = self.client.propose(&cmd, None, !physical).await??;
        let resp = cmd_res.into_inner();
        if timeout(self.compact_timeout, compact_physical_fut)
            .await
            .is_err()
        {
            return Err(tonic::Status::deadline_exceeded("Compact timeout"));
        }

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
