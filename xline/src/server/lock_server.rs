#![allow(unused)]
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use etcd_client::{EventType, WatchOptions};
use parking_lot::Mutex;
use tokio::{sync::mpsc, time::Duration};
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;
use uuid::Uuid;

use super::{
    auth_server::get_token,
    command::{Command, CommandResponse, KeyRange, SyncResponse},
    kv_server::KvServer,
};
use crate::{
    client::errors::ClientError,
    rpc::{
        Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse,
        LeaseGrantRequest, LeaseGrantResponse, Lock, LockRequest, LockResponse, PutRequest,
        RangeRequest, RangeResponse, Request, RequestOp, RequestUnion, RequestWithToken,
        RequestWrapper, Response, ResponseHeader, SortOrder, SortTarget, TargetUnion, TxnRequest,
        TxnResponse, UnlockRequest, UnlockResponse, WatchClient, WatchCreateRequest, WatchRequest,
    },
    state::State,
    storage::KvStore,
};

/// Default session ttl
const DEFAULT_SESSION_TTL: i64 = 60;

/// Lock Server
//#[derive(Debug)]
#[allow(dead_code)] // Remove this after feature is completed
pub(crate) struct LockServer {
    /// KV storage
    storage: Arc<KvStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// State of current node
    state: Arc<State>,
    /// Server name
    name: String,
}

impl LockServer {
    /// New `LockServer`
    pub(crate) fn new(
        storage: Arc<KvStore>,
        client: Arc<Client<Command>>,
        state: Arc<State>,
        name: String,
    ) -> Self {
        Self {
            storage,
            client,
            state,
            name,
        }
    }

    /// Generate propose id
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }

    /// Generate `Command` proposal from `Request`
    fn command_from_request_wrapper(propose_id: ProposeId, wrapper: RequestWithToken) -> Command {
        #[allow(clippy::wildcard_enum_match_arm)]
        let keys = match wrapper.request {
            RequestWrapper::DeleteRangeRequest(ref req) => {
                vec![KeyRange::new(req.key.as_slice(), "")]
            }
            RequestWrapper::RangeRequest(ref req) => {
                vec![KeyRange::new(req.key.as_slice(), req.range_end.as_slice())]
            }
            RequestWrapper::TxnRequest(ref req) => req
                .compare
                .iter()
                .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
                .collect(),
            _ => vec![],
        };
        Command::new(keys, wrapper, propose_id)
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: T,
        token: Option<String>,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>), tonic::Status>
    where
        T: Into<RequestWrapper>,
    {
        let wrapper = match token {
            Some(token) => RequestWithToken::new_with_token(request.into(), token),
            None => RequestWithToken::new(request.into()),
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

    /// Crate txn for try acquire lock
    fn create_acquire_txn(prefix: &str, lease_id: i64) -> TxnRequest {
        let key = format!("{}{:x}", prefix, lease_id);
        #[allow(clippy::as_conversions)] // this cast is always safe
        let cmp = Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Create as i32,
            key: key.as_bytes().to_vec(),
            range_end: vec![],
            target_union: Some(TargetUnion::CreateRevision(0)),
        };
        let put = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: key.as_bytes().to_vec(),
                value: vec![],
                lease: lease_id,
                ..Default::default()
            })),
        };
        let get = RequestOp {
            request: Some(Request::RequestRange(RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            })),
        };
        let range_end = KeyRange::get_prefix(prefix.as_bytes());
        #[allow(clippy::as_conversions)] // this cast is always safe
        let get_owner = RequestOp {
            request: Some(Request::RequestRange(RangeRequest {
                key: prefix.as_bytes().to_vec(),
                range_end,
                sort_order: SortOrder::Ascend as i32,
                sort_target: SortTarget::Create as i32,
                limit: 1,
                ..Default::default()
            })),
        };
        TxnRequest {
            compare: vec![cmp],
            success: vec![put, get_owner.clone()],
            failure: vec![get, get_owner],
        }
    }

    /// Wait until last key deleted
    async fn wait_delete(
        &self,
        pfx: String,
        my_rev: i64,
        token: Option<&String>,
    ) -> Result<(), tonic::Status> {
        let rev = my_rev.overflow_sub(1);
        let self_addr = self.state.self_address();
        let mut watch_client = WatchClient::connect(format!("http://{self_addr}"))
            .await
            .map_err(|e| tonic::Status::internal(format!("Connect error: {e}")))?;
        loop {
            let range_end = KeyRange::get_prefix(pfx.as_bytes());
            #[allow(clippy::as_conversions)] // this cast is always safe
            let get_req = RangeRequest {
                key: pfx.as_bytes().to_vec(),
                range_end,
                limit: 1,
                sort_order: SortOrder::Descend as i32,
                sort_target: SortTarget::Create as i32,
                max_create_revision: rev,
                ..Default::default()
            };
            let (cmd_res, sync_res) = self.propose(get_req, token.cloned(), false).await?;
            let response = Into::<RangeResponse>::into(cmd_res.decode());
            let last_key = match response.kvs.first() {
                Some(kv) => kv.key.as_slice(),
                None => return Ok(()),
            };
            #[allow(clippy::unwrap_used)] // sync_res always has value when use slow path
            let response_revision = sync_res.unwrap().revision();

            let (request_sender, request_receiver) = mpsc::channel(100);
            let request_stream = ReceiverStream::new(request_receiver);
            request_sender
                .send(WatchRequest {
                    request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                        key: last_key.to_vec(),
                        ..Default::default()
                    })),
                })
                .await
                .unwrap_or_else(|e| panic!("failed to send watch request: {}", e));

            let mut response_stream = watch_client.watch(request_stream).await?.into_inner();
            while let Some(watch_res) = response_stream.message().await? {
                #[allow(clippy::as_conversions)] // this cast is always safe
                if watch_res
                    .events
                    .iter()
                    .any(|e| e.r#type == EventType::Delete as i32)
                {
                    break;
                }
            }
        }
    }

    /// Delete key
    async fn delete_key(
        &self,
        key: &[u8],
        token: Option<String>,
    ) -> Result<Option<ResponseHeader>, tonic::Status> {
        let keys = vec![KeyRange::new(key, "")];
        let del_req = DeleteRangeRequest {
            key: key.into(),
            ..Default::default()
        };
        let (cmd_res, _) = self.propose(del_req, token, true).await?;
        let res = Into::<DeleteRangeResponse>::into(cmd_res.decode());
        Ok(res.header)
    }

    /// Lease grant
    async fn lease_grant(&self, token: Option<String>) -> Result<i64, tonic::Status> {
        let lease_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {}", e))
            .as_secs()
            .cast(); // TODO: generate lease unique id
        let lease_grant_req = LeaseGrantRequest {
            ttl: DEFAULT_SESSION_TTL,
            id: lease_id,
        };
        let (cmd_res, _) = self.propose(lease_grant_req, token, true).await?;
        let res = Into::<LeaseGrantResponse>::into(cmd_res.decode());
        Ok(res.id)
    }
}

#[tonic::async_trait]
impl Lock for LockServer {
    /// Lock acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to etcd only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    async fn lock(
        &self,
        request: tonic::Request<LockRequest>,
    ) -> Result<tonic::Response<LockResponse>, tonic::Status> {
        debug!("Receive LockRequest {:?}", request);
        let token = get_token(request.metadata());
        let lock_req = request.into_inner();
        let lease_id = if lock_req.lease == 0 {
            self.lease_grant(token.clone()).await?
        } else {
            lock_req.lease
        };

        let prefix = format!("{}/", String::from_utf8_lossy(&lock_req.name).into_owned());
        let key = format!("{}{:x}", prefix, lease_id);

        let txn = Self::create_acquire_txn(&prefix, lease_id);
        let (cmd_res, sync_res) = self.propose(txn, token.clone(), false).await?;
        let mut txn_res = Into::<TxnResponse>::into(cmd_res.decode());
        #[allow(clippy::unwrap_used)] // sync_res always has value when use slow path
        let my_rev = sync_res.unwrap().revision();
        let owner_res = txn_res
            .responses
            .swap_remove(1)
            .response
            .and_then(|r| {
                if let Response::ResponseRange(res) = r {
                    Some(res)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| unreachable!("owner_resp should be a Get response"));

        let owner_key = owner_res.kvs;
        let header = if owner_key
            .get(0)
            .map_or(false, |kv| kv.create_revision == my_rev)
        {
            owner_res.header
        } else {
            if let Err(e) = self.wait_delete(prefix, my_rev, token.as_ref()).await {
                let _ignore = self.delete_key(key.as_bytes(), token).await;
                return Err(e);
            }
            let range_req = RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            };
            let result = self.propose(range_req, token.clone(), true).await;
            match result {
                Ok(res) => {
                    let res = Into::<RangeResponse>::into(res.0.decode());
                    if res.kvs.is_empty() {
                        return Err(tonic::Status::internal("session expired"));
                    }
                    res.header
                }
                Err(e) => {
                    let _ignore = self.delete_key(key.as_bytes(), token).await;
                    return Err(e);
                }
            }
        };
        let res = LockResponse {
            header,
            key: key.into_bytes(),
        };
        Ok(tonic::Response::new(res))
    }

    /// Unlock takes a key returned by Lock and releases the hold on lock. The
    /// next Lock caller waiting for the lock will then be woken up and given
    /// ownership of the lock.
    async fn unlock(
        &self,
        request: tonic::Request<UnlockRequest>,
    ) -> Result<tonic::Response<UnlockResponse>, tonic::Status> {
        debug!("Receive UnlockRequest {:?}", request);
        let token = get_token(request.metadata());
        let header = self.delete_key(&request.get_ref().key, token).await?;
        Ok(tonic::Response::new(UnlockResponse { header }))
    }
}
