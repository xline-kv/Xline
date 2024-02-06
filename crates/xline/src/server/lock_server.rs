use std::sync::Arc;

use async_stream::stream;
use clippy_utilities::OverflowArithmetic;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tonic::transport::{Channel, Endpoint};
use tracing::debug;
use utils::build_endpoint;
#[cfg(madsim)]
use utils::ClientTlsConfig;
use xlineapi::{
    command::{Command, CommandResponse, CurpClient, KeyRange, SyncResponse},
    execute_error::ExecuteError,
    AuthInfo, EventType,
};

use crate::{
    id_gen::IdGenerator,
    rpc::{
        Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse,
        LeaseGrantRequest, LeaseGrantResponse, Lock, LockRequest, LockResponse, PutRequest,
        RangeRequest, RangeResponse, Request, RequestOp, RequestUnion, RequestWrapper, Response,
        ResponseHeader, SortOrder, SortTarget, TargetUnion, TxnRequest, TxnResponse, UnlockRequest,
        UnlockResponse, WatchClient, WatchCreateRequest, WatchRequest,
    },
    storage::{storage_api::StorageApi, AuthStore},
};

/// Default session ttl
const DEFAULT_SESSION_TTL: i64 = 60;

/// Lock Server
pub(super) struct LockServer<S>
where
    S: StorageApi,
{
    /// Consensus client
    client: Arc<CurpClient>,
    /// Auth store
    auth_store: Arc<AuthStore<S>>,
    /// Id Generator
    id_gen: Arc<IdGenerator>,
    /// Server addresses
    addrs: Vec<Endpoint>,
}

impl<S> LockServer<S>
where
    S: StorageApi,
{
    /// New `LockServer`
    pub(super) fn new(
        client: Arc<CurpClient>,
        auth_store: Arc<AuthStore<S>>,
        id_gen: Arc<IdGenerator>,
        addrs: &[String],
        client_tls_config: Option<&ClientTlsConfig>,
    ) -> Self {
        let addrs = addrs
            .iter()
            .map(|addr| {
                build_endpoint(addr, client_tls_config)
                    .unwrap_or_else(|_e| panic!("invalid address: {addr}"))
            })
            .collect();
        Self {
            client,
            auth_store,
            id_gen,
            addrs,
        }
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

    /// Crate txn for try acquire lock
    fn create_acquire_txn(prefix: &str, lease_id: i64) -> TxnRequest {
        let key = format!("{prefix}{lease_id:x}");
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
        auth_info: Option<&AuthInfo>,
    ) -> Result<(), tonic::Status> {
        let rev = my_rev.overflow_sub(1);
        let mut watch_client =
            WatchClient::new(Channel::balance_list(self.addrs.clone().into_iter()));
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
            let (cmd_res, _sync_res) = self.propose(get_req, auth_info.cloned(), false).await?;
            let response = Into::<RangeResponse>::into(cmd_res.into_inner());
            let last_key = match response.kvs.first() {
                Some(kv) => kv.key.clone(),
                None => return Ok(()),
            };
            let request_stream = stream! {
                yield WatchRequest {
                    request_union: Some(RequestUnion::CreateRequest(WatchCreateRequest {
                        key: last_key,
                        ..Default::default()
                    })),
                };
            };
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
        auth_info: Option<AuthInfo>,
    ) -> Result<Option<ResponseHeader>, tonic::Status> {
        let del_req = DeleteRangeRequest {
            key: key.into(),
            ..Default::default()
        };
        let (cmd_res, _) = self.propose(del_req, auth_info, true).await?;
        let res = Into::<DeleteRangeResponse>::into(cmd_res.into_inner());
        Ok(res.header)
    }

    /// Lease grant
    async fn lease_grant(&self, auth_info: Option<AuthInfo>) -> Result<i64, tonic::Status> {
        let lease_id = self.id_gen.next();
        let lease_grant_req = LeaseGrantRequest {
            ttl: DEFAULT_SESSION_TTL,
            id: lease_id,
        };
        let (cmd_res, _) = self.propose(lease_grant_req, auth_info, true).await?;
        let res = Into::<LeaseGrantResponse>::into(cmd_res.into_inner());
        Ok(res.id)
    }
}

#[tonic::async_trait]
impl<S> Lock for LockServer<S>
where
    S: StorageApi,
{
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
        let auth_info = self.auth_store.try_get_auth_info_from_request(&request)?;
        let lock_req = request.into_inner();
        let lease_id = if lock_req.lease == 0 {
            self.lease_grant(auth_info.clone()).await?
        } else {
            lock_req.lease
        };

        let prefix = format!("{}/", String::from_utf8_lossy(&lock_req.name).into_owned());
        let key = format!("{prefix}{lease_id:x}");

        let txn = Self::create_acquire_txn(&prefix, lease_id);
        let (cmd_res, sync_res) = self.propose(txn, auth_info.clone(), false).await?;
        let mut txn_res = Into::<TxnResponse>::into(cmd_res.into_inner());
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
            if let Err(e) = self.wait_delete(prefix, my_rev, auth_info.as_ref()).await {
                let _ignore = self.delete_key(key.as_bytes(), auth_info).await;
                return Err(e);
            }
            let range_req = RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            };
            let result = self.propose(range_req, auth_info.clone(), true).await;
            match result {
                Ok(res) => {
                    let res = Into::<RangeResponse>::into(res.0.into_inner());
                    if res.kvs.is_empty() {
                        return Err(ExecuteError::LeaseExpired(lease_id).into());
                    }
                    res.header
                }
                Err(e) => {
                    let _ignore = self.delete_key(key.as_bytes(), auth_info).await;
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
        let auth_info = self.auth_store.try_get_auth_info_from_request(&request)?;
        let header = self.delete_key(&request.get_ref().key, auth_info).await?;
        Ok(tonic::Response::new(UnlockResponse { header }))
    }
}
