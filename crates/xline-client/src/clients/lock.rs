use std::{fmt::Debug, sync::Arc, time::Duration};

use async_dropper::{AsyncDrop, AsyncDropper};
use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use tokio::{task::JoinHandle, time::sleep};
use tonic::transport::Channel;
use xlineapi::{
    command::{Command, CommandResponse, KeyRange, SyncResponse},
    Compare, CompareResult, CompareTarget, DeleteRangeRequest, EventType, PutRequest, RangeRequest,
    RangeResponse, Request, RequestOp, RequestWrapper, Response, ResponseHeader, SortOrder,
    SortTarget, TargetUnion, TxnRequest, TxnResponse,
};

use crate::{
    clients::{lease::LeaseClient, watch::WatchClient, DEFAULT_SESSION_TTL},
    error::{Result, XlineClientError},
    lease_gen::LeaseIdGenerator,
    types::kv::TxnRequest as KvTxnRequest,
    CurpClient,
};

/// Session represents a lease kept alive for the lifetime of a client.
#[derive(Debug)]
pub struct Session {
    /// The lock client that used to create the session
    client: LockClient,
    /// lease id
    lease_id: i64,
    /// `keep_alive` task will auto-renew the lease
    keep_alive: Option<JoinHandle<Result<()>>>,
}

impl Drop for Session {
    #[inline]
    fn drop(&mut self) {
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.abort();
        }
    }
}

/// Xutex（Xline Mutex） implements the sync lock with xline
#[derive(Debug)]
pub struct Xutex {
    /// Lock session
    session: Session,
    /// Lock
    prefix: String,
    /// Lock key
    key: String,
    /// The revision of lock key
    rev: i64,
    /// Request header
    header: Option<ResponseHeader>,
}

/// An RAII implementation of  a “scoped lock” of an `Xutex`
#[derive(Default, Debug)]
pub struct XutexGuard {
    /// The lock client that used to unlock `key` when `XutexGuard` is dropped
    client: Option<LockClient>,
    /// The key that the lock held
    key: String,
}

impl XutexGuard {
    /// Create a new `XutexGuard`
    fn new(client: LockClient, key: String) -> AsyncDropper<Self> {
        AsyncDropper::new(Self {
            client: Some(client),
            key,
        })
    }

    /// Get the key of the Xutex
    #[inline]
    #[must_use]
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Return a `TxnRequest` which will perform the success ops when the locked key is exist.
    /// This method is syntactic sugar
    #[inline]
    #[must_use]
    pub fn txn_check_locked_key(&self) -> KvTxnRequest {
        let mut txn_request = KvTxnRequest::new();
        #[allow(clippy::as_conversions)]
        let cmp = Compare {
            result: CompareResult::Greater as i32,
            target: CompareTarget::Create as i32,
            key: self.key().into(),
            range_end: Vec::new(),
            target_union: Some(TargetUnion::CreateRevision(0)),
        };
        txn_request.inner.compare.push(cmp);
        txn_request
    }
}

#[async_trait]
impl AsyncDrop for XutexGuard {
    #[inline]
    async fn async_drop(&mut self) {
        if let Some(ref client) = self.client {
            let _ignore = client.delete_key(self.key.as_bytes()).await;
        }
    }
}

impl Xutex {
    /// Create an Xutex
    ///
    /// # Errors
    ///
    /// Return errors when the lease client failed to grant a lease
    #[inline]
    pub async fn new(
        client: LockClient,
        prefix: &str,
        ttl: Option<i64>,
        lease_id: Option<i64>,
    ) -> Result<Self> {
        let ttl = ttl.unwrap_or(DEFAULT_SESSION_TTL);
        let lease_id = if let Some(id) = lease_id {
            id
        } else {
            let lease_response = client.lease_client.grant(ttl, None).await?;
            lease_response.id
        };
        let mut lease_client = client.lease_client.clone();
        let keep_alive = Some(tokio::spawn(async move {
            /// The renew interval factor of which value equals 60% of one second.
            const RENEW_INTERVAL_FACTOR: u64 = 600;
            let (mut keeper, mut stream) = lease_client.keep_alive(lease_id).await?;
            loop {
                keeper.keep_alive()?;
                if let Some(resp) = stream.message().await? {
                    if resp.ttl < 0 {
                        return Err(XlineClientError::InvalidArgs(String::from(
                            "lease keepalive response has negative ttl",
                        )));
                    }
                    sleep(Duration::from_millis(
                        resp.ttl.unsigned_abs().overflow_mul(RENEW_INTERVAL_FACTOR),
                    ))
                    .await;
                }
            }
        }));

        let session = Session {
            client,
            lease_id,
            keep_alive,
        };

        Ok(Self {
            session,
            prefix: format!("{prefix}/"),
            key: String::new(),
            rev: -1,
            header: None,
        })
    }

    /// try to acquire lock
    async fn try_acquire(&mut self) -> Result<TxnResponse> {
        let lease_id = self.session.lease_id;
        let prefix = self.prefix.as_str();
        self.key = format!("{prefix}{lease_id:x}");
        #[allow(clippy::as_conversions)] // this cast is always safe
        let cmp = Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Create as i32,
            key: self.key.as_bytes().to_vec(),
            range_end: vec![],
            target_union: Some(TargetUnion::CreateRevision(0)),
        };
        let put = RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: self.key.as_bytes().to_vec(),
                value: vec![],
                lease: lease_id,
                ..Default::default()
            })),
        };
        let get = RequestOp {
            request: Some(Request::RequestRange(RangeRequest {
                key: self.key.as_bytes().to_vec(),
                ..Default::default()
            })),
        };
        let range_end = KeyRange::get_prefix(prefix);
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
        let acquire_txn = TxnRequest {
            compare: vec![cmp],
            success: vec![put, get_owner.clone()],
            failure: vec![get, get_owner],
        };
        let (cmd_res, sync_res) = self.session.client.propose(acquire_txn, false).await?;
        let resp = Into::<TxnResponse>::into(cmd_res.into_inner());
        self.rev = if resp.succeeded {
            sync_res
                .unwrap_or_else(|| unreachable!("sync_res always has value when use slow path"))
                .revision()
        } else {
            #[allow(clippy::indexing_slicing)]
            // it's safe to do since the txn response must have two responses.
            if let Some(Response::ResponseRange(ref res)) = resp.responses[0].response {
                res.kvs[0].create_revision
            } else {
                unreachable!("The first response in txn responses should be a RangeResponse when txn failed: {:?}", resp);
            }
        };
        Ok(resp)
    }

    /// Acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to Xline only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    ///
    /// NOTES. Due to the inherent insecurity of distributed locks, it is difficult to balance efficiency
    ///  and correctness. You cannot have your cake and eat it too. That’s why this method is named `lock_unsafe`.
    /// The term 'unsafe' in this context has a different meaning compared to 'unsafe' in Rust. On the grounds of
    /// safety, we recommend users use transactions（`txn_check_locked_key`） to operate Xline while holding the lock.
    /// FYI: [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Panics
    ///
    /// Panic if the given `LockRequest.inner.lease` less than or equal to 0
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use xline_client::{
    ///     clients::Xutex,
    ///     types::kv::{Compare, CompareResult, PutOptions, TxnOp},
    ///     Client, ClientOptions,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default()).await?;
    ///
    ///     let lock_client = client.lock_client();
    ///     let kv_client = client.kv_client();
    ///
    ///     let mut xutex = Xutex::new(lock_client, "lock-test", None, None).await?;
    ///     // when the `xutex_guard` drop, the lock will be unlocked.
    ///     let xutex_guard = xutex.lock_unsafe().await?;
    ///     let txn_req = xutex_guard
    ///         .txn_check_locked_key()
    ///         .when([Compare::value("key2", CompareResult::Equal, "value2")])
    ///         .and_then([TxnOp::put("key2", "value3", Some(PutOptions::default().with_prev_kv(true)))])
    ///         .or_else(&[]);
    ///
    ///     let _resp = kv_client.txn(txn_req).await?;
    ///     // the lock will be released when the lock session is dropped.
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn lock_unsafe(&mut self) -> Result<AsyncDropper<XutexGuard>> {
        if self
            .session
            .keep_alive
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Err(XlineClientError::LeaseError(String::from(
                "Lock renew task exists unexpectedly",
            )));
        }
        let resp = self.try_acquire().await?;
        #[allow(clippy::indexing_slicing)]
        if let Some(Response::ResponseRange(ref lock_owner)) = resp.responses[1].response {
            // if no key on prefix or the key is created by current Xutex, that indicates we have already held the lock
            if lock_owner
                .kvs
                .get(0)
                .map_or(false, |kv| kv.create_revision == self.rev)
            {
                self.header = resp.header;
                return Ok(XutexGuard::new(
                    self.session.client.clone(),
                    self.key.clone(),
                ));
            }
        } else {
            unreachable!("owner_resp should be a Get response")
        }

        self.session
            .client
            .wait_delete(self.prefix.clone(), self.rev)
            .await?;
        // make sure the session is no expired, and the owner key still exists.
        let range_req = RangeRequest {
            key: self.key.as_bytes().to_vec(),
            ..Default::default()
        };
        match self.session.client.propose(range_req, true).await {
            Ok((cmd_res, _sync_res)) => {
                let res = Into::<RangeResponse>::into(cmd_res.into_inner());
                if res.kvs.is_empty() {
                    return Err(XlineClientError::RpcError(String::from("session expired")));
                }
                self.header = res.header;
                Ok(XutexGuard::new(
                    self.session.client.clone(),
                    self.key.clone(),
                ))
            }
            Err(e) => {
                self.session.client.delete_key(self.key.as_bytes()).await?;
                Err(e)
            }
        }
    }
}

/// Client for Lock operations.
#[derive(Clone)]
pub struct LockClient {
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient>,
    /// The lease client
    lease_client: LeaseClient,
    /// The watch client
    watch_client: WatchClient,
    /// Auth token
    token: Option<String>,
}

impl Debug for LockClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockClient")
            .field("watch_client", &self.watch_client)
            .field("token", &self.token)
            .finish()
    }
}

// These methods primarily originate from xline lock server,
// see also: `xline/src/server/lock_server.rs`
impl LockClient {
    /// Creates a new `LockClient`
    #[inline]
    pub fn new(
        curp_client: Arc<CurpClient>,
        channel: Channel,
        token: Option<String>,
        id_gen: Arc<LeaseIdGenerator>,
    ) -> Self {
        Self {
            curp_client: Arc::clone(&curp_client),
            lease_client: LeaseClient::new(curp_client, channel.clone(), token.clone(), id_gen),
            watch_client: WatchClient::new(channel, token.clone()),
            token,
        }
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: T,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>)>
    where
        T: Into<RequestWrapper>,
    {
        let request = request.into();
        let cmd = Command::new(request);
        self.curp_client
            .propose(&cmd, self.token.as_ref(), use_fast_path)
            .await?
            .map_err(Into::into)
    }

    /// Wait until last key deleted
    async fn wait_delete(&self, pfx: String, my_rev: i64) -> Result<()> {
        let rev = my_rev.overflow_sub(1);
        let mut watch_client = self.watch_client.clone();
        loop {
            let range_end = KeyRange::get_prefix(&pfx);
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

            let (cmd_res, _sync_res) = self.propose(get_req, false).await?;
            let response = Into::<RangeResponse>::into(cmd_res.into_inner());
            let last_key = match response.kvs.first() {
                Some(kv) => kv.key.clone(),
                None => return Ok(()),
            };
            let (_, mut response_stream) = watch_client.watch(last_key, None).await?;
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
    async fn delete_key(&self, key: &[u8]) -> Result<()> {
        let del_req = DeleteRangeRequest {
            key: key.into(),
            ..Default::default()
        };
        let (_cmd_res, _sync_res) = self.propose(del_req, true).await?;
        Ok(())
    }
}
