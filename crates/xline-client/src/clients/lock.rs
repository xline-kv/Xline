use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use clippy_utilities::OverflowArithmetic;
use futures::{Future, FutureExt};
use tonic::transport::Channel;
use xlineapi::{
    command::{Command, CommandResponse, KeyRange, SyncResponse},
    Compare, CompareResult, CompareTarget, DeleteRangeRequest, DeleteRangeResponse, EventType,
    LockResponse, PutRequest, RangeRequest, RangeResponse, Request, RequestOp, RequestWrapper,
    Response, ResponseHeader, SortOrder, SortTarget, TargetUnion, TxnRequest, TxnResponse,
    UnlockResponse,
};

use crate::{
    clients::{lease::LeaseClient, watch::WatchClient},
    error::{Result, XlineClientError},
    lease_gen::LeaseIdGenerator,
    types::{
        lease::LeaseGrantRequest,
        lock::{LockRequest, UnlockRequest},
        watch::WatchRequest,
    },
    CurpClient,
};

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
            .field("lease_client", &self.lease_client)
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

    /// Acquires a distributed shared lock on a given named lock.
    /// On success, it will return a unique key that exists so long as the
    /// lock is held by the caller. This key can be used in conjunction with
    /// transactions to safely ensure updates to Xline only occur while holding
    /// lock ownership. The lock is held until Unlock is called on the key or the
    /// lease associate with the owner expires.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{
    ///     types::lock::{LockRequest, UnlockRequest},
    ///     Client, ClientOptions,
    /// };
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .lock_client();
    ///
    ///     // acquire a lock
    ///     let resp = client
    ///         .lock(LockRequest::new("lock-test"))
    ///         .await?;
    ///
    ///     let key = resp.key;
    ///
    ///     println!("lock key: {:?}", String::from_utf8_lossy(&key));
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn lock(&self, request: LockRequest) -> Result<LockResponse> {
        let mut lease_id = request.inner.lease;
        if lease_id == 0 {
            let resp = self
                .lease_client
                .grant(LeaseGrantRequest::new(request.ttl))
                .await?;
            lease_id = resp.id;
        }
        let prefix = format!(
            "{}/",
            String::from_utf8_lossy(&request.inner.name).into_owned()
        );
        let key = format!("{prefix}{lease_id:x}");
        let lock_success = AtomicBool::new(false);
        let lock_inner = self.lock_inner(prefix, key.clone(), lease_id, &lock_success);
        tokio::pin!(lock_inner);

        LockFuture {
            key,
            lock_success: &lock_success,
            lock_client: self,
            lock_inner,
        }
        .await
    }

    /// The inner lock logic
    async fn lock_inner(
        &self,
        prefix: String,
        key: String,
        lease_id: i64,
        lock_success: &AtomicBool,
    ) -> Result<LockResponse> {
        let txn = Self::create_acquire_txn(&prefix, lease_id);
        let (cmd_res, sync_res) = self.propose(txn, false).await?;
        let mut txn_res = Into::<TxnResponse>::into(cmd_res.into_inner());
        let my_rev = sync_res
            .unwrap_or_else(|| unreachable!("sync_res always has value when use slow path"))
            .revision();
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
            self.wait_delete(prefix, my_rev).await?;
            let range_req = RangeRequest {
                key: key.as_bytes().to_vec(),
                ..Default::default()
            };
            let result = self.propose(range_req, true).await;
            match result {
                Ok(res) => {
                    let res = Into::<RangeResponse>::into(res.0.into_inner());
                    if res.kvs.is_empty() {
                        return Err(XlineClientError::RpcError(String::from("session expired")));
                    }
                    res.header
                }
                Err(e) => {
                    let _ignore = self.delete_key(key.as_bytes()).await;
                    return Err(e);
                }
            }
        };

        // The `Release` ordering ensures that this store will not be reordered.
        lock_success.store(true, Ordering::Release);

        Ok(LockResponse {
            header,
            key: key.into_bytes(),
        })
    }

    /// Takes a key returned by Lock and releases the hold on lock. The
    /// next Lock caller waiting for the lock will then be woken up and given
    /// ownership of the lock.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{
    ///     types::lock::{LockRequest, UnlockRequest},
    ///     Client, ClientOptions,
    /// };
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .lock_client();
    ///
    ///     // acquire a lock first
    ///
    ///     client.unlock(UnlockRequest::new("lock_key")).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn unlock(&self, request: UnlockRequest) -> Result<UnlockResponse> {
        let header = self.delete_key(&request.inner.key).await?;
        Ok(UnlockResponse { header })
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
        let cmd = Command::new(request.keys(), request);
        self.curp_client
            .propose(&cmd, self.token.as_ref(), use_fast_path)
            .await?
            .map_err(Into::into)
    }

    /// Create txn for try acquire lock
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
    async fn wait_delete(&self, pfx: String, my_rev: i64) -> Result<()> {
        let rev = my_rev.overflow_sub(1);
        let mut watch_client = self.watch_client.clone();
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

            let (cmd_res, _sync_res) = self.propose(get_req, false).await?;
            let response = Into::<RangeResponse>::into(cmd_res.into_inner());
            let last_key = match response.kvs.first() {
                Some(kv) => kv.key.clone(),
                None => return Ok(()),
            };
            let (_, mut response_stream) = watch_client.watch(WatchRequest::new(last_key)).await?;
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
    async fn delete_key(&self, key: &[u8]) -> Result<Option<ResponseHeader>> {
        let del_req = DeleteRangeRequest {
            key: key.into(),
            ..Default::default()
        };
        let (cmd_res, _sync_res) = self.propose(del_req, true).await?;
        let res = Into::<DeleteRangeResponse>::into(cmd_res.into_inner());
        Ok(res.header)
    }
}

/// The future that will do the lock operation
/// This exists because we need to do some clean up after the lock operation has failed or being cancelled
struct LockFuture<'a> {
    /// The key associated with the lock
    key: String,
    /// Whether the acquire attempt is success
    lock_success: &'a AtomicBool,
    /// The lock client
    lock_client: &'a LockClient,
    /// The inner lock future
    lock_inner: Pin<&'a mut (dyn Future<Output = Result<LockResponse>> + Send)>,
}

impl Debug for LockFuture<'_> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "key: {}, lock_success: {:?}, lock_client:{:?}",
            self.key, self.lock_success, self.lock_client
        )
    }
}

impl Future for LockFuture<'_> {
    type Output = Result<LockResponse>;

    #[inline]
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.lock_inner.poll_unpin(cx)
    }
}

impl Drop for LockFuture<'_> {
    #[inline]
    fn drop(&mut self) {
        // We can safely use `Relaxed` ordering here as the if condition makes sure it
        // won't be reordered.
        if self.lock_success.load(Ordering::Relaxed) {
            return;
        }
        let lock_client = self.lock_client.clone();
        let key = self.key.clone();
        let _ignore = tokio::spawn(async move {
            let _ignore = lock_client.delete_key(key.as_bytes()).await;
        });
    }
}
