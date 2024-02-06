use std::{fmt::Debug, sync::Arc};

use tonic::transport::Channel;
use xlineapi::{
    command::Command, CompactionResponse, DeleteRangeResponse, PutResponse, RangeResponse,
    RequestWrapper, TxnResponse,
};

use crate::{
    error::Result,
    types::kv::{CompactionRequest, DeleteRangeRequest, PutRequest, RangeRequest, TxnRequest},
    AuthService, CurpClient,
};

/// Client for KV operations.
#[derive(Clone)]
pub struct KvClient {
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient>,
    /// The lease RPC client, only communicate with one server at a time
    #[cfg(not(madsim))]
    kv_client: xlineapi::KvClient<AuthService<Channel>>,
    /// The lease RPC client, only communicate with one server at a time
    #[cfg(madsim)]
    kv_client: xlineapi::KvClient<Channel>,
    /// The auth token
    token: Option<String>,
}

impl Debug for KvClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvClient")
            .field("kv_client", &self.kv_client)
            .field("kv_client", &self.kv_client)
            .field("token", &self.token)
            .finish()
    }
}

impl KvClient {
    /// New `KvClient`
    #[inline]
    pub(crate) fn new(
        curp_client: Arc<CurpClient>,
        channel: Channel,
        token: Option<String>,
    ) -> Self {
        Self {
            curp_client,
            kv_client: xlineapi::KvClient::new(AuthService::new(
                channel,
                token.as_ref().and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            token,
        }
    }

    /// Put a key-value into the store
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{types::kv::PutRequest, Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .kv_client();
    ///
    ///     client.put(PutRequest::new("key1", "value1")).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn put(&self, request: PutRequest) -> Result<PutResponse> {
        let request = RequestWrapper::from(xlineapi::PutRequest::from(request));
        let cmd = Command::new(request.keys(), request);
        let (cmd_res, _sync_res) = self
            .curp_client
            .propose(&cmd, self.token.as_ref(), true)
            .await??;
        Ok(cmd_res.into_inner().into())
    }

    /// Get a range of keys from the store
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{types::kv::RangeRequest, Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .kv_client();
    ///
    ///     let resp = client.range(RangeRequest::new("key1")).await?;
    ///
    ///     if let Some(kv) = resp.kvs.first() {
    ///         println!(
    ///             "got key: {}, value: {}",
    ///             String::from_utf8_lossy(&kv.key),
    ///             String::from_utf8_lossy(&kv.value)
    ///         );
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn range(&self, request: RangeRequest) -> Result<RangeResponse> {
        let request = RequestWrapper::from(xlineapi::RangeRequest::from(request));
        let cmd = Command::new(request.keys(), request);
        let (cmd_res, _sync_res) = self
            .curp_client
            .propose(&cmd, self.token.as_ref(), true)
            .await??;
        Ok(cmd_res.into_inner().into())
    }

    /// Delete a range of keys from the store
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    /// ```no_run
    /// use xline_client::{types::kv::DeleteRangeRequest, Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .kv_client();
    ///
    ///     client
    ///         .delete(DeleteRangeRequest::new("key1").with_prev_kv(true))
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn delete(&self, request: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let request = RequestWrapper::from(xlineapi::DeleteRangeRequest::from(request));
        let cmd = Command::new(request.keys(), request);
        let (cmd_res, _sync_res) = self
            .curp_client
            .propose(&cmd, self.token.as_ref(), true)
            .await??;
        Ok(cmd_res.into_inner().into())
    }

    /// Creates a transaction, which can provide serializable writes
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{
    ///     types::kv::{Compare, PutRequest, RangeRequest, TxnOp, TxnRequest, CompareResult},
    ///     Client, ClientOptions,
    /// };
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .kv_client();
    ///
    ///     let txn_req = TxnRequest::new()
    ///         .when(&[Compare::value("key2", CompareResult::Equal, "value2")][..])
    ///         .and_then(
    ///             &[TxnOp::put(
    ///                 PutRequest::new("key2", "value3").with_prev_kv(true),
    ///             )][..],
    ///         )
    ///         .or_else(&[TxnOp::range(RangeRequest::new("key2"))][..]);
    ///
    ///     let _resp = client.txn(txn_req).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn txn(&self, request: TxnRequest) -> Result<TxnResponse> {
        let request = RequestWrapper::from(xlineapi::TxnRequest::from(request));
        let cmd = Command::new(request.keys(), request);
        let (cmd_res, Some(sync_res)) = self.curp_client.propose(&cmd,self.token.as_ref(), false).await?? else {
            unreachable!("sync_res is always Some when use_fast_path is false");
        };
        let mut res_wrapper = cmd_res.into_inner();
        res_wrapper.update_revision(sync_res.revision());
        Ok(res_wrapper.into())
    }

    /// Compacts the key-value store up to a given revision.
    /// All keys with revisions less than the given revision will be compacted.
    /// The compaction process will remove all historical versions of these keys, except for the most recent one.
    /// For example, here is a revision list: [(A, 1), (A, 2), (A, 3), (A, 4), (A, 5)].
    /// We compact at revision 3. After the compaction, the revision list will become [(A, 3), (A, 4), (A, 5)].
    /// All revisions less than 3 are deleted. The latest revision, 3, will be kept.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    ///```no_run
    /// use xline_client::{
    ///     types::kv::{CompactionRequest, PutRequest},
    ///     Client, ClientOptions,
    /// };
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .kv_client();
    ///
    ///     let resp_put = client.put(PutRequest::new("key", "val")).await?;
    ///     let rev = resp_put.header.unwrap().revision;
    ///
    ///     let _resp = client.compact(CompactionRequest::new(rev)).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn compact(&self, request: CompactionRequest) -> Result<CompactionResponse> {
        if request.physical() {
            let mut kv_client = self.kv_client.clone();
            return kv_client
                .compact(xlineapi::CompactionRequest::from(request))
                .await
                .map(tonic::Response::into_inner)
                .map_err(Into::into);
        }
        let request = RequestWrapper::from(xlineapi::CompactionRequest::from(request));
        let cmd = Command::new(request.keys(), request);
        let (cmd_res, _sync_res) = self
            .curp_client
            .propose(&cmd, self.token.as_ref(), true)
            .await??;
        Ok(cmd_res.into_inner().into())
    }
}
