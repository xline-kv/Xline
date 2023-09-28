use std::sync::Arc;

use curp::client::Client as CurpClient;
use xline::server::{Command, KeyRange};
use xlineapi::{
    CompactionResponse, DeleteRangeResponse, PutResponse, RangeResponse, RequestWithToken,
    TxnResponse,
};

use crate::{
    error::Result,
    types::kv::{CompactionRequest, DeleteRangeRequest, PutRequest, RangeRequest, TxnRequest},
};

/// Client for KV operations.
#[derive(Clone, Debug)]
pub struct KvClient {
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The auth token
    token: Option<String>,
}

impl KvClient {
    /// New `KvClient`
    #[inline]
    pub(crate) fn new(curp_client: Arc<CurpClient<Command>>, token: Option<String>) -> Self {
        Self { curp_client, token }
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
    /// use xline_client::{error::Result, types::kv::PutRequest, Client, ClientOptions};
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
        let key_ranges = vec![KeyRange::new_one_key(request.key())];
        let propose_id = self.curp_client.gen_propose_id().await?;
        let request = RequestWithToken::new_with_token(
            xlineapi::PutRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let (cmd_res, _sync_res) = self.curp_client.propose(cmd, true).await?;
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
    /// use xline_client::{error::Result, types::kv::RangeRequest, Client, ClientOptions};
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
        let key_ranges = vec![KeyRange::new(request.key(), request.range_end())];
        let propose_id = self.curp_client.gen_propose_id().await?;
        let request = RequestWithToken::new_with_token(
            xlineapi::RangeRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let (cmd_res, _sync_res) = self.curp_client.propose(cmd, true).await?;
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
    /// use xline_client::{error::Result, types::kv::DeleteRangeRequest, Client, ClientOptions};
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
        let key_ranges = vec![KeyRange::new(request.key(), request.range_end())];
        let propose_id = self.curp_client.gen_propose_id().await?;
        let request = RequestWithToken::new_with_token(
            xlineapi::DeleteRangeRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let (cmd_res, _sync_res) = self.curp_client.propose(cmd, true).await?;
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
    ///     error::Result,
    ///     types::kv::{Compare, PutRequest, RangeRequest, TxnOp, TxnRequest, CompareResult},
    ///     Client, ClientOptions,
    /// };
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
        let key_ranges = request
            .inner
            .compare
            .iter()
            .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
            .collect();
        let propose_id = self.curp_client.gen_propose_id().await?;
        let request = RequestWithToken::new_with_token(
            xlineapi::TxnRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let (cmd_res, Some(sync_res)) = self.curp_client.propose(cmd, false).await? else {
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
    ///     error::Result,
    ///     types::kv::{CompactionRequest, PutRequest},
    ///     Client, ClientOptions,
    /// };
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
        let use_fast_path = request.physical();
        let propose_id = self.curp_client.gen_propose_id().await?;
        let request = RequestWithToken::new_with_token(
            xlineapi::CompactionRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(vec![], request, propose_id);

        let res_wrapper = if use_fast_path {
            let (cmd_res, _sync_res) = self.curp_client.propose(cmd, true).await?;
            cmd_res.into_inner()
        } else {
            let (cmd_res, Some(sync_res)) = self.curp_client.propose(cmd, false).await? else {
                unreachable!("sync_res is always Some when use_fast_path is false");
            };
            let mut res_wrapper = cmd_res.into_inner();
            res_wrapper.update_revision(sync_res.revision());
            res_wrapper
        };

        Ok(res_wrapper.into())
    }
}
