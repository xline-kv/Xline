use std::sync::Arc;

use curp::{client::Client as CurpClient, cmd::ProposeId};
use uuid::Uuid;
use xline::server::{Command, KeyRange};
use xlineapi::{DeleteRangeResponse, PutResponse, RangeResponse, RequestWithToken, TxnResponse};

use crate::{
    error::Result,
    types::kv::{DeleteRangeRequest, PutRequest, RangeRequest, Txn},
};

/// Client for KV operations.
#[derive(Clone, Debug)]
pub struct KvClient {
    /// Name of the KvClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// Auth token
    token: Option<String>,
}

impl KvClient {
    /// New `KvClient`
    #[inline]
    pub fn new(name: String, curp_client: Arc<CurpClient<Command>>, token: Option<String>) -> Self {
        Self {
            name,
            curp_client,
            token,
        }
    }

    /// Send `PutRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` failed to send request
    #[inline]
    pub async fn put(&mut self, request: PutRequest) -> Result<PutResponse> {
        let key_ranges = vec![KeyRange::new_one_key(request.key())];
        let propose_id = self.generate_propose_id();
        let request = RequestWithToken::new_with_token(
            xlineapi::PutRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let cmd_res = self.curp_client.propose(cmd).await?;
        Ok(cmd_res.decode().into())
    }

    /// Send `RangeRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` failed to send request
    #[inline]
    pub async fn range(&mut self, request: RangeRequest) -> Result<RangeResponse> {
        let key_ranges = vec![KeyRange::new(request.key(), request.range_end())];
        let propose_id = self.generate_propose_id();
        let request = RequestWithToken::new_with_token(
            xlineapi::RangeRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let cmd_res = self.curp_client.propose(cmd).await?;
        Ok(cmd_res.decode().into())
    }

    /// Send `DeleteRangeRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` failed to send request
    #[inline]
    pub async fn delete(&mut self, request: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let key_ranges = vec![KeyRange::new(request.key(), request.range_end())];
        let propose_id = self.generate_propose_id();
        let request = RequestWithToken::new_with_token(
            xlineapi::DeleteRangeRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let cmd_res = self.curp_client.propose(cmd).await?;
        Ok(cmd_res.decode().into())
    }

    /// Send `TxnRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` failed to send request
    #[inline]
    pub async fn txn(&mut self, txn: Txn) -> Result<TxnResponse> {
        let key_ranges = txn
            .req
            .compare
            .iter()
            .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
            .collect();
        let propose_id = self.generate_propose_id();
        let request = RequestWithToken::new_with_token(
            xlineapi::TxnRequest::from(txn).into(),
            self.token.clone(),
        );
        let cmd = Command::new(key_ranges, request, propose_id);
        let (cmd_res, sync_res) = self.curp_client.propose_indexed(cmd).await?;
        let mut res_wrapper = cmd_res.decode();
        res_wrapper.update_revision(sync_res.revision());
        Ok(res_wrapper.into())
    }

    /// Generate a new `ProposeId`
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }
}
