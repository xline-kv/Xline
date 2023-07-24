use std::sync::Arc;

use curp::{client::Client as CurpClient, cmd::ProposeId};
use futures::channel::mpsc::channel;
use tonic::{transport::Channel, Streaming};
use uuid::Uuid;
use xline::server::Command;
use xlineapi::{
    LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse, LeaseRevokeResponse,
    LeaseTimeToLiveResponse, RequestWithToken,
};

use crate::{
    error::{ClientError, Result},
    lease_gen::LeaseIdGenerator,
    types::lease::{
        LeaseGrantRequest, LeaseKeepAliveRequest, LeaseKeeper, LeaseRevokeRequest,
        LeaseTimeToLiveRequest,
    },
    AuthService,
};

/// Client for Lease operations.
#[derive(Clone, Debug)]
pub struct LeaseClient {
    /// Name of the LeaseClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The lease RPC client, only communicate with one server at a time
    lease_client: xlineapi::LeaseClient<AuthService<Channel>>,
    /// Auth token
    token: Option<String>,
    /// Lease Id generator
    id_gen: Arc<LeaseIdGenerator>,
}

impl LeaseClient {
    /// New `LeaseClient`
    #[inline]
    pub fn new(
        name: String,
        curp_client: Arc<CurpClient<Command>>,
        channel: Channel,
        token: Option<String>,
        id_gen: Arc<LeaseIdGenerator>,
    ) -> Self {
        Self {
            name,
            curp_client,
            lease_client: xlineapi::LeaseClient::new(AuthService::new(
                channel,
                token.as_ref().and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            token,
            id_gen,
        }
    }

    /// Send `LeaseGrantRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If `CurpClient` failed to send request
    #[inline]
    pub async fn grant(&self, mut request: LeaseGrantRequest) -> Result<LeaseGrantResponse> {
        let propose_id = self.generate_propose_id();
        if request.inner.id == 0 {
            request.inner.id = self.id_gen.next();
        }
        let request = RequestWithToken::new_with_token(
            xlineapi::LeaseGrantRequest::from(request).into(),
            self.token.clone(),
        );
        let cmd = Command::new(vec![], request, propose_id);
        let cmd_res = self.curp_client.propose(cmd).await?;
        Ok(cmd_res.decode().into())
    }

    /// Send `LeaseRevokeRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If client failed to send request
    #[inline]
    pub async fn revoke(&mut self, request: LeaseRevokeRequest) -> Result<LeaseRevokeResponse> {
        let res = self.lease_client.lease_revoke(request.inner).await?;
        Ok(res.into_inner())
    }

    /// Send `LeaseKeepAliveRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If client failed to send request
    #[inline]
    pub async fn keep_alive(
        &mut self,
        request: LeaseKeepAliveRequest,
    ) -> Result<(LeaseKeeper, Streaming<LeaseKeepAliveResponse>)> {
        let (mut sender, receiver) = channel::<xlineapi::LeaseKeepAliveRequest>(100);

        sender
            .try_send(request.into())
            .map_err(|e| ClientError::LeaseError(e.to_string()))?;

        let mut stream = self
            .lease_client
            .lease_keep_alive(receiver)
            .await?
            .into_inner();

        let id = match stream.message().await? {
            Some(resp) => resp.id,
            None => {
                return Err(ClientError::LeaseError(String::from(
                    "failed to create lease keeper",
                )));
            }
        };

        Ok((LeaseKeeper::new(id, sender), stream))
    }

    /// Send `LeaseTimeToLiveRequest` by `CurpClient`
    ///
    /// # Errors
    ///
    /// If client failed to send request
    #[inline]
    pub async fn time_to_live(
        &mut self,
        request: LeaseTimeToLiveRequest,
    ) -> Result<LeaseTimeToLiveResponse> {
        Ok(self
            .lease_client
            .lease_time_to_live(xlineapi::LeaseTimeToLiveRequest::from(request))
            .await?
            .into_inner())
    }

    /// Lists all existing leases.
    ///
    /// # Errors
    ///
    /// If client failed to send request
    #[inline]
    pub async fn leases(&self) -> Result<LeaseLeasesResponse> {
        let propose_id = self.generate_propose_id();
        let request = RequestWithToken::new_with_token(
            xlineapi::LeaseLeasesRequest {}.into(),
            self.token.clone(),
        );
        let cmd = Command::new(vec![], request, propose_id);
        let cmd_res = self.curp_client.propose(cmd).await?;
        Ok(cmd_res.decode().into())
    }

    /// Generate a new `ProposeId`
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }
}
