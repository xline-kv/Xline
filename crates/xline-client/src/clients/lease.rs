use std::{fmt::Debug, sync::Arc};

use futures::channel::mpsc::channel;
use tonic::{transport::Channel, Streaming};
use xlineapi::{
    command::Command, LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse,
    LeaseRevokeResponse, LeaseTimeToLiveResponse, RequestWrapper,
};

use crate::{
    error::{Result, XlineClientError},
    lease_gen::LeaseIdGenerator,
    types::lease::LeaseKeeper,
    AuthService, CurpClient,
};

/// Client for Lease operations.
#[derive(Clone)]
pub struct LeaseClient {
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient>,
    /// The lease RPC client, only communicate with one server at a time
    #[cfg(not(madsim))]
    lease_client: xlineapi::LeaseClient<AuthService<Channel>>,
    /// The lease RPC client, only communicate with one server at a time
    #[cfg(madsim)]
    lease_client: xlineapi::LeaseClient<Channel>,
    /// Auth token
    token: Option<String>,
    /// Lease Id generator
    id_gen: Arc<LeaseIdGenerator>,
}

impl Debug for LeaseClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseClient")
            .field("lease_client", &self.lease_client)
            .field("token", &self.token)
            .field("id_gen", &self.id_gen)
            .finish()
    }
}

impl LeaseClient {
    /// Creates a new `LeaseClient`
    #[inline]
    pub fn new(
        curp_client: Arc<CurpClient>,
        channel: Channel,
        token: Option<String>,
        id_gen: Arc<LeaseIdGenerator>,
    ) -> Self {
        Self {
            curp_client,
            lease_client: xlineapi::LeaseClient::new(AuthService::new(
                channel,
                token.as_ref().and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            token,
            id_gen,
        }
    }

    /// Creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    ///
    /// `ttl` is the advisory time-to-live in seconds. Expired lease will return -1.
    /// `id` is the requested ID for the lease. If ID is set to `None` or 0, the lessor chooses an ID.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .lease_client();
    ///
    ///     let resp = client.grant(60, None).await?;
    ///     println!("lease id: {}", resp.id);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn grant(&self, ttl: i64, id: Option<i64>) -> Result<LeaseGrantResponse> {
        let mut id = id.unwrap_or_default();
        if id == 0 {
            id = self.id_gen.next();
        }
        let cmd = Command::new(RequestWrapper::from(xlineapi::LeaseGrantRequest {
            ttl,
            id,
        }));
        let (cmd_res, _sync_res) = self
            .curp_client
            .propose(&cmd, self.token.as_ref(), true)
            .await??;
        Ok(cmd_res.into_inner().into())
    }

    /// Revokes a lease. All keys attached to the lease will expire and be deleted.
    ///
    /// `id` is the lease ID to revoke. When the ID is revoked, all associated keys will be deleted.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner RPC client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .lease_client();
    ///
    ///     // granted a lease id 1
    ///
    ///     let _resp = client.revoke(1).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn revoke(&mut self, id: i64) -> Result<LeaseRevokeResponse> {
        let res = self
            .lease_client
            .lease_revoke(xlineapi::LeaseRevokeRequest { id })
            .await?;
        Ok(res.into_inner())
    }

    /// Keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    ///
    /// `id` is the lease ID for the lease to keep alive.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner RPC client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .lease_client();
    ///
    ///     // granted a lease id 1
    ///
    ///     let (mut keeper, mut stream) = client.keep_alive(1).await?;
    ///
    ///     if let Some(resp) = stream.message().await? {
    ///         println!("new ttl: {}", resp.ttl);
    ///     }
    ///
    ///     keeper.keep_alive()?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn keep_alive(
        &mut self,
        id: i64,
    ) -> Result<(LeaseKeeper, Streaming<LeaseKeepAliveResponse>)> {
        let (mut sender, receiver) = channel::<xlineapi::LeaseKeepAliveRequest>(100);

        sender
            .try_send(xlineapi::LeaseKeepAliveRequest { id })
            .map_err(|e| XlineClientError::LeaseError(e.to_string()))?;

        let mut stream = self
            .lease_client
            .lease_keep_alive(receiver)
            .await?
            .into_inner();

        let resp_id = match stream.message().await? {
            Some(resp) => resp.id,
            None => {
                return Err(XlineClientError::LeaseError(String::from(
                    "failed to create lease keeper",
                )));
            }
        };

        Ok((LeaseKeeper::new(resp_id, sender), stream))
    }

    /// Retrieves lease information.
    ///
    /// `id` is the lease ID for the lease,
    /// `keys` is true to query all the keys attached to this lease.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner RPC client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .lease_client();
    ///
    ///     // granted a lease id 1
    ///
    ///     let resp = client.time_to_live(1, false).await?;
    ///
    ///     println!("remaining ttl: {}", resp.ttl);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn time_to_live(&mut self, id: i64, keys: bool) -> Result<LeaseTimeToLiveResponse> {
        Ok(self
            .lease_client
            .lease_time_to_live(xlineapi::LeaseTimeToLiveRequest { id, keys })
            .await?
            .into_inner())
    }

    /// Lists all existing leases.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .lease_client();
    ///
    ///     for lease in client.leases().await?.leases {
    ///         println!("lease: {}", lease.id);
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn leases(&self) -> Result<LeaseLeasesResponse> {
        let request = RequestWrapper::from(xlineapi::LeaseLeasesRequest {});
        let cmd = Command::new(request);
        let (cmd_res, _sync_res) = self
            .curp_client
            .propose(&cmd, self.token.as_ref(), true)
            .await??;
        Ok(cmd_res.into_inner().into())
    }
}
