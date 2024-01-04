//! CURP client
//! The Curp client should be used to access the Curp service cluster, instead of using direct RPC.

/// Unary rpc client
mod unary;

/// Retry layer
mod retry;

/// Tests for client
#[cfg(test)]
mod tests;

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::{stream::FuturesUnordered, StreamExt};
use utils::config::ClientConfig;

use self::{
    retry::Retry,
    unary::{UnaryBuilder, UnaryConfig},
};
use crate::{
    client_new::retry::RetryConfig,
    members::ServerId,
    rpc::{
        connect::ConnectApi, protocol_client::ProtocolClient, ConfChange, FetchClusterRequest,
        FetchClusterResponse, Member, Protocol, ReadState,
    },
};

/// The response of propose command, deserialized from [`crate::rpc::ProposeResponse`] or
/// [`crate::rpc::WaitSyncedResponse`].
#[allow(type_alias_bounds)] // that's not bad
type ProposeResponse<C: Command> = Result<(C::ER, Option<C::ASR>), C::Error>;

/// `ClientApi`, a higher wrapper for `ConnectApi`, providing some methods for communicating to
/// the whole curp cluster. Automatically discovery curp server to update it's quorum.
#[async_trait]
pub trait ClientApi {
    /// The client error
    type Error;

    /// The command type
    type Cmd: Command;

    /// Get the local connection when the client is on the server node.
    async fn local_connect(&self) -> Option<Arc<dyn ConnectApi>>;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error>;

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, Self::Error>;

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), Self::Error>;

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
    ) -> Result<(), Self::Error>;

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &Self::Cmd) -> Result<ReadState, Self::Error>;

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<FetchClusterResponse, Self::Error>;

    /// Fetch leader id
    #[inline]
    async fn fetch_leader_id(&self, linearizable: bool) -> Result<ServerId, Self::Error> {
        if linearizable {
            let resp = self.fetch_cluster(true).await?;
            return Ok(resp.leader_id.unwrap_or_else(|| {
                unreachable!("linearizable fetch cluster should return a leader id")
            }));
        }
        let resp = self.fetch_cluster(false).await?;
        if let Some(id) = resp.leader_id {
            return Ok(id);
        }
        // fallback to linearizable fetch
        self.fetch_leader_id(true).await
    }
}

/// Update leader state
#[async_trait]
trait LeaderStateUpdate {
    /// update
    async fn update_leader(&self, leader_id: Option<ServerId>, term: u64) -> bool;
}

/// Client builder to build a client
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    /// initial cluster version
    cluster_version: Option<u64>,
    /// initial cluster members
    all_members: Option<HashMap<ServerId, Vec<String>>>,
    /// initial leader state
    leader_state: Option<(ServerId, u64)>,
    /// client configuration
    config: ClientConfig,
}

/// A client builder with bypass with local server
#[derive(Debug, Clone)]
pub struct ClientBuilderWithBypass<P: Protocol> {
    /// inner builder
    inner: ClientBuilder,
    /// local server id
    local_server_id: ServerId,
    /// local server
    local_server: P,
}

impl ClientBuilder {
    /// Create a client builder
    #[inline]
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self {
            cluster_version: None,
            all_members: None,
            leader_state: None,
            config,
        }
    }

    /// Set the local server to bypass `gRPC` request
    #[inline]
    #[must_use]
    pub fn bypass<P: Protocol>(
        self,
        local_server_id: ServerId,
        local_server: P,
    ) -> ClientBuilderWithBypass<P> {
        ClientBuilderWithBypass {
            inner: self,
            local_server_id,
            local_server,
        }
    }

    /// Set the initial cluster version
    #[inline]
    #[must_use]
    pub fn cluster_version(mut self, cluster_version: u64) -> Self {
        self.cluster_version = Some(cluster_version);
        self
    }

    /// Set the initial all members
    #[inline]
    #[must_use]
    pub fn all_members(mut self, all_members: HashMap<ServerId, Vec<String>>) -> Self {
        self.all_members = Some(all_members);
        self
    }

    /// Set the initial leader state
    #[inline]
    #[must_use]
    pub fn leader_state(mut self, leader_id: ServerId, term: u64) -> Self {
        self.leader_state = Some((leader_id, term));
        self
    }

    /// Discover the initial states from some endpoints
    ///
    /// # Errors
    ///
    /// Return `tonic::Status` for connection failure or some server errors.
    #[inline]
    pub async fn discover_from(mut self, addrs: Vec<String>) -> Result<Self, tonic::Status> {
        let propose_timeout = *self.config.propose_timeout();
        let mut futs: FuturesUnordered<_> = addrs
            .into_iter()
            .map(|mut addr| {
                if !addr.starts_with("http://") {
                    addr.insert_str(0, "http://");
                }
                async move {
                    let mut protocol_client = ProtocolClient::connect(addr).await.map_err(|e| {
                        tonic::Status::cancelled(format!("cannot connect to addr, error: {e}"))
                    })?;
                    let mut req = tonic::Request::new(FetchClusterRequest::default());
                    req.set_timeout(propose_timeout);
                    let fetch_cluster_res = protocol_client.fetch_cluster(req).await?.into_inner();
                    Ok::<FetchClusterResponse, tonic::Status>(fetch_cluster_res)
                }
            })
            .collect();
        let mut err = tonic::Status::invalid_argument("addrs is empty");
        // find the first one return `FetchClusterResponse`
        while let Some(r) = futs.next().await {
            match r {
                Ok(r) => {
                    self.cluster_version = Some(r.cluster_version);
                    if let Some(id) = r.leader_id {
                        self.leader_state = Some((id, r.term));
                    }
                    self.all_members = Some(r.into_members_addrs());
                    return Ok(self);
                }
                Err(e) => err = e,
            }
        }
        Err(err)
    }

    /// Init an unary builder
    fn init_unary_builder(&self) -> UnaryBuilder {
        let mut builder = UnaryBuilder::new(
            self.all_members.clone().unwrap_or_else(|| {
                unreachable!("must set the initial members or discover from some endpoints")
            }),
            UnaryConfig::new(
                *self.config.propose_timeout(),
                *self.config.wait_synced_timeout(),
            ),
        );
        if let Some(version) = self.cluster_version {
            builder.set_cluster_version(version);
        }
        if let Some((id, term)) = self.leader_state {
            builder.set_leader_state(id, term);
        }
        builder
    }

    /// Init retry config
    fn init_retry_config(&self) -> RetryConfig {
        if *self.config.use_backoff() {
            RetryConfig::new_exponential(
                *self.config.initial_retry_timeout(),
                *self.config.max_retry_timeout(),
                *self.config.retry_count(),
            )
        } else {
            RetryConfig::new_fixed(
                *self.config.initial_retry_timeout(),
                *self.config.retry_count(),
            )
        }
    }

    /// Build the client
    ///
    /// # Errors
    ///
    /// Return `tonic::transport::Error` for connection failure.
    #[inline]
    pub async fn build<C: Command>(
        &self,
    ) -> Result<
        impl ClientApi<Error = tonic::Status, Cmd = C> + Send + Sync + 'static,
        tonic::transport::Error,
    > {
        let unary = self.init_unary_builder().build::<C>().await?;
        let client = Retry::new(unary, self.init_retry_config());
        Ok(client)
    }
}

impl<P: Protocol> ClientBuilderWithBypass<P> {
    /// Build the client with local server
    ///
    /// # Errors
    ///
    /// Return `tonic::transport::Error` for connection failure.
    #[inline]
    pub async fn build<C: Command>(
        self,
    ) -> Result<impl ClientApi<Error = tonic::Status, Cmd = C>, tonic::transport::Error> {
        let unary = self
            .inner
            .init_unary_builder()
            .build_bypassed::<C, P>(self.local_server_id, self.local_server)
            .await?;
        let client = Retry::new(unary, self.inner.init_retry_config());
        Ok(client)
    }
}
