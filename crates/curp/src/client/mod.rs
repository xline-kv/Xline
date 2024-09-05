//! CURP client
//! The Curp client should be used to access the Curp service cluster, instead of using direct RPC.

/// Metrics layer
#[cfg(feature = "client-metrics")]
mod metrics;

/// Unary rpc client
mod unary;

#[cfg(ignore)]
/// Stream rpc client
mod stream;

#[allow(unused)]
/// Retry layer
mod retry;

#[allow(unused)]
/// State for clients
mod state;

#[allow(unused)]
/// State of the cluster
mod cluster_state;

#[allow(unused)]
/// Client cluster fetch implementation
mod fetch;

#[allow(unused)]
/// Config of the client
mod config;

#[allow(unused)]
/// Lease keep alive implementation
mod keep_alive;

// TODO: rewrite these tests
/// Tests for client
#[cfg(ignore)]
mod tests;

#[cfg(madsim)]
use std::sync::atomic::AtomicU64;
use std::{collections::HashMap, fmt::Debug, ops::Deref, sync::Arc, time::Duration};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::{debug, warn};
#[cfg(madsim)]
use utils::ClientTlsConfig;
use utils::{build_endpoint, config::ClientConfig};

use self::{
    cluster_state::ClusterStateInit,
    config::Config,
    fetch::Fetch,
    keep_alive::KeepAlive,
    retry::{Context, Retry, RetryConfig},
    unary::Unary,
};
use crate::{
    members::ServerId,
    rpc::{
        self,
        connect::{BypassedConnect, ConnectApi},
        protocol_client::ProtocolClient,
        ConfChange, FetchClusterRequest, FetchClusterResponse, Member, ProposeId, Protocol,
        ReadState,
    },
    tracker::Tracker,
};

/// The response of propose command, deserialized from [`crate::rpc::ProposeResponse`] or
/// [`crate::rpc::WaitSyncedResponse`].
#[allow(type_alias_bounds)] // that's not bad
pub(crate) type ProposeResponse<C: Command> = Result<(C::ER, Option<C::ASR>), C::Error>;

/// `ClientApi`, a higher wrapper for `ConnectApi`, providing some methods for communicating to
/// the whole curp cluster. Automatically discovery curp server to update it's quorum.
#[async_trait]
#[allow(clippy::module_name_repetitions)] // better than just Api
pub trait ClientApi {
    /// The client error
    type Error;

    /// The command type
    type Cmd: Command;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        token: Option<&String>, // TODO: Allow external custom interceptors, do not pass token in parameters
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
        node_client_urls: Vec<String>,
    ) -> Result<(), Self::Error>;

    /// Send move leader request
    async fn move_leader(&self, node_id: ServerId) -> Result<(), Self::Error>;

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &Self::Cmd) -> Result<ReadState, Self::Error>;

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    ///
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<FetchClusterResponse, Self::Error>;

    /// Fetch leader id
    #[inline]
    async fn fetch_leader_id(&self, linearizable: bool) -> Result<ServerId, Self::Error> {
        if linearizable {
            let resp = self.fetch_cluster(true).await?;
            return Ok(resp
                .leader_id
                .unwrap_or_else(|| {
                    unreachable!("linearizable fetch cluster should return a leader id")
                })
                .into());
        }
        let resp = self.fetch_cluster(false).await?;
        if let Some(id) = resp.leader_id {
            return Ok(id.into());
        }
        debug!("no leader id in FetchClusterResponse, try to send linearizable request");
        // fallback to linearizable fetch
        self.fetch_leader_id(true).await
    }
}

/// Propose id guard, used to ensure the sequence of propose id is recorded.
struct ProposeIdGuard<'a> {
    /// The propose id
    propose_id: ProposeId,
    /// The tracker
    tracker: &'a RwLock<Tracker>,
}

impl Deref for ProposeIdGuard<'_> {
    type Target = ProposeId;

    fn deref(&self) -> &Self::Target {
        &self.propose_id
    }
}

impl<'a> ProposeIdGuard<'a> {
    /// Create a new propose id guard
    fn new(tracker: &'a RwLock<Tracker>, propose_id: ProposeId) -> Self {
        Self {
            propose_id,
            tracker,
        }
    }
}

impl Drop for ProposeIdGuard<'_> {
    fn drop(&mut self) {
        let _ig = self.tracker.write().record(self.propose_id.1);
    }
}

/// This trait override some unrepeatable methods in ClientApi, and a client with this trait will be able to retry.
#[async_trait]
trait RepeatableClientApi {
    /// The client error
    type Error;

    /// The command type
    type Cmd: Command;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        token: Option<&String>,
        use_fast_path: bool,
        ctx: Context,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error>;

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
        ctx: Context,
    ) -> Result<Vec<Member>, Self::Error>;

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self, ctx: Context) -> Result<(), Self::Error>;

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
        ctx: Context,
    ) -> Result<(), Self::Error>;

    /// Send move leader request
    async fn move_leader(&self, node_id: u64, ctx: Context) -> Result<(), Self::Error>;

    /// Send fetch read state from leader
    async fn fetch_read_state(
        &self,
        cmd: &Self::Cmd,
        ctx: Context,
    ) -> Result<ReadState, Self::Error>;
}

/// Client builder to build a client
#[derive(Debug, Clone, Default)]
#[allow(clippy::module_name_repetitions)] // better than just Builder
pub struct ClientBuilder {
    /// initial cluster version
    cluster_version: Option<u64>,
    /// initial cluster members
    all_members: Option<HashMap<ServerId, Vec<String>>>,
    /// is current client send request to raw curp server
    is_raw_curp: bool,
    /// initial leader state
    leader_state: Option<(ServerId, u64)>,
    /// client configuration
    config: ClientConfig,
    /// Client tls config
    tls_config: Option<ClientTlsConfig>,
}

/// A client builder with bypass with local server
#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)] // same as above
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
    pub fn new(config: ClientConfig, is_raw_curp: bool) -> Self {
        Self {
            is_raw_curp,
            config,
            ..ClientBuilder::default()
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

    /// Set the tls config
    #[inline]
    #[must_use]
    pub fn tls_config(mut self, tls_config: Option<ClientTlsConfig>) -> Self {
        self.tls_config = tls_config;
        self
    }

    /// Discover the initial states from some endpoints
    ///
    /// # Errors
    ///
    /// Return `tonic::Status` for connection failure or some server errors.
    #[inline]
    pub async fn discover_from(mut self, addrs: Vec<String>) -> Result<Self, tonic::Status> {
        /// Sleep duration in secs when the cluster is unavailable
        const DISCOVER_SLEEP_DURATION: u64 = 1;
        loop {
            match self.try_discover_from(&addrs).await {
                Ok(()) => return Ok(self),
                Err(e) if matches!(e.code(), tonic::Code::Unavailable) => {
                    warn!("cluster is unavailable, sleep for {DISCOVER_SLEEP_DURATION} secs");
                    tokio::time::sleep(Duration::from_secs(DISCOVER_SLEEP_DURATION)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Discover the initial states from some endpoints
    ///
    /// # Errors
    ///
    /// Return `tonic::Status` for connection failure or some server errors.
    #[inline]
    pub async fn try_discover_from(&mut self, addrs: &[String]) -> Result<(), tonic::Status> {
        let propose_timeout = *self.config.propose_timeout();
        let mut futs: FuturesUnordered<_> = addrs
            .iter()
            .map(|addr| {
                let tls_config = self.tls_config.clone();
                async move {
                    let endpoint = build_endpoint(addr, tls_config.as_ref()).map_err(|e| {
                        tonic::Status::internal(format!("create endpoint failed, error: {e}"))
                    })?;
                    let channel = endpoint.connect().await.map_err(|e| {
                        tonic::Status::cancelled(format!("cannot connect to addr, error: {e}"))
                    })?;
                    let mut protocol_client = ProtocolClient::new(channel);
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
                    if let Some(ref id) = r.leader_id {
                        self.leader_state = Some((id.into(), r.term));
                    }
                    self.all_members = if self.is_raw_curp {
                        Some(r.into_peer_urls())
                    } else {
                        Some(Self::ensure_no_empty_address(r.into_client_urls())?)
                    };
                    return Ok(());
                }
                Err(e) => err = e,
            }
        }
        Err(err)
    }

    /// Ensures that no server has an empty list of addresses.
    fn ensure_no_empty_address(
        urls: HashMap<ServerId, Vec<String>>,
    ) -> Result<HashMap<ServerId, Vec<String>>, tonic::Status> {
        (!urls.values().any(Vec::is_empty))
            .then_some(urls)
            .ok_or(tonic::Status::unavailable("cluster not published"))
    }

    /// Init retry config
    fn init_retry_config(&self) -> RetryConfig {
        if *self.config.fixed_backoff() {
            RetryConfig::new_fixed(
                *self.config.initial_retry_timeout(),
                *self.config.retry_count(),
            )
        } else {
            RetryConfig::new_exponential(
                *self.config.initial_retry_timeout(),
                *self.config.max_retry_timeout(),
                *self.config.retry_count(),
            )
        }
    }

    /// Init unary config
    fn init_config(&self, local_server_id: Option<ServerId>) -> Config {
        Config::new(
            local_server_id,
            self.tls_config.clone(),
            *self.config.propose_timeout(),
            *self.config.wait_synced_timeout(),
            self.is_raw_curp,
        )
    }

    /// Connect to members
    fn connect_members(&self, tls_config: Option<&ClientTlsConfig>) -> ClusterStateInit {
        let all_members = self
            .all_members
            .clone()
            .unwrap_or_else(|| unreachable!("requires members"));
        let connects = rpc::connects(all_members, tls_config)
            .map(|(_id, conn)| conn)
            .collect();

        ClusterStateInit::new(connects)
    }

    /// Build the client
    ///
    /// # Errors
    ///
    /// Return `tonic::transport::Error` for connection failure.
    #[inline]
    pub fn build<C: Command>(
        &self,
    ) -> Result<impl ClientApi<Error = tonic::Status, Cmd = C> + Send + Sync + 'static, tonic::Status>
    {
        let config = self.init_config(None);
        let keep_alive = KeepAlive::new(*self.config.keep_alive_interval());
        let fetch = Fetch::new(config.clone());
        let cluster_state_init = self.connect_members(self.tls_config.as_ref());
        let client = Retry::new(
            Unary::new(config),
            self.init_retry_config(),
            keep_alive,
            fetch,
            cluster_state_init,
        );

        Ok(client)
    }

    #[cfg(madsim)]
    /// Build the client, also returns the current client id
    ///
    /// # Errors
    ///
    /// Return `tonic::transport::Error` for connection failure.
    #[inline]
    #[must_use]
    pub fn build_with_client_id<C: Command>(
        &self,
    ) -> (
        impl ClientApi<Error = tonic::Status, Cmd = C> + Send + Sync + 'static,
        Arc<AtomicU64>,
    ) {
        let state = Arc::new(self.init_state_builder().build());

        let client = Retry::new(
            Unary::new(Arc::clone(&state), self.init_unary_config()),
            self.init_retry_config(),
            Some(self.spawn_bg_tasks(Arc::clone(&state))),
        );
        let client_id = state.clone_client_id();

        (client, client_id)
    }
}

impl<P: Protocol> ClientBuilderWithBypass<P> {
    /// Build the state with local server
    pub(super) fn bypassed_connect(
        local_server_id: ServerId,
        local_server: P,
    ) -> (u64, Arc<dyn ConnectApi>) {
        debug!("client bypassed server({local_server_id})");
        let connect = BypassedConnect::new(local_server_id, local_server);
        (local_server_id, Arc::new(connect))
    }

    /// Build the client with local server
    ///
    /// # Errors
    ///
    /// Return `tonic::transport::Error` for connection failure.
    #[inline]
    pub fn build<C: Command>(
        self,
    ) -> Result<impl ClientApi<Error = tonic::Status, Cmd = C>, tonic::Status> {
        let bypassed = Self::bypassed_connect(self.local_server_id, self.local_server);
        let config = self.inner.init_config(Some(self.local_server_id));
        let keep_alive = KeepAlive::new(*self.inner.config.keep_alive_interval());
        let fetch = Fetch::new(config.clone()).with_override(bypassed);
        let cluster_state_init = self.inner.connect_members(self.inner.tls_config.as_ref());
        let client = Retry::new(
            Unary::new(config),
            self.inner.init_retry_config(),
            keep_alive,
            fetch,
            cluster_state_init,
        );

        Ok(client)
    }
}
