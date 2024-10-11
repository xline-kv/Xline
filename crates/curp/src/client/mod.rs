//! CURP client
//! The Curp client should be used to access the Curp service cluster, instead of using direct RPC.

/// Metrics layer
#[cfg(feature = "client-metrics")]
mod metrics;

/// Unary rpc client
mod unary;

#[allow(unused)]
/// Retry layer
mod retry;

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

/// Connect APIs
mod connect;

/// Tests for client
#[cfg(test)]
mod tests;

#[allow(clippy::module_name_repetitions)] // More conprehensive than just `Api`
pub use connect::ClientApi;

#[cfg(madsim)]
use std::sync::atomic::AtomicU64;
use std::{collections::HashMap, ops::Deref, sync::Arc};

use curp_external_api::cmd::Command;
use parking_lot::RwLock;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::debug;
use utils::config::ClientConfig;
#[cfg(madsim)]
use utils::ClientTlsConfig;

use self::{
    cluster_state::{ClusterState, ClusterStateInit},
    config::Config,
    fetch::{ConnectToCluster, Fetch},
    keep_alive::KeepAlive,
    retry::{Retry, RetryConfig},
    unary::Unary,
};
use crate::{
    member::Membership,
    members::ServerId,
    rpc::{
        self,
        connect::{BypassedConnect, ConnectApi},
        MembershipResponse, NodeMetadata, ProposeId, Protocol,
    },
    server::StreamingProtocol,
    tracker::Tracker,
};

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

/// Sets the initial cluster for the client builder
#[derive(Debug, Clone)]
enum SetCluster {
    /// Some nodes, used for discovery
    Nodes(Vec<Vec<String>>),
    /// Full cluster metadata
    Full {
        /// The leader id
        leader_id: u64,
        /// The term of current cluster
        term: u64,
        /// The cluster members
        members: HashMap<u64, Vec<String>>,
    },
}

/// Client builder to build a client
#[derive(Debug, Clone, Default)]
#[allow(clippy::module_name_repetitions)] // better than just Builder
pub struct ClientBuilder {
    /// initial cluster members
    init_cluster: Option<SetCluster>,
    /// is current client send request to raw curp server
    is_raw_curp: bool,
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

    /// Set the initial cluster
    #[inline]
    #[must_use]
    pub fn init_cluster(
        mut self,
        leader_id: u64,
        term: u64,
        members: impl IntoIterator<Item = (u64, Vec<String>)>,
    ) -> Self {
        self.init_cluster = Some(SetCluster::Full {
            leader_id,
            term,
            members: members.into_iter().collect(),
        });
        self
    }

    /// Set the initial nodes
    #[inline]
    #[must_use]
    pub fn init_nodes(mut self, nodes: impl IntoIterator<Item = Vec<String>>) -> Self {
        self.init_cluster = Some(SetCluster::Nodes(nodes.into_iter().collect()));
        self
    }

    /// Set the tls config
    #[inline]
    #[must_use]
    pub fn tls_config(mut self, tls_config: Option<ClientTlsConfig>) -> Self {
        self.tls_config = tls_config;
        self
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

    /// Build connect to closure
    fn build_connect_to(
        &self,
        bypassed: Option<(u64, Arc<dyn ConnectApi>)>,
    ) -> impl ConnectToCluster {
        let tls_config = self.tls_config.clone();
        let is_raw_curp = self.is_raw_curp;
        move |resp: &MembershipResponse| -> HashMap<u64, Arc<dyn ConnectApi>> {
            resp.nodes
                .clone()
                .into_iter()
                .map(|node| {
                    let (node_id, meta) = node.into_parts();
                    let addrs = if is_raw_curp {
                        meta.into_peer_urls()
                    } else {
                        meta.into_client_urls()
                    };
                    let connect = rpc::connect(node_id, addrs, tls_config.clone());
                    (node_id, connect)
                })
                .chain(bypassed.clone())
                .collect::<HashMap<_, _>>()
        }
    }

    /// Connect to members
    #[allow(clippy::as_conversions)] // convert usize to u64 is legal
    fn connect_members(&self, tls_config: Option<&ClientTlsConfig>) -> ClusterState {
        match self
            .init_cluster
            .clone()
            .unwrap_or_else(|| unreachable!("requires cluster to be set"))
        {
            SetCluster::Nodes(nodes) => {
                let nodes = nodes
                    .into_iter()
                    .enumerate()
                    .map(|(dummy_id, addrs)| (dummy_id as u64, addrs))
                    .collect();
                let connects = rpc::connects(nodes, tls_config)
                    .map(|(_id, conn)| conn)
                    .collect();

                ClusterState::Init(ClusterStateInit::new(connects))
            }
            SetCluster::Full {
                leader_id,
                term,
                members,
            } => {
                let connects = rpc::connects(members.clone(), tls_config).collect();
                let member_ids = members.keys().copied().collect();
                let metas = members
                    .clone()
                    .into_iter()
                    .map(|(id, addrs)| (id, NodeMetadata::new("", addrs.clone(), addrs)))
                    .collect();
                let membership = Membership::new(vec![member_ids], metas);
                let cluster_state =
                    cluster_state::ClusterStateFull::new(leader_id, term, connects, membership);

                ClusterState::Full(cluster_state)
            }
        }
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
        let fetch = Fetch::new(
            *self.config.wait_synced_timeout(),
            self.build_connect_to(None),
        );
        let cluster_state = self.connect_members(self.tls_config.as_ref());
        let client = Retry::new(
            Unary::new(config),
            self.init_retry_config(),
            keep_alive,
            fetch,
            cluster_state,
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
        let config = self.init_config(None);
        let keep_alive = KeepAlive::new(*self.config.keep_alive_interval());
        let fetch = Fetch::new(
            *self.config.wait_synced_timeout(),
            self.build_connect_to(None),
        );
        let cluster_state_init = self.connect_members(self.tls_config.as_ref());
        Retry::new_with_client_id(
            Unary::new(config),
            self.init_retry_config(),
            keep_alive,
            fetch,
            ClusterState::Init(cluster_state_init),
        )
    }
}

impl<P: Protocol + StreamingProtocol> ClientBuilderWithBypass<P> {
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
        let fetch = Fetch::new(
            *self.inner.config.wait_synced_timeout(),
            self.inner.build_connect_to(Some(bypassed)),
        );
        let cluster_state = self.inner.connect_members(self.inner.tls_config.as_ref());
        let client = Retry::new(
            Unary::new(config),
            self.inner.init_retry_config(),
            keep_alive,
            fetch,
            cluster_state,
        );

        Ok(client)
    }
}
