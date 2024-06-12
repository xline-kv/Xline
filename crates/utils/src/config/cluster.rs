use std::collections::HashMap;

use getset::Getters;
use serde::Deserialize;

use super::{client::ClientConfig, curp::CurpConfig, server::ServerTimeout};

/// Cluster configuration object, including cluster relevant configuration fields
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct ClusterConfig {
    /// Get xline server name
    #[getset(get = "pub")]
    name: String,
    /// Xline server peer listen urls
    #[getset(get = "pub")]
    peer_listen_urls: Vec<String>,
    /// Xline server peer advertise urls
    #[getset(get = "pub")]
    peer_advertise_urls: Vec<String>,
    /// Xline server client listen urls
    #[getset(get = "pub")]
    client_listen_urls: Vec<String>,
    /// Xline server client advertise urls
    #[getset(get = "pub")]
    client_advertise_urls: Vec<String>,
    /// All the nodes in the xline cluster
    #[getset(get = "pub")]
    peers: HashMap<String, Vec<String>>,
    /// Leader node.
    #[getset(get = "pub")]
    is_leader: bool,
    /// Curp server timeout settings
    #[getset(get = "pub")]
    #[serde(default = "CurpConfig::default")]
    curp_config: CurpConfig,
    /// Curp client config settings
    #[getset(get = "pub")]
    #[serde(default = "ClientConfig::default")]
    client_config: ClientConfig,
    /// Xline server timeout settings
    #[getset(get = "pub")]
    #[serde(default = "ServerTimeout::default")]
    server_timeout: ServerTimeout,
    /// Xline server initial state
    #[getset(get = "pub")]
    #[serde(with = "state_format", default = "InitialClusterState::default")]
    initial_cluster_state: InitialClusterState,
}

impl Default for ClusterConfig {
    #[inline]
    fn default() -> Self {
        Self {
            name: "default".to_owned(),
            peer_listen_urls: vec!["http://127.0.0.1:2380".to_owned()],
            peer_advertise_urls: vec!["http://127.0.0.1:2380".to_owned()],
            client_listen_urls: vec!["http://127.0.0.1:2379".to_owned()],
            client_advertise_urls: vec!["http://127.0.0.1:2379".to_owned()],
            peers: HashMap::from([(
                "default".to_owned(),
                vec!["http://127.0.0.1:2379".to_owned()],
            )]),
            is_leader: false,
            curp_config: CurpConfig::default(),
            client_config: ClientConfig::default(),
            server_timeout: ServerTimeout::default(),
            initial_cluster_state: InitialClusterState::default(),
        }
    }
}

impl ClusterConfig {
    /// Generate a new `ClusterConfig` object
    #[must_use]
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        peer_listen_urls: Vec<String>,
        peer_advertise_urls: Vec<String>,
        client_listen_urls: Vec<String>,
        client_advertise_urls: Vec<String>,
        members: HashMap<String, Vec<String>>,
        is_leader: bool,
        curp: CurpConfig,
        client_config: ClientConfig,
        server_timeout: ServerTimeout,
        initial_cluster_state: InitialClusterState,
    ) -> Self {
        Self {
            name,
            peer_listen_urls,
            peer_advertise_urls,
            client_listen_urls,
            client_advertise_urls,
            peers: members,
            is_leader,
            curp_config: curp,
            client_config,
            server_timeout,
            initial_cluster_state,
        }
    }
}

/// Cluster Range type alias
#[allow(clippy::module_name_repetitions)]
pub type ClusterRange = std::ops::Range<u64>;

/// Initial cluster state of xline server
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub enum InitialClusterState {
    /// Create a new cluster
    #[default]
    New,
    /// Join an existing cluster
    Existing,
}

/// `InitialClusterState` deserialization formatter
pub mod state_format {
    use serde::{Deserialize, Deserializer};

    use super::InitialClusterState;
    use crate::parse_state;

    /// deserializes a cluster log rotation strategy
    #[allow(single_use_lifetimes)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<InitialClusterState, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_state(&s).map_err(serde::de::Error::custom)
    }
}
