use anyhow::{anyhow, Result};
use getset::Getters;
use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf, str::FromStr, time::Duration};

/// Xline server configuration object
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct XlineServerConfig {
    /// cluster configuration object
    #[getset(get = "pub")]
    cluster: ClusterConfig,
    /// log configuration object
    #[getset(get = "pub")]
    log: LogConfig,
    /// trace configuration object
    #[getset(get = "pub")]
    trace: TraceConfig,
    /// auth configuration object
    #[getset(get = "pub")]
    auth: AuthConfig,
}

/// Duration Wrapper for meeting the orphan rule
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ClusterDuration(Duration);



impl FromStr for ClusterDuration {
    type Err = anyhow::Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with("us") {
            if let Some(dur) = s.strip_suffix("us") {
                Ok(ClusterDuration(Duration::from_micros(dur.parse()?)))
            } else {
                Err(anyhow!(format!("Failed to parse {s} to ClusterDuration")))
            }
        } else if s.ends_with("ms") {
            if let Some(dur) = s.strip_suffix("ms") {
                Ok(ClusterDuration(Duration::from_millis(dur.parse()?)))
            } else {
                Err(anyhow!(format!("Failed to parse {s} to ClusterDuration")))
            }
        } else if s.ends_with('s') {
            if let Some(dur) = s.strip_suffix('s') {
                Ok(ClusterDuration(Duration::from_secs(dur.parse()?)))
            } else {
                Err(anyhow!(format!("Failed to parse {s} to ClusterDuration")))
            }
        } else {
            Err(anyhow!(format!("Invalid time unit:{s}")))
        }
    }
}

/// Cluster Duration operations
pub mod cluster_duration_utils {
    use super::ClusterDuration;
    use serde::{self, Deserialize, Deserializer};
    use std::{str::FromStr, time::Duration};

    /// deseralizes a cluster duration
    #[allow(single_use_lifetimes)] // TODO: Think is it necessary to allow this clippy??
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<ClusterDuration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ClusterDuration::from_str(&s).map_err(serde::de::Error::custom)
    }

    /// default heartbeat interval
    #[must_use]
    #[inline]
    pub fn default_heartbeat_interval() -> ClusterDuration {
        ClusterDuration(Duration::from_millis(150))
    }

    /// default wait synced timeout
    #[must_use]
    #[inline]
    pub fn default_server_wait_synced_timeout() -> ClusterDuration {
        ClusterDuration(Duration::from_secs(5))
    }

    /// default retry timeout
    #[must_use]
    #[inline]
    pub fn default_retry_timeout() -> ClusterDuration {
        ClusterDuration(Duration::from_millis(800))
    }

    /// default rpc timeout
    #[must_use]
    #[inline]
    pub fn default_rpc_timeout() -> ClusterDuration {
        ClusterDuration(Duration::from_millis(50))
    }

    /// default candidate timeout
    #[must_use]
    #[inline]
    pub fn default_candidate_timeout() -> ClusterDuration {
        ClusterDuration(Duration::from_secs(1))
    }

    /// default crup client timeout
    #[must_use]
    #[inline]
    pub fn default_client_timeout() -> ClusterDuration {
        ClusterDuration(Duration::from_secs(1))
    }

    /// default client wait synced timeout
    #[must_use]
    #[inline]
    pub fn default_client_wait_synced_timeout() -> ClusterDuration {
        ClusterDuration(Duration::from_secs(2))
    }
}

/// Cluster configuration object, including cluster relevant configuration fields
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct ClusterConfig {
    /// Get xline server name
    #[getset(get = "pub")]
    name: String,
    /// All the nodes in the xline cluster
    #[getset(get = "pub")]
    members: HashMap<String, String>,
    /// Leader node.
    #[getset(get = "pub")]
    is_leader: bool,
    /// Curp server timeout settings
    #[getset(get = "pub")]
    server_timeout: ServerTimeout,
    /// Curp client timeout settings
    #[getset(get = "pub")]
    client_timeout: ClientTimeout,
}

impl ClusterConfig {
    /// Generate a new `ClusterConfig` object
    #[must_use]
    #[inline]
    pub fn new(
        name: String,
        members: HashMap<String, String>,
        is_leader: bool,
        server_timeout: ServerTimeout,
        client_timeout: ClientTimeout,
    ) -> Self {
        Self {
            name,
            members,
            is_leader,
            server_timeout,
            client_timeout,
        }
    }
}

/// Curp server timeout settings
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct ServerTimeout {
    /// Heartbeat Interval
    #[getset(get = "pub")]
    #[serde(
        with = "cluster_duration_utils",
        default = "cluster_duration_utils::default_heartbeat_interval"
    )]
    heartbeat_interval: ClusterDuration,
    /// Curp wait sync timeout
    #[getset(get = "pub")]
    #[serde(
        with = "cluster_duration_utils",
        default = "cluster_duration_utils::default_server_wait_synced_timeout"
    )]
    wait_synced_timeout: ClusterDuration,

    /// Curp propose retry timeout
    #[getset(get = "pub")]
    #[serde(
        with = "cluster_duration_utils",
        default = "cluster_duration_utils::default_retry_timeout"
    )]
    retry_timeout: ClusterDuration,

    /// Curp rpc timeout
    #[getset(get = "pub")]
    #[serde(
        with = "cluster_duration_utils",
        default = "cluster_duration_utils::default_rpc_timeout"
    )]
    rpc_timeout: ClusterDuration,

    /// Candidate election timeout
    #[getset(get = "pub")]
    #[serde(
        with = "cluster_duration_utils",
        default = "cluster_duration_utils::default_candidate_timeout"
    )]
    candidate_timeout: ClusterDuration,
}

impl ServerTimeout {
    /// Create a new server timeout
    #[must_use]
    #[inline]
    pub fn new(
        heartbeat_interval: ClusterDuration,
        wait_synced_timeout: ClusterDuration,
        retry_timeout: ClusterDuration,
        rpc_timeout: ClusterDuration,
        candidate_timeout: ClusterDuration,
    ) -> Self {
        Self {
            heartbeat_interval,
            wait_synced_timeout,
            retry_timeout,
            rpc_timeout,
            candidate_timeout,
        }
    }
}

/// Curp client timeout settings
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct ClientTimeout {
    /// Curp client timeout settings
    #[getset(get = "pub")]
    #[serde(
        with = "cluster_duration_utils",
        default = "cluster_duration_utils::default_client_timeout"
    )]
    timeout: ClusterDuration,

    /// Curp cliet wait sync timeout
    #[getset(get = "pub")]
    #[serde(
        with = "cluster_duration_utils",
        default = "cluster_duration_utils::default_client_wait_synced_timeout"
    )]
    wait_synced_timeout: ClusterDuration,
}

impl ClientTimeout {
    /// Create a new client timeout
    #[must_use]
    #[inline]
    pub fn new(timeout: ClusterDuration, wait_synced_timeout: ClusterDuration) -> Self {
        Self {
            timeout,
            wait_synced_timeout,
        }
    }
}

/// Cluster configuration object
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct LogConfig {
    /// Log file path
    #[getset(get = "pub")]
    path: PathBuf,
    /// Log rotation strategy
    #[getset(get = "pub")]
    rotation: RotationConfig,
    /// Log verbosity level
    #[getset(get = "pub")]
    level: LevelConfig,
}

impl LogConfig {
    /// Generate a new `LogConfig` object
    #[must_use]
    #[inline]
    pub fn new(path: PathBuf, rotation: RotationConfig, level: LevelConfig) -> Self {
        Self {
            path,
            rotation,
            level,
        }
    }
}

/// Xline log rotation strategy
#[non_exhaustive]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
pub enum RotationConfig {
    /// Rotate log file in every hour
    Hourly,
    /// Rotate log file every day
    Daily,
    /// Never rotate log file
    Never,
}

impl FromStr for RotationConfig {
    type Err = anyhow::Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hourly" => Ok(RotationConfig::Hourly),
            "daily" => Ok(RotationConfig::Daily),
            "never" => Ok(RotationConfig::Never),
            _ => Err(anyhow!(format!("Failed to parse {s} to RotationConfig"))),
        }
    }
}

/// A verbosity level configuration field, including log and tracing.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
#[allow(clippy::missing_docs_in_private_items, missing_docs)] // The meaning of every variant is quite straightforward, it's ok to ignore doc here.
#[non_exhaustive]
pub enum LevelConfig {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl FromStr for LevelConfig {
    type Err = anyhow::Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trace" => Ok(LevelConfig::Trace),
            "debug" => Ok(LevelConfig::Debug),
            "info" => Ok(LevelConfig::Info),
            "warn" => Ok(LevelConfig::Warn),
            "error" => Ok(LevelConfig::Error),
            _ => Err(anyhow!(format!("Failed to parse {s} to LevelConfig"))),
        }
    }
}

/// Xline tracing configuration object
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct TraceConfig {
    /// Open jaeger online, sending data to jaeger agent directly
    #[getset(get = "pub")]
    jaeger_online: bool,
    /// Open jaeger offline, saving data to the `jaeger_output_dir`
    #[getset(get = "pub")]
    jaeger_offline: bool,
    /// The dir path to save the data when `jaeger_offline` is on
    #[getset(get = "pub")]
    jaeger_output_dir: PathBuf,
    /// The verbosity level of tracing
    #[getset(get = "pub")]
    jaeger_level: LevelConfig,
}

impl TraceConfig {
    /// Generate a new `TraceConfig` object
    #[must_use]
    #[inline]
    pub fn new(
        jaeger_online: bool,
        jaeger_offline: bool,
        jaeger_output_dir: PathBuf,
        jaeger_level: LevelConfig,
    ) -> Self {
        Self {
            jaeger_online,
            jaeger_offline,
            jaeger_output_dir,
            jaeger_level,
        }
    }
}

/// Xline tracing configuration object
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct AuthConfig {
    /// The public key file
    #[getset(get = "pub")]
    auth_public_key: Option<PathBuf>,
    /// The private key file
    #[getset(get = "pub")]
    auth_private_key: Option<PathBuf>,
}

impl AuthConfig {
    /// Generate a new `AuthConfig` object
    #[must_use]
    #[inline]
    pub fn new(auth_public_key: Option<PathBuf>, auth_private_key: Option<PathBuf>) -> Self {
        Self {
            auth_public_key,
            auth_private_key,
        }
    }
}

impl XlineServerConfig {
    /// Generates a new `XlineServerConfig` object
    #[must_use]
    #[inline]
    pub fn new(
        cluster: ClusterConfig,
        log: LogConfig,
        trace: TraceConfig,
        auth: AuthConfig,
    ) -> Self {
        Self {
            cluster,
            log,
            trace,
            auth,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_duration_utils::{
        default_candidate_timeout, default_client_timeout, default_client_wait_synced_timeout,
        default_heartbeat_interval, default_retry_timeout, default_rpc_timeout,
        default_server_wait_synced_timeout,
    };

    fn default_timeout_server() -> ServerTimeout {
        ServerTimeout::new(
            default_heartbeat_interval(),
            default_server_wait_synced_timeout(),
            default_retry_timeout(),
            default_rpc_timeout(),
            default_candidate_timeout(),
        )
    }

    fn default_timeout_client() -> ClientTimeout {
        ClientTimeout::new(
            default_client_timeout(),
            default_client_wait_synced_timeout(),
        )
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_cluster_duration_convert_should_success() {
        assert_eq!(
            ClusterDuration::from_str("5s").unwrap(),
            ClusterDuration(Duration::from_secs(5))
        );
        assert_eq!(
            ClusterDuration::from_str("5us").unwrap(),
            ClusterDuration(Duration::from_micros(5))
        );
        assert_eq!(
            ClusterDuration::from_str("5ms").unwrap(),
            ClusterDuration(Duration::from_millis(5))
        );
        assert!(ClusterDuration::from_str("5hello").is_err());
        assert!(ClusterDuration::from_str("hellos").is_err());
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_xline_server_config_shoule_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            r#"[cluster]
            name = 'node1'
            is_leader = true
            
            [cluster.members]
            node1 = '127.0.0.1:2379'
            node2 = '127.0.0.1:2380'
            node3 = '127.0.0.1:2381'
            
            [cluster.server_timeout]
            heartbeat_interval = '200ms'
            wait_synced_timeout = '100ms'
            rpc_timeout = '100ms'
            retry_timeout = '100us'
            candidate_timeout = '5s'

            [cluster.client_timeout]
            timeout = '5s'
            wait_synced_timeout = '100s'
            
            
            [log]
            path = '/tmp/xline'
            rotation = 'Daily'
            level = 'Info'
            
            [trace]
            jaeger_online = false
            jaeger_offline = false
            jaeger_output_dir = './jaeger_jsons'
            jaeger_level = 'Info'
            
            [auth]"#,
        )
        .unwrap();

        let server_timeout = ServerTimeout::new(
            ClusterDuration(Duration::from_millis(200)),
            ClusterDuration(Duration::from_millis(100)),
            ClusterDuration(Duration::from_micros(100)),
            ClusterDuration(Duration::from_millis(100)),
            ClusterDuration(Duration::from_secs(5)),
        );

        let client_timeout = ClientTimeout::new(
            ClusterDuration(Duration::from_secs(5)),
            ClusterDuration(Duration::from_secs(100)),
        );

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                HashMap::from([
                    ("node1".to_owned(), "127.0.0.1:2379".to_owned()),
                    ("node2".to_owned(), "127.0.0.1:2380".to_owned()),
                    ("node3".to_owned(), "127.0.0.1:2381".to_owned()),
                ]),
                true,
                server_timeout,
                client_timeout
            )
        );

        assert_eq!(
            config.log,
            LogConfig::new(
                PathBuf::from("/tmp/xline"),
                RotationConfig::Daily,
                LevelConfig::Info
            )
        );
        assert_eq!(
            config.trace,
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::Info
            )
        );
    }

    #[test]
    fn test_xline_server_default_config_should_be_loaded() {
        let config: XlineServerConfig =
            toml::from_str(include_str!("../config/xline_server.conf")).unwrap();

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                HashMap::from([
                    ("node1".to_owned(), "127.0.0.1:2379".to_owned()),
                    ("node2".to_owned(), "127.0.0.1:2380".to_owned()),
                    ("node3".to_owned(), "127.0.0.1:2381".to_owned()),
                ]),
                true,
                default_timeout_server(),
                default_timeout_client()
            )
        );
        assert_eq!(
            config.log,
            LogConfig::new(
                PathBuf::from("/tmp/xline"),
                RotationConfig::Daily,
                LevelConfig::Info
            )
        );
        assert_eq!(
            config.trace,
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::Info
            )
        );
    }
}
