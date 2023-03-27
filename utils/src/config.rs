use std::{collections::HashMap, path::PathBuf, time::Duration};

use getset::Getters;
use serde::Deserialize;
use tracing_appender::rolling::RollingFileAppender;

/// Xline server configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct XlineServerConfig {
    /// cluster configuration object
    #[getset(get = "pub")]
    cluster: ClusterConfig,
    /// xline storage configuration object
    #[getset(get = "pub")]
    storage: StorageConfig,
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

// TODO: support persistent storage configuration in the future

/// Cluster Range type alias
pub type ClusterRange = std::ops::Range<u64>;
/// Log verbosity level alias
#[allow(clippy::module_name_repetitions)]
pub type LevelConfig = tracing::metadata::LevelFilter;

/// `Duration` deserialization formatter
pub mod duration_format {
    use std::time::Duration;

    use serde::{self, Deserialize, Deserializer};

    use crate::parse_duration;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)] //  the false positive case blocks us
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

/// batch size deserialization formatter
pub mod bytes_format {
    use serde::{self, Deserialize, Deserializer};

    use crate::parse_batch_bytes;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)] //  the false positive case blocks us
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<usize, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_batch_bytes(&s).map_err(serde::de::Error::custom)
    }
}

/// Cluster configuration object, including cluster relevant configuration fields
#[allow(clippy::module_name_repetitions)]
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
    #[serde(default = "CurpConfig::default")]
    curp_config: CurpConfig,
    /// Curp client timeout settings
    #[getset(get = "pub")]
    #[serde(default = "ClientTimeout::default")]
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
        curp: CurpConfig,
        client_timeout: ClientTimeout,
    ) -> Self {
        Self {
            name,
            members,
            is_leader,
            curp_config: curp,
            client_timeout,
        }
    }
}

/// Curp server settings
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
#[allow(clippy::module_name_repetitions, clippy::exhaustive_structs)]
pub struct CurpConfig {
    /// Heartbeat Interval
    #[serde(with = "duration_format", default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,

    /// Curp wait sync timeout
    #[serde(
        with = "duration_format",
        default = "default_server_wait_synced_timeout"
    )]
    pub wait_synced_timeout: Duration,

    /// Curp propose retry timeout
    #[serde(with = "duration_format", default = "default_retry_timeout")]
    pub retry_timeout: Duration,

    /// Curp rpc timeout
    #[serde(with = "duration_format", default = "default_rpc_timeout")]
    pub rpc_timeout: Duration,

    /// Curp append entries batch timeout
    /// If the `batch_timeout` has expired, then it will be dispatched
    /// wether its size reaches the `BATCHING_MSG_MAX_SIZE` or not.
    #[serde(with = "duration_format", default = "default_batch_timeout")]
    pub batch_timeout: Duration,

    /// The maximum number of bytes per batch.
    #[serde(with = "bytes_format", default = "default_batch_max_size")]
    pub batch_max_size: usize,

    /// How many ticks a follower is allowed to miss before it starts a new round of election
    /// The actual timeout will be randomized and in between heartbeat_interval * [follower_timeout_ticks, 2 * follower_timeout_ticks)
    #[serde(default = "default_follower_timeout_ticks")]
    pub follower_timeout_ticks: u8,

    /// How many ticks a candidate needs to wait before it starts a new round of election
    /// It should be smaller than `follower_timeout_ticks`
    /// The actual timeout will be randomized and in between heartbeat_interval * [candidate_timeout_ticks, 2 * candidate_timeout_ticks)
    #[serde(default = "default_candidate_timeout_ticks")]
    pub candidate_timeout_ticks: u8,

    /// Curp storage path
    #[serde(default = "default_curp_data_dir")]
    pub data_dir: PathBuf,
}

/// default heartbeat interval
#[must_use]
#[inline]
pub fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(300)
}

/// default batch timeout
#[must_use]
#[inline]
pub const fn default_batch_timeout() -> Duration {
    Duration::from_millis(15)
}

/// default batch timeout
#[must_use]
#[inline]
#[allow(clippy::integer_arithmetic)]
pub const fn default_batch_max_size() -> usize {
    2 * 1024 * 1024
}

/// default wait synced timeout
#[must_use]
#[inline]
pub fn default_server_wait_synced_timeout() -> Duration {
    Duration::from_secs(5)
}

/// default retry timeout
#[must_use]
#[inline]
pub fn default_retry_timeout() -> Duration {
    Duration::from_millis(50)
}

/// default rpc timeout
#[must_use]
#[inline]
pub fn default_rpc_timeout() -> Duration {
    Duration::from_millis(50)
}

/// default candidate timeout ticks
#[must_use]
#[inline]
pub fn default_candidate_timeout_ticks() -> u8 {
    2
}

/// default client wait synced timeout
#[must_use]
#[inline]
pub fn default_client_wait_synced_timeout() -> Duration {
    Duration::from_secs(2)
}

/// default client propose timeout
#[must_use]
#[inline]
pub fn default_propose_timeout() -> Duration {
    Duration::from_secs(1)
}

/// default follower timeout
#[must_use]
#[inline]
pub fn default_follower_timeout_ticks() -> u8 {
    5
}

/// default curp data path
#[must_use]
#[inline]
pub fn default_curp_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/curp")
}

impl CurpConfig {
    /// Create a new server timeout
    #[must_use]
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        heartbeat_interval: Duration,
        wait_synced_timeout: Duration,
        retry_timeout: Duration,
        rpc_timeout: Duration,
        batch_timeout: Duration,
        batch_max_size: usize,
        follower_timeout_ticks: u8,
        candidate_timeout_ticks: u8,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            heartbeat_interval,
            wait_synced_timeout,
            retry_timeout,
            rpc_timeout,
            batch_timeout,
            batch_max_size,
            follower_timeout_ticks,
            candidate_timeout_ticks,
            data_dir,
        }
    }
}

impl Default for CurpConfig {
    #[inline]
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat_interval(),
            wait_synced_timeout: default_server_wait_synced_timeout(),
            retry_timeout: default_retry_timeout(),
            rpc_timeout: default_rpc_timeout(),
            batch_timeout: default_batch_timeout(),
            batch_max_size: default_batch_max_size(),
            follower_timeout_ticks: default_follower_timeout_ticks(),
            candidate_timeout_ticks: default_candidate_timeout_ticks(),
            data_dir: default_curp_data_dir(),
        }
    }
}

/// Curp client timeout settings
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct ClientTimeout {
    /// Curp client wait sync timeout
    #[getset(get = "pub")]
    #[serde(
        with = "duration_format",
        default = "default_client_wait_synced_timeout"
    )]
    wait_synced_timeout: Duration,

    /// Curp client propose request timeout
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_propose_timeout")]
    propose_timeout: Duration,

    /// Curp client retry interval
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_retry_timeout")]
    retry_timeout: Duration,
}

impl ClientTimeout {
    /// Create a new client timeout
    #[must_use]
    #[inline]
    pub fn new(
        wait_synced_timeout: Duration,
        propose_timeout: Duration,
        retry_timeout: Duration,
    ) -> Self {
        Self {
            wait_synced_timeout,
            propose_timeout,
            retry_timeout,
        }
    }
}

impl Default for ClientTimeout {
    #[inline]
    fn default() -> Self {
        Self {
            wait_synced_timeout: default_client_wait_synced_timeout(),
            propose_timeout: default_propose_timeout(),
            retry_timeout: default_retry_timeout(),
        }
    }
}

/// Storage Configuration
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "engine",
    content = "data_dir",
    rename_all(deserialize = "lowercase")
)]
pub enum StorageConfig {
    /// Memory Storage Engine
    Memory,
    /// RocksDB Storage Engine
    RocksDB(PathBuf),
}

/// Log configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct LogConfig {
    /// Log file path
    #[getset(get = "pub")]
    path: PathBuf,
    /// Log rotation strategy
    #[getset(get = "pub")]
    #[serde(with = "rotation_format", default = "default_rotation")]
    rotation: RotationConfig,
    /// Log verbosity level
    #[getset(get = "pub")]
    #[serde(with = "level_format", default = "default_log_level")]
    level: LevelConfig,
}

/// `LevelConfig` deserialization formatter
pub mod level_format {
    use serde::{self, Deserialize, Deserializer};

    use super::LevelConfig;
    use crate::parse_log_level;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)] // TODO: Think is it necessary to allow this clippy??
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<LevelConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_log_level(&s).map_err(serde::de::Error::custom)
    }
}

/// default log level
#[must_use]
#[inline]
pub fn default_log_level() -> LevelConfig {
    LevelConfig::INFO
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
#[allow(clippy::module_name_repetitions)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum RotationConfig {
    /// Rotate log file in every hour
    Hourly,
    /// Rotate log file every day
    Daily,
    /// Never rotate log file
    Never,
}

impl std::fmt::Display for RotationConfig {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            RotationConfig::Hourly => write!(f, "hourly"),
            RotationConfig::Daily => write!(f, "daily"),
            RotationConfig::Never => write!(f, "never"),
        }
    }
}

/// `RotationConfig` deserialization formatter
pub mod rotation_format {
    use serde::{self, Deserialize, Deserializer};

    use super::RotationConfig;
    use crate::parse_rotation;

    /// deserializes a cluster log rotation strategy
    #[allow(single_use_lifetimes)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<RotationConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_rotation(&s).map_err(serde::de::Error::custom)
    }
}

/// default log rotation strategy
#[must_use]
#[inline]
pub fn default_rotation() -> RotationConfig {
    RotationConfig::Daily
}

/// Generates a `RollingFileAppender` from the given `RotationConfig` and `name`
#[must_use]
#[inline]
pub fn file_appender(
    rotation: RotationConfig,
    file_path: &PathBuf,
    name: &str,
) -> RollingFileAppender {
    match rotation {
        RotationConfig::Hourly => {
            tracing_appender::rolling::hourly(file_path, format!("xline_{name}.log"))
        }
        RotationConfig::Daily => {
            tracing_appender::rolling::daily(file_path, format!("xline_{name}.log"))
        }
        RotationConfig::Never => {
            tracing_appender::rolling::never(file_path, format!("xline_{name}.log"))
        }
        #[allow(unreachable_patterns)]
        // It's ok because `parse_rotation` have check the validity before.
        _ => unreachable!("should not call file_appender when parse_rotation failed"),
    }
}

/// Xline tracing configuration object
#[allow(clippy::module_name_repetitions)]
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
    #[serde(with = "level_format", default = "default_log_level")]
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
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct AuthConfig {
    /// The public key file
    #[getset(get = "pub")]
    auth_public_key: Option<PathBuf>,
    /// The private key file
    #[getset(get = "pub")]
    auth_private_key: Option<PathBuf>,
    // TODO: support SSL/TLS configuration in the future
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
        storage: StorageConfig,
        log: LogConfig,
        trace: TraceConfig,
        auth: AuthConfig,
    ) -> Self {
        Self {
            cluster,
            storage,
            log,
            trace,
            auth,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_xline_server_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            r#"[cluster]
            name = 'node1'
            is_leader = true

            [cluster.members]
            node1 = '127.0.0.1:2379'
            node2 = '127.0.0.1:2380'
            node3 = '127.0.0.1:2381'

            [cluster.curp_config]
            heartbeat_interval = '200ms'
            wait_synced_timeout = '100ms'
            rpc_timeout = '100ms'
            retry_timeout = '100us'

            [cluster.client_timeout]
            retry_timeout = '5s'

            [storage]
            engine = 'memory'

            [log]
            path = '/var/log/xline'
            rotation = 'daily'
            level = 'info'

            [trace]
            jaeger_online = false
            jaeger_offline = false
            jaeger_output_dir = './jaeger_jsons'
            jaeger_level = 'info'

            [auth]"#,
        )
        .unwrap();

        let curp_config = CurpConfig::new(
            Duration::from_millis(200),
            Duration::from_millis(100),
            Duration::from_micros(100),
            Duration::from_millis(100),
            default_batch_timeout(),
            default_batch_max_size(),
            default_follower_timeout_ticks(),
            default_candidate_timeout_ticks(),
            default_curp_data_dir(),
        );

        let client_timeout = ClientTimeout::new(
            default_client_wait_synced_timeout(),
            default_propose_timeout(),
            Duration::from_secs(5),
        );

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                HashMap::from_iter([
                    ("node1".to_owned(), "127.0.0.1:2379".to_owned()),
                    ("node2".to_owned(), "127.0.0.1:2380".to_owned()),
                    ("node3".to_owned(), "127.0.0.1:2381".to_owned()),
                ]),
                true,
                curp_config,
                client_timeout
            )
        );

        assert_eq!(config.storage, StorageConfig::Memory);

        assert_eq!(
            config.log,
            LogConfig::new(
                PathBuf::from("/var/log/xline"),
                RotationConfig::Daily,
                LevelConfig::INFO
            )
        );
        assert_eq!(
            config.trace,
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::INFO
            )
        );
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_xline_server_default_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            r#"[cluster]
                name = 'node1'
                is_leader = true

                [cluster.members]
                node1 = '127.0.0.1:2379'
                node2 = '127.0.0.1:2380'
                node3 = '127.0.0.1:2381'

                [log]
                path = '/var/log/xline'

                [storage]
                engine = 'rocksdb'
                data_dir = '/usr/local/xline/data-dir'

                [trace]
                jaeger_online = false
                jaeger_offline = false
                jaeger_output_dir = './jaeger_jsons'
                jaeger_level = 'info'

                [auth]
                # auth_public_key = './public_key'.pem'
                # auth_private_key = './private_key.pem'"#,
        )
        .unwrap();

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
                CurpConfig::default(),
                ClientTimeout::default()
            )
        );

        if let StorageConfig::RocksDB(path) = config.storage {
            assert_eq!(path, PathBuf::from("/usr/local/xline/data-dir"));
        } else {
            unreachable!();
        }

        assert_eq!(
            config.log,
            LogConfig::new(
                PathBuf::from("/var/log/xline"),
                RotationConfig::Daily,
                LevelConfig::INFO
            )
        );
        assert_eq!(
            config.trace,
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::INFO
            )
        );
    }
}
