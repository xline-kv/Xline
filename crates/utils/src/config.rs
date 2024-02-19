use std::{collections::HashMap, path::PathBuf, time::Duration};

use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;
use tracing_appender::rolling::RollingFileAppender;

/// Xline server configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
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
    /// compactor configuration object
    #[getset(get = "pub")]
    compact: CompactConfig,
    /// tls configuration object
    #[getset(get = "pub")]
    tls: TlsConfig,
    /// Metrics config
    #[getset(get = "pub")]
    #[serde(default = "MetricsConfig::default")]
    metrics: MetricsConfig,
}

/// Cluster Range type alias
pub type ClusterRange = std::ops::Range<u64>;
/// Log verbosity level alias
#[allow(clippy::module_name_repetitions)]
pub type LevelConfig = tracing::metadata::LevelFilter;

/// `Duration` deserialization formatter
pub mod duration_format {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer};

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
    use serde::{Deserialize, Deserializer};

    use crate::parse_batch_bytes;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)] //  the false positive case blocks us
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
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
        peers: HashMap<String, Vec<String>>,
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
            peers,
            is_leader,
            curp_config: curp,
            client_config,
            server_timeout,
            initial_cluster_state,
        }
    }
}

/// Compaction configuration
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Getters)]
#[allow(clippy::module_name_repetitions)]
pub struct CompactConfig {
    /// The max number of historical versions processed in a single compact operation
    #[getset(get = "pub")]
    #[serde(default = "default_compact_batch_size")]
    compact_batch_size: usize,
    /// The interval between two compaction batches
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_compact_sleep_interval")]
    compact_sleep_interval: Duration,
    /// The auto compactor config
    #[getset(get = "pub")]
    auto_compact_config: Option<AutoCompactConfig>,
}

impl Default for CompactConfig {
    #[inline]
    fn default() -> Self {
        Self {
            compact_batch_size: default_compact_batch_size(),
            compact_sleep_interval: default_compact_sleep_interval(),
            auto_compact_config: None,
        }
    }
}

impl CompactConfig {
    /// Create a new compact config
    #[must_use]
    #[inline]
    pub fn new(
        compact_batch_size: usize,
        compact_sleep_interval: Duration,
        auto_compact_config: Option<AutoCompactConfig>,
    ) -> Self {
        Self {
            compact_batch_size,
            compact_sleep_interval,
            auto_compact_config,
        }
    }
}

/// default compact batch size
#[must_use]
#[inline]
pub const fn default_compact_batch_size() -> usize {
    1000
}

/// default compact interval
#[must_use]
#[inline]
pub const fn default_compact_sleep_interval() -> Duration {
    Duration::from_millis(10)
}

/// Curp server timeout settings
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Builder)]
#[allow(clippy::module_name_repetitions, clippy::exhaustive_structs)]
pub struct CurpConfig {
    /// Heartbeat Interval
    #[builder(default = "default_heartbeat_interval()")]
    #[serde(with = "duration_format", default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,

    /// Curp wait sync timeout
    #[builder(default = "default_server_wait_synced_timeout()")]
    #[serde(
        with = "duration_format",
        default = "default_server_wait_synced_timeout"
    )]
    pub wait_synced_timeout: Duration,

    /// Curp propose retry count
    #[builder(default = "default_retry_count()")]
    #[serde(default = "default_retry_count")]
    pub retry_count: usize,

    /// Curp rpc timeout
    #[builder(default = "default_rpc_timeout()")]
    #[serde(with = "duration_format", default = "default_rpc_timeout")]
    pub rpc_timeout: Duration,

    /// Curp append entries batch timeout
    /// If the `batch_timeout` has expired, then it will be dispatched
    /// whether its size reaches the `BATCHING_MSG_MAX_SIZE` or not.
    #[builder(default = "default_batch_timeout()")]
    #[serde(with = "duration_format", default = "default_batch_timeout")]
    pub batch_timeout: Duration,

    /// The maximum number of bytes per batch.
    #[builder(default = "default_batch_max_size()")]
    #[serde(with = "bytes_format", default = "default_batch_max_size")]
    pub batch_max_size: u64,

    /// How many ticks a follower is allowed to miss before it starts a new round of election
    /// The actual timeout will be randomized and in between heartbeat_interval * [follower_timeout_ticks, 2 * follower_timeout_ticks)
    #[builder(default = "default_follower_timeout_ticks()")]
    #[serde(default = "default_follower_timeout_ticks")]
    pub follower_timeout_ticks: u8,

    /// How many ticks a candidate needs to wait before it starts a new round of election
    /// It should be smaller than `follower_timeout_ticks`
    /// The actual timeout will be randomized and in between heartbeat_interval * [candidate_timeout_ticks, 2 * candidate_timeout_ticks)
    #[builder(default = "default_candidate_timeout_ticks()")]
    #[serde(default = "default_candidate_timeout_ticks")]
    pub candidate_timeout_ticks: u8,

    /// Curp storage path
    #[builder(default = "EngineConfig::default()")]
    #[serde(default = "EngineConfig::default")]
    pub engine_cfg: EngineConfig,

    /// Number of command execute workers
    #[builder(default = "default_cmd_workers()")]
    #[serde(default = "default_cmd_workers")]
    pub cmd_workers: u8,

    /// How often should the gc task run
    #[builder(default = "default_gc_interval()")]
    #[serde(with = "duration_format", default = "default_gc_interval")]
    pub gc_interval: Duration,

    /// Number of log entries to keep in memory
    #[builder(default = "default_log_entries_cap()")]
    #[serde(default = "default_log_entries_cap")]
    pub log_entries_cap: usize,
}

/// default heartbeat interval
#[must_use]
#[inline]
pub const fn default_heartbeat_interval() -> Duration {
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
#[allow(clippy::arithmetic_side_effects)]
pub const fn default_batch_max_size() -> u64 {
    2 * 1024 * 1024
}

/// default wait synced timeout
#[must_use]
#[inline]
pub const fn default_server_wait_synced_timeout() -> Duration {
    Duration::from_secs(5)
}

/// default initial retry timeout
#[must_use]
#[inline]
pub const fn default_initial_retry_timeout() -> Duration {
    Duration::from_millis(1500)
}

/// default max retry timeout
#[must_use]
#[inline]
pub const fn default_max_retry_timeout() -> Duration {
    Duration::from_millis(10_000)
}

/// default retry count
#[cfg(not(madsim))]
#[must_use]
#[inline]
pub const fn default_retry_count() -> usize {
    3
}
/// default retry count
#[cfg(madsim)]
#[must_use]
#[inline]
pub const fn default_retry_count() -> usize {
    10
}

/// default use backoff
#[must_use]
#[inline]
pub const fn default_fixed_backoff() -> bool {
    false
}

/// default rpc timeout
#[must_use]
#[inline]
pub const fn default_rpc_timeout() -> Duration {
    Duration::from_millis(50)
}

/// default candidate timeout ticks
#[must_use]
#[inline]
pub const fn default_candidate_timeout_ticks() -> u8 {
    2
}

/// default client wait synced timeout
#[must_use]
#[inline]
pub const fn default_client_wait_synced_timeout() -> Duration {
    Duration::from_secs(2)
}

/// default client propose timeout
#[must_use]
#[inline]
pub const fn default_propose_timeout() -> Duration {
    Duration::from_secs(1)
}

/// default client id keep alive interval
#[must_use]
#[inline]
pub const fn default_client_id_keep_alive_interval() -> Duration {
    Duration::from_secs(1)
}

/// default follower timeout
#[must_use]
#[inline]
pub const fn default_follower_timeout_ticks() -> u8 {
    5
}

/// default number of execute workers
#[must_use]
#[inline]
pub const fn default_cmd_workers() -> u8 {
    8
}

/// default range retry timeout
#[must_use]
#[inline]
pub const fn default_range_retry_timeout() -> Duration {
    Duration::from_secs(2)
}

/// default compact timeout
#[must_use]
#[inline]
pub const fn default_compact_timeout() -> Duration {
    Duration::from_secs(5)
}

/// default sync victims interval
#[must_use]
#[inline]
pub const fn default_sync_victims_interval() -> Duration {
    Duration::from_millis(10)
}

/// default gc interval
#[must_use]
#[inline]
pub const fn default_gc_interval() -> Duration {
    Duration::from_secs(20)
}

/// default number of log entries to keep in memory
#[must_use]
#[inline]
pub const fn default_log_entries_cap() -> usize {
    5000
}

/// default watch progress notify interval
#[must_use]
#[inline]
pub const fn default_watch_progress_notify_interval() -> Duration {
    Duration::from_secs(600)
}

impl Default for CurpConfig {
    #[inline]
    fn default() -> Self {
        Self {
            heartbeat_interval: default_heartbeat_interval(),
            wait_synced_timeout: default_server_wait_synced_timeout(),
            retry_count: default_retry_count(),
            rpc_timeout: default_rpc_timeout(),
            batch_timeout: default_batch_timeout(),
            batch_max_size: default_batch_max_size(),
            follower_timeout_ticks: default_follower_timeout_ticks(),
            candidate_timeout_ticks: default_candidate_timeout_ticks(),
            engine_cfg: EngineConfig::default(),
            cmd_workers: default_cmd_workers(),
            gc_interval: default_gc_interval(),
            log_entries_cap: default_log_entries_cap(),
        }
    }
}

/// Curp client settings
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
#[allow(clippy::module_name_repetitions)]
pub struct ClientConfig {
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

    /// Curp client initial retry interval
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_initial_retry_timeout")]
    initial_retry_timeout: Duration,

    /// Curp client max retry interval
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_max_retry_timeout")]
    max_retry_timeout: Duration,

    /// Curp client retry interval
    #[getset(get = "pub")]
    #[serde(default = "default_retry_count")]
    retry_count: usize,

    /// Whether to use exponential backoff in retries
    #[getset(get = "pub")]
    #[serde(default = "default_fixed_backoff")]
    fixed_backoff: bool,

    /// Curp client keep client id alive interval
    #[getset(get = "pub")]
    #[serde(
        with = "duration_format",
        default = "default_client_id_keep_alive_interval"
    )]
    keep_alive_interval: Duration,
}

impl ClientConfig {
    /// Create a new client timeout
    ///
    /// # Panics
    ///
    /// Panics if `initial_retry_timeout` is larger than `max_retry_timeout`
    #[must_use]
    #[inline]
    pub fn new(
        wait_synced_timeout: Duration,
        propose_timeout: Duration,
        initial_retry_timeout: Duration,
        max_retry_timeout: Duration,
        retry_count: usize,
        fixed_backoff: bool,
        keep_alive_interval: Duration,
    ) -> Self {
        assert!(
            initial_retry_timeout <= max_retry_timeout,
            "`initial_retry_timeout` should less or equal to `max_retry_timeout`"
        );
        Self {
            wait_synced_timeout,
            propose_timeout,
            initial_retry_timeout,
            max_retry_timeout,
            retry_count,
            fixed_backoff,
            keep_alive_interval,
        }
    }
}

impl Default for ClientConfig {
    #[inline]
    fn default() -> Self {
        Self {
            wait_synced_timeout: default_client_wait_synced_timeout(),
            propose_timeout: default_propose_timeout(),
            initial_retry_timeout: default_initial_retry_timeout(),
            max_retry_timeout: default_max_retry_timeout(),
            retry_count: default_retry_count(),
            fixed_backoff: default_fixed_backoff(),
            keep_alive_interval: default_client_id_keep_alive_interval(),
        }
    }
}

/// Xline server settings
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct ServerTimeout {
    /// Range request retry timeout settings
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_range_retry_timeout")]
    range_retry_timeout: Duration,
    /// Range request retry timeout settings
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_compact_timeout")]
    compact_timeout: Duration,
    /// Sync victims interval
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_sync_victims_interval")]
    sync_victims_interval: Duration,
    /// Watch progress notify interval settings
    #[getset(get = "pub")]
    #[serde(
        with = "duration_format",
        default = "default_watch_progress_notify_interval"
    )]
    watch_progress_notify_interval: Duration,
}

impl ServerTimeout {
    /// Create a new server timeout
    #[must_use]
    #[inline]
    pub fn new(
        range_retry_timeout: Duration,
        compact_timeout: Duration,
        sync_victims_interval: Duration,
        watch_progress_notify_interval: Duration,
    ) -> Self {
        Self {
            range_retry_timeout,
            compact_timeout,
            sync_victims_interval,
            watch_progress_notify_interval,
        }
    }
}

impl Default for ServerTimeout {
    #[inline]
    fn default() -> Self {
        Self {
            range_retry_timeout: default_range_retry_timeout(),
            compact_timeout: default_compact_timeout(),
            sync_victims_interval: default_sync_victims_interval(),
            watch_progress_notify_interval: default_watch_progress_notify_interval(),
        }
    }
}

/// Auto Compactor Configuration
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "mode",
    content = "retention",
    rename_all(deserialize = "lowercase")
)]
pub enum AutoCompactConfig {
    /// auto periodic compactor
    #[serde(with = "duration_format")]
    Periodic(Duration),
    /// auto revision compactor
    Revision(i64),
}

/// Engine Configuration
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "type",
    content = "data_dir",
    rename_all(deserialize = "lowercase")
)]
pub enum EngineConfig {
    /// Memory Storage Engine
    Memory,
    /// RocksDB Storage Engine
    RocksDB(PathBuf),
}

impl Default for EngineConfig {
    #[inline]
    fn default() -> Self {
        Self::Memory
    }
}

/// /// Storage Configuration
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
pub struct StorageConfig {
    /// Engine Configuration
    #[serde(default = "EngineConfig::default")]
    pub engine: EngineConfig,
    /// Quota
    #[serde(default = "default_quota")]
    pub quota: u64,
}

impl StorageConfig {
    /// Create a new storage config
    #[inline]
    #[must_use]
    pub fn new(engine: EngineConfig, quota: u64) -> Self {
        Self { engine, quota }
    }
}

impl Default for StorageConfig {
    #[inline]
    fn default() -> Self {
        Self {
            engine: EngineConfig::default(),
            quota: default_quota(),
        }
    }
}

/// Default quota: 8GB
#[inline]
#[must_use]
pub fn default_quota() -> u64 {
    // 8 * 1024 * 1024 * 1024
    0x0002_0000_0000
}

/// Log configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct LogConfig {
    /// Log file path
    #[getset(get = "pub")]
    #[serde(default)]
    path: Option<PathBuf>,
    /// Log rotation strategy
    #[getset(get = "pub")]
    #[serde(with = "rotation_format", default = "default_rotation")]
    rotation: RotationConfig,
    /// Log verbosity level
    #[getset(get = "pub")]
    #[serde(with = "level_format", default = "default_log_level")]
    level: LevelConfig,
}

impl Default for LogConfig {
    #[inline]
    fn default() -> Self {
        Self {
            path: None,
            rotation: default_rotation(),
            level: default_log_level(),
        }
    }
}

/// `LevelConfig` deserialization formatter
pub mod level_format {
    use serde::{Deserialize, Deserializer};

    use super::LevelConfig;
    use crate::parse_log_level;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)]
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
pub const fn default_log_level() -> LevelConfig {
    LevelConfig::INFO
}

impl LogConfig {
    /// Generate a new `LogConfig` object
    #[must_use]
    #[inline]
    pub fn new(path: PathBuf, rotation: RotationConfig, level: LevelConfig) -> Self {
        Self {
            path: Some(path),
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
    use serde::{Deserialize, Deserializer};

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
pub const fn default_rotation() -> RotationConfig {
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

impl Default for TraceConfig {
    #[inline]
    fn default() -> Self {
        Self {
            jaeger_online: false,
            jaeger_offline: false,
            jaeger_output_dir: "".into(),
            jaeger_level: default_log_level(),
        }
    }
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
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
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

/// Xline tls configuration object
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
pub struct TlsConfig {
    /// The CA certificate file used by peer to verify client certificates
    #[getset(get = "pub")]
    pub peer_ca_cert_path: Option<PathBuf>,
    /// The public key file used by peer
    #[getset(get = "pub")]
    pub peer_cert_path: Option<PathBuf>,
    /// The private key file used by peer
    #[getset(get = "pub")]
    pub peer_key_path: Option<PathBuf>,
    /// The CA certificate file used by client to verify peer certificates
    #[getset(get = "pub")]
    pub client_ca_cert_path: Option<PathBuf>,
    /// The public key file used by client
    #[getset(get = "pub")]
    pub client_cert_path: Option<PathBuf>,
    /// The private key file used by client
    #[getset(get = "pub")]
    pub client_key_path: Option<PathBuf>,
}

impl TlsConfig {
    /// Create a new `TlsConfig` object
    #[must_use]
    #[inline]
    pub fn new(
        peer_ca_cert_path: Option<PathBuf>,
        peer_cert_path: Option<PathBuf>,
        peer_key_path: Option<PathBuf>,
        client_ca_cert_path: Option<PathBuf>,
        client_cert_path: Option<PathBuf>,
        client_key_path: Option<PathBuf>,
    ) -> Self {
        Self {
            peer_ca_cert_path,
            peer_cert_path,
            peer_key_path,
            client_ca_cert_path,
            client_cert_path,
            client_key_path,
        }
    }

    /// Whether the server tls is enabled
    #[must_use]
    #[inline]
    pub fn server_tls_enabled(&self) -> bool {
        self.peer_cert_path.is_some() && self.peer_key_path.is_some()
    }
}

/// Xline metrics push protocol
#[non_exhaustive]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum MetricsPushProtocol {
    /// HTTP protocol
    HTTP,
    /// GRPC protocol
    #[default]
    GRPC,
}

impl std::fmt::Display for MetricsPushProtocol {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            MetricsPushProtocol::HTTP => write!(f, "http"),
            MetricsPushProtocol::GRPC => write!(f, "grpc"),
        }
    }
}

/// Metrics push protocol format
pub mod protocol_format {
    use serde::{Deserialize, Deserializer};

    use super::MetricsPushProtocol;
    use crate::parse_metrics_push_protocol;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<MetricsPushProtocol, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_metrics_push_protocol(&s).map_err(serde::de::Error::custom)
    }
}

/// Xline metrics configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct MetricsConfig {
    /// Enable or not
    #[getset(get = "pub")]
    #[serde(default = "default_metrics_enable")]
    enable: bool,
    /// The http port to expose
    #[getset(get = "pub")]
    #[serde(default = "default_metrics_port")]
    port: u16,
    /// The http path to expose
    #[getset(get = "pub")]
    #[serde(default = "default_metrics_path")]
    path: String,
    /// Enable push or not
    #[getset(get = "pub")]
    #[serde(default = "default_metrics_push")]
    push: bool,
    /// Push endpoint
    #[getset(get = "pub")]
    #[serde(default = "default_metrics_push_endpoint")]
    push_endpoint: String,
    /// Push protocol
    #[getset(get = "pub")]
    #[serde(with = "protocol_format", default = "default_metrics_push_protocol")]
    push_protocol: MetricsPushProtocol,
}

impl MetricsConfig {
    /// Create a new `MetricsConfig`
    #[must_use]
    #[inline]
    pub fn new(
        enable: bool,
        port: u16,
        path: String,
        push: bool,
        push_endpoint: String,
        push_protocol: MetricsPushProtocol,
    ) -> Self {
        Self {
            enable,
            port,
            path,
            push,
            push_endpoint,
            push_protocol,
        }
    }
}

impl Default for MetricsConfig {
    #[inline]
    fn default() -> Self {
        Self {
            enable: default_metrics_enable(),
            port: default_metrics_port(),
            path: default_metrics_path(),
            push: default_metrics_push(),
            push_endpoint: default_metrics_push_endpoint(),
            push_protocol: default_metrics_push_protocol(),
        }
    }
}

/// Default metrics enable
#[must_use]
#[inline]
pub const fn default_metrics_enable() -> bool {
    true
}

/// Default metrics port
#[must_use]
#[inline]
pub const fn default_metrics_port() -> u16 {
    9100
}

/// Default metrics path
#[must_use]
#[inline]
pub fn default_metrics_path() -> String {
    "/metrics".to_owned()
}

/// Default metrics push option
#[must_use]
#[inline]
pub fn default_metrics_push() -> bool {
    false
}

/// Default metrics push protocol
#[must_use]
#[inline]
pub fn default_metrics_push_protocol() -> MetricsPushProtocol {
    MetricsPushProtocol::GRPC
}

/// Default metrics push endpoint
#[must_use]
#[inline]
pub fn default_metrics_push_endpoint() -> String {
    "http://127.0.0.1:4318".to_owned()
}

impl XlineServerConfig {
    /// Generates a new `XlineServerConfig` object
    #[must_use]
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster: ClusterConfig,
        storage: StorageConfig,
        log: LogConfig,
        trace: TraceConfig,
        auth: AuthConfig,
        compact: CompactConfig,
        tls: TlsConfig,
        metrics: MetricsConfig,
    ) -> Self {
        Self {
            cluster,
            storage,
            log,
            trace,
            auth,
            compact,
            tls,
            metrics,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::too_many_lines)] // just a testcase, not too bad
    #[test]
    fn test_xline_server_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            r#"[cluster]
            name = 'node1'
            is_leader = true
            initial_cluster_state = 'new'
            peer_listen_urls = ['127.0.0.1:2380']
            peer_advertise_urls = ['127.0.0.1:2380']
            client_listen_urls = ['127.0.0.1:2379']
            client_advertise_urls = ['127.0.0.1:2379']

            [cluster.server_timeout]
            range_retry_timeout = '3s'
            compact_timeout = '5s'
            sync_victims_interval = '20ms'
            watch_progress_notify_interval = '1s'

            [cluster.peers]
            node1 = ['127.0.0.1:2378', '127.0.0.1:2379']
            node2 = ['127.0.0.1:2380']
            node3 = ['127.0.0.1:2381']

            [cluster.curp_config]
            heartbeat_interval = '200ms'
            wait_synced_timeout = '100ms'
            rpc_timeout = '100ms'
            retry_timeout = '100ms'

            [cluster.client_config]
            initial_retry_timeout = '5s'
            max_retry_timeout = '50s'

            [storage]
            engine = { type = 'memory'}

            [compact]
            compact_batch_size = 123
            compact_sleep_interval = '5ms'

            [compact.auto_compact_config]
            mode = 'periodic'
            retention = '10h'

            [log]
            path = '/var/log/xline'
            rotation = 'daily'
            level = 'info'

            [trace]
            jaeger_online = false
            jaeger_offline = false
            jaeger_output_dir = './jaeger_jsons'
            jaeger_level = 'info'

            [auth]
            auth_public_key = './public_key.pem'
            auth_private_key = './private_key.pem'

            [tls]
            peer_cert_path = './cert.pem'
            peer_key_path = './key.pem'
            client_ca_cert_path = './ca.pem'

            [metrics]
            enable = true
            port = 9100
            path = "/metrics"
            push = true
            push_endpoint = 'http://some-endpoint.com:4396'
            push_protocol = 'http'
            "#,
        )
        .unwrap();

        let curp_config = CurpConfigBuilder::default()
            .heartbeat_interval(Duration::from_millis(200))
            .wait_synced_timeout(Duration::from_millis(100))
            .rpc_timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        let client_config = ClientConfig::new(
            default_client_wait_synced_timeout(),
            default_propose_timeout(),
            Duration::from_secs(5),
            Duration::from_secs(50),
            default_retry_count(),
            default_fixed_backoff(),
            default_client_id_keep_alive_interval(),
        );

        let server_timeout = ServerTimeout::new(
            Duration::from_secs(3),
            Duration::from_secs(5),
            Duration::from_millis(20),
            Duration::from_secs(1),
        );

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                HashMap::from_iter([
                    (
                        "node1".to_owned(),
                        vec!["127.0.0.1:2378".to_owned(), "127.0.0.1:2379".to_owned()]
                    ),
                    ("node2".to_owned(), vec!["127.0.0.1:2380".to_owned()]),
                    ("node3".to_owned(), vec!["127.0.0.1:2381".to_owned()]),
                ]),
                true,
                curp_config,
                client_config,
                server_timeout,
                InitialClusterState::New
            )
        );

        assert_eq!(
            config.storage,
            StorageConfig::new(EngineConfig::Memory, default_quota())
        );

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

        assert_eq!(
            config.compact,
            CompactConfig {
                compact_batch_size: 123,
                compact_sleep_interval: Duration::from_millis(5),
                auto_compact_config: Some(AutoCompactConfig::Periodic(Duration::from_secs(
                    10 * 60 * 60
                )))
            }
        );

        assert_eq!(
            config.auth,
            AuthConfig {
                auth_private_key: Some(PathBuf::from("./private_key.pem")),
                auth_public_key: Some(PathBuf::from("./public_key.pem")),
            }
        );

        assert_eq!(
            config.tls,
            TlsConfig {
                peer_cert_path: Some(PathBuf::from("./cert.pem")),
                peer_key_path: Some(PathBuf::from("./key.pem")),
                client_ca_cert_path: Some(PathBuf::from("./ca.pem")),
                ..Default::default()
            }
        );

        assert_eq!(
            config.metrics,
            MetricsConfig {
                enable: true,
                port: 9100,
                path: "/metrics".to_owned(),
                push: true,
                push_endpoint: "http://some-endpoint.com:4396".to_owned(),
                push_protocol: MetricsPushProtocol::HTTP,
            },
        );
    }

    #[test]
    fn test_xline_server_default_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            r#"[cluster]
                name = 'node1'
                is_leader = true
                peer_listen_urls = ['127.0.0.1:2380']
                peer_advertise_urls = ['127.0.0.1:2380']
                client_listen_urls = ['127.0.0.1:2379']
                client_advertise_urls = ['127.0.0.1:2379']

                [cluster.peers]
                node1 = ['127.0.0.1:2379']
                node2 = ['127.0.0.1:2380']
                node3 = ['127.0.0.1:2381']

                [cluster.storage]

                [log]
                path = '/var/log/xline'

                [storage]
                engine = { type = 'rocksdb', data_dir = '/usr/local/xline/data-dir' }

                [compact]

                [trace]
                jaeger_online = false
                jaeger_offline = false
                jaeger_output_dir = './jaeger_jsons'
                jaeger_level = 'info'

                [auth]

                [tls]
                "#,
        )
        .unwrap();

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2380".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                vec!["127.0.0.1:2379".to_owned()],
                HashMap::from([
                    ("node1".to_owned(), vec!["127.0.0.1:2379".to_owned()]),
                    ("node2".to_owned(), vec!["127.0.0.1:2380".to_owned()]),
                    ("node3".to_owned(), vec!["127.0.0.1:2381".to_owned()]),
                ]),
                true,
                CurpConfigBuilder::default().build().unwrap(),
                ClientConfig::default(),
                ServerTimeout::default(),
                InitialClusterState::default()
            )
        );

        if let EngineConfig::RocksDB(path) = config.storage.engine {
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
        assert_eq!(config.compact, CompactConfig::default());
        assert_eq!(config.auth, AuthConfig::default());
        assert_eq!(config.tls, TlsConfig::default());
        assert_eq!(config.metrics, MetricsConfig::default());
    }

    #[test]
    fn test_auto_revision_compactor_config_should_be_loaded() {
        let config: XlineServerConfig = toml::from_str(
            r#"[cluster]
                name = 'node1'
                is_leader = true
                peer_listen_urls = ['127.0.0.1:2380']
                peer_advertise_urls = ['127.0.0.1:2380']
                client_listen_urls = ['127.0.0.1:2379']
                client_advertise_urls = ['127.0.0.1:2379']

                [cluster.peers]
                node1 = ['127.0.0.1:2379']
                node2 = ['127.0.0.1:2380']
                node3 = ['127.0.0.1:2381']

                [cluster.storage]

                [log]
                path = '/var/log/xline'

                [storage]
                engine = { type = 'memory' }

                [compact]

                [compact.auto_compact_config]
                mode = 'revision'
                retention = 10000

                [trace]
                jaeger_online = false
                jaeger_offline = false
                jaeger_output_dir = './jaeger_jsons'
                jaeger_level = 'info'

                [auth]

                [tls]
                "#,
        )
        .unwrap();

        assert_eq!(
            config.compact,
            CompactConfig {
                auto_compact_config: Some(AutoCompactConfig::Revision(10000)),
                ..Default::default()
            }
        );
    }
}
