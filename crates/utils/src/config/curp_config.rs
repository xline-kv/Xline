use std::time::Duration;

use derive_builder::Builder;
use getset::Getters;
use serde::Deserialize;

use super::engine_config::EngineConfig;

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

/// default heartbeat interval
#[must_use]
#[inline]
pub const fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(300)
}

/// default wait synced timeout
#[must_use]
#[inline]
pub const fn default_server_wait_synced_timeout() -> Duration {
    Duration::from_secs(5)
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

/// default rpc timeout
#[must_use]
#[inline]
pub const fn default_rpc_timeout() -> Duration {
    Duration::from_millis(150)
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

/// default follower timeout
#[must_use]
#[inline]
pub const fn default_follower_timeout_ticks() -> u8 {
    5
}

/// default candidate timeout ticks
#[must_use]
#[inline]
pub const fn default_candidate_timeout_ticks() -> u8 {
    2
}

/// default number of execute workers
#[must_use]
#[inline]
pub const fn default_cmd_workers() -> u8 {
    8
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
