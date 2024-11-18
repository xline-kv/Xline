use std::time::Duration;

use getset::Getters;
use serde::Deserialize;

use super::prelude::{
    default_compact_timeout, default_range_retry_timeout, default_sync_victims_interval,
    default_watch_progress_notify_interval, AuthConfig, ClusterConfig, CompactConfig, LogConfig,
    MetricsConfig, StorageConfig, TlsConfig, TraceConfig,
};

use super::duration_format;

/// Xline server configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Default, Getters)]
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

/// Xline server settings
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct XlineServerTimeout {
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

impl XlineServerTimeout {
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

impl Default for XlineServerTimeout {
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
