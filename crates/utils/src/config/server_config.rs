use std::time::Duration;

use getset::Getters;
use serde::Deserialize;

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

/// default watch progress notify interval
#[must_use]
#[inline]
pub const fn default_watch_progress_notify_interval() -> Duration {
    Duration::from_secs(600)
}

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
