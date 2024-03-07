use std::time::Duration;

use getset::Getters;
use serde::Deserialize;

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
        }
    }
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
