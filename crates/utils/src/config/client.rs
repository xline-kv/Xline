use std::time::Duration;

use getset::Getters;
use serde::Deserialize;

use super::duration_format;
use super::prelude::{
    default_client_id_keep_alive_interval, default_client_wait_synced_timeout,
    default_fixed_backoff, default_initial_retry_timeout, default_max_retry_timeout,
    default_propose_timeout, default_retry_count,
};

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
    /// Create a builder for `ClientConfig`
    #[inline]
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
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

/// Builder for `ClientConfig`
#[derive(Default, Debug, Clone, Copy)]
pub struct Builder {
    /// Curp client wait sync timeout
    wait_synced_timeout: Option<Duration>,
    /// Curp client propose request timeout
    propose_timeout: Option<Duration>,
    /// Curp client initial retry interval
    initial_retry_timeout: Option<Duration>,
    /// Curp client max retry interval
    max_retry_timeout: Option<Duration>,
    /// Curp client retry interval
    retry_count: Option<usize>,
    /// Whether to use exponential backoff in retries
    fixed_backoff: Option<bool>,
    /// Curp client keep client id alive interval
    keep_alive_interval: Option<Duration>,
}

impl Builder {
    /// Set the wait sync timeout
    #[inline]
    #[must_use]
    pub fn wait_synced_timeout(mut self, timeout: Duration) -> Self {
        self.wait_synced_timeout = Some(timeout);
        self
    }

    /// Set the propose timeout
    #[inline]
    #[must_use]
    pub fn propose_timeout(mut self, timeout: Duration) -> Self {
        self.propose_timeout = Some(timeout);
        self
    }

    /// Set the initial retry timeout
    #[inline]
    #[must_use]
    pub fn initial_retry_timeout(mut self, timeout: Duration) -> Self {
        self.initial_retry_timeout = Some(timeout);
        self
    }

    /// Set the max retry timeout
    #[inline]
    #[must_use]
    pub fn max_retry_timeout(mut self, timeout: Duration) -> Self {
        self.max_retry_timeout = Some(timeout);
        self
    }

    /// Set the retry count
    #[inline]
    #[must_use]
    pub fn retry_count(mut self, count: usize) -> Self {
        self.retry_count = Some(count);
        self
    }

    /// Set whether to use fixed backoff
    #[inline]
    #[must_use]
    pub fn fixed_backoff(mut self, use_backoff: bool) -> Self {
        self.fixed_backoff = Some(use_backoff);
        self
    }

    /// Set the keep alive interval
    #[inline]
    #[must_use]
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = Some(interval);
        self
    }

    /// # Panics
    ///
    /// Panics if `initial_retry_timeout` is larger than `max_retry_timeout`
    /// Build the `ClientConfig` and validate it
    #[inline]
    #[must_use]
    pub fn build(self) -> ClientConfig {
        let initial_retry_timeout = self
            .initial_retry_timeout
            .unwrap_or_else(default_initial_retry_timeout);
        let max_retry_timeout = self
            .max_retry_timeout
            .unwrap_or_else(default_max_retry_timeout);

        // Assert that `initial_retry_timeout <= max_retry_timeout`
        assert!(
            initial_retry_timeout <= max_retry_timeout,
            "`initial_retry_timeout` should be less than or equal to `max_retry_timeout`"
        );

        ClientConfig {
            wait_synced_timeout: self
                .wait_synced_timeout
                .unwrap_or_else(default_client_wait_synced_timeout),
            propose_timeout: self.propose_timeout.unwrap_or_else(default_propose_timeout),
            initial_retry_timeout,
            max_retry_timeout,
            retry_count: self.retry_count.unwrap_or_else(default_retry_count),
            fixed_backoff: self.fixed_backoff.unwrap_or_else(default_fixed_backoff),
            keep_alive_interval: self
                .keep_alive_interval
                .unwrap_or_else(default_client_id_keep_alive_interval),
        }
    }
}
