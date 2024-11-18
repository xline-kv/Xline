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
