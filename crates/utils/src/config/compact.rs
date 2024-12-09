use getset::Getters;
use serde::Deserialize;
use std::time::Duration;

use super::duration_format;

/// Compaction configuration
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Getters)]
#[allow(clippy::module_name_repetitions)]
pub struct CompactConfig {
    /// The max number of historical versions processed in a single compact operation
    #[serde(default = "default_compact_batch_size")]
    #[getset(get = "pub")]
    compact_batch_size: usize,
    /// The interval between two compaction batches
    #[serde(with = "duration_format", default = "default_compact_sleep_interval")]
    #[getset(get = "pub")]
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
    /// Create a builder for `CompactConfig`
    #[inline]
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }
}

/// Builder for `CompactConfig`
#[derive(Default, Debug, Clone, Copy)]
pub struct Builder {
    /// The max number of historical versions processed in a single compact operation
    compact_batch_size: Option<usize>,
    /// The interval between two compaction batches
    compact_sleep_interval: Option<Duration>,
    /// The auto compactor config
    auto_compact_config: Option<AutoCompactConfig>,
}

impl Builder {
    /// Set the compact batch size
    #[inline]
    #[must_use]
    pub fn compact_batch_size(mut self, size: usize) -> Self {
        self.compact_batch_size = Some(size);
        self
    }

    /// Set the compact sleep interval
    #[inline]
    #[must_use]
    pub fn compact_sleep_interval(mut self, interval: Duration) -> Self {
        self.compact_sleep_interval = Some(interval);
        self
    }

    /// Set the auto compactor config
    #[inline]
    #[must_use]
    pub fn auto_compact_config(mut self, config: Option<AutoCompactConfig>) -> Self {
        self.auto_compact_config = config;
        self
    }

    /// Build the `CompactConfig`
    #[inline]
    #[must_use]
    pub fn build(self) -> CompactConfig {
        CompactConfig {
            compact_batch_size: self.compact_batch_size.unwrap_or_default(),
            compact_sleep_interval: self.compact_sleep_interval.unwrap_or_default(),
            auto_compact_config: self.auto_compact_config,
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
