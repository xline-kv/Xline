use std::time::Duration;

use getset::Getters;
use serde::Deserialize;

/// Compaction configuration
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Getters)]
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
pub struct CompactConfig {
    /// The max number of historical versions processed in a single compact operation
    #[getset(get = "pub")]
    #[serde(default = "default_compact_batch_size")]
    pub compact_batch_size: usize,
    /// The interval between two compaction batches
    #[getset(get = "pub")]
    #[serde(with = "duration_format", default = "default_compact_sleep_interval")]
    pub compact_sleep_interval: Duration,
    /// The auto compactor config
    #[getset(get = "pub")]
    pub auto_compact_config: Option<AutoCompactConfig>,
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
