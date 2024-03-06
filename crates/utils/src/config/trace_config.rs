use std::path::PathBuf;

use getset::Getters;
use serde::Deserialize;

use super::log_config::{default_log_level, level_format, LevelConfig};

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
