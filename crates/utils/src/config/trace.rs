use std::path::PathBuf;

use getset::Getters;
use serde::Deserialize;

use super::prelude::{default_log_level, level_format, LevelConfig};

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
    /// Create a builder for `TraceConfig`
    #[inline]
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }
}

/// Builder for `TraceConfig`
#[derive(Default, Debug)]
pub struct Builder {
    /// Open jaeger online, sending data to jaeger agent directly
    jaeger_online: Option<bool>,
    /// Open jaeger offline, saving data to the `jaeger_output_dir`
    jaeger_offline: Option<bool>,
    /// The dir path to save the data when `jaeger_offline` is on
    jaeger_output_dir: Option<PathBuf>,
    /// The verbosity level of tracing
    jaeger_level: Option<LevelConfig>,
}

impl Builder {
    /// Set `jaeger_online`
    #[inline]
    #[must_use]
    pub fn jaeger_online(mut self, value: bool) -> Self {
        self.jaeger_online = Some(value);
        self
    }

    /// Set `jaeger_offline`
    #[inline]
    #[must_use]
    pub fn jaeger_offline(mut self, value: bool) -> Self {
        self.jaeger_offline = Some(value);
        self
    }

    /// Set `jaeger_output_dir`
    #[inline]
    #[must_use]
    pub fn jaeger_output_dir(mut self, value: PathBuf) -> Self {
        self.jaeger_output_dir = Some(value);
        self
    }

    /// Set `jaeger_level`
    #[inline]
    #[must_use]
    pub fn jaeger_level(mut self, value: LevelConfig) -> Self {
        self.jaeger_level = Some(value);
        self
    }

    /// Build the `TraceConfig` object
    #[inline]
    #[must_use]
    pub fn build(self) -> TraceConfig {
        TraceConfig {
            jaeger_online: self.jaeger_online.unwrap_or(false),
            jaeger_offline: self.jaeger_offline.unwrap_or(false),
            jaeger_output_dir: self.jaeger_output_dir.unwrap_or_else(|| "".into()),
            jaeger_level: self.jaeger_level.unwrap_or_else(default_log_level),
        }
    }
}
