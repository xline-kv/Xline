use std::path::PathBuf;

use getset::Getters;
use serde::Deserialize;

use tracing_appender::rolling::RollingFileAppender;

/// Log verbosity level alias
#[allow(clippy::module_name_repetitions)]
pub type LevelConfig = tracing::metadata::LevelFilter;

/// Log configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct LogConfig {
    /// Log file path
    #[getset(get = "pub")]
    #[serde(default)]
    path: Option<PathBuf>,
    /// Log rotation strategy
    #[getset(get = "pub")]
    #[serde(with = "rotation_format", default = "default_rotation")]
    rotation: RotationConfig,
    /// Log verbosity level
    #[getset(get = "pub")]
    #[serde(with = "level_format", default = "default_log_level")]
    level: LevelConfig,
}

impl Default for LogConfig {
    #[inline]
    fn default() -> Self {
        Self {
            path: None,
            rotation: default_rotation(),
            level: default_log_level(),
        }
    }
}

/// `LevelConfig` deserialization formatter
pub mod level_format {
    use serde::{Deserialize, Deserializer};

    use super::LevelConfig;
    use crate::parse_log_level;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<LevelConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_log_level(&s).map_err(serde::de::Error::custom)
    }
}

/// default log level
#[must_use]
#[inline]
pub const fn default_log_level() -> LevelConfig {
    LevelConfig::INFO
}

impl LogConfig {
    /// Create a builder for `LogConfig`
    #[must_use]
    #[inline]
    pub fn builder() -> Builder {
        Builder::default()
    }
}

/// Builder for `LogConfig`
#[derive(Default, Debug)]
pub struct Builder {
    /// Log file path
    path: Option<PathBuf>,
    /// Log rotation strategy
    rotation: Option<RotationConfig>,
    /// Log verbosity level
    level: Option<LevelConfig>,
}

impl Builder {
    /// Set the log file path
    #[inline]
    #[must_use]
    pub fn path(mut self, path: Option<PathBuf>) -> Self {
        self.path = path;
        self
    }

    /// Set the log rotation strategy
    #[inline]
    #[must_use]
    pub fn rotation(mut self, rotation: RotationConfig) -> Self {
        self.rotation = Some(rotation);
        self
    }

    /// Set the log verbosity level
    #[inline]
    #[must_use]
    pub fn level(mut self, level: LevelConfig) -> Self {
        self.level = Some(level);
        self
    }

    /// Build the `LogConfig` and apply defaults where needed
    #[inline]
    #[must_use]
    pub fn build(self) -> LogConfig {
        LogConfig {
            path: self.path,
            rotation: self.rotation.unwrap_or_else(default_rotation),
            level: self.level.unwrap_or_else(default_log_level),
        }
    }
}

/// Xline log rotation strategy
#[non_exhaustive]
#[allow(clippy::module_name_repetitions)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum RotationConfig {
    /// Rotate log file in every hour
    Hourly,
    /// Rotate log file every day
    Daily,
    /// Never rotate log file
    Never,
}

impl std::fmt::Display for RotationConfig {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            RotationConfig::Hourly => write!(f, "hourly"),
            RotationConfig::Daily => write!(f, "daily"),
            RotationConfig::Never => write!(f, "never"),
        }
    }
}

/// `RotationConfig` deserialization formatter
pub mod rotation_format {
    use serde::{Deserialize, Deserializer};

    use super::RotationConfig;
    use crate::parse_rotation;

    /// deserializes a cluster log rotation strategy
    #[allow(single_use_lifetimes)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<RotationConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_rotation(&s).map_err(serde::de::Error::custom)
    }
}

/// default log rotation strategy
#[must_use]
#[inline]
pub const fn default_rotation() -> RotationConfig {
    RotationConfig::Never
}

/// Generates a `RollingFileAppender` from the given `RotationConfig` and `name`
#[must_use]
#[inline]
pub fn file_appender(
    rotation: RotationConfig,
    file_path: &PathBuf,
    name: &str,
) -> RollingFileAppender {
    match rotation {
        RotationConfig::Hourly => {
            tracing_appender::rolling::hourly(file_path, format!("xline_{name}.log"))
        }
        RotationConfig::Daily => {
            tracing_appender::rolling::daily(file_path, format!("xline_{name}.log"))
        }
        RotationConfig::Never => {
            tracing_appender::rolling::never(file_path, format!("xline_{name}.log"))
        }
        #[allow(unreachable_patterns)]
        // It's ok because `parse_rotation` have check the validity before.
        _ => unreachable!("should not call file_appender when parse_rotation failed"),
    }
}
