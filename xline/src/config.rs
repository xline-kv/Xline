use anyhow::{anyhow, Result};
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf, str::FromStr};

/// Xline server configuration object
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Getters)]
pub struct XlineServerConfig {
    /// cluster configuration object
    #[getset(get = "pub")]
    cluster: ClusterConfig,
    /// log configuration object
    #[getset(get = "pub")]
    log: LogConfig,
    /// trace configuration object
    #[getset(get = "pub")]
    trace: TraceConfig,
    /// auth configuration object
    #[getset(get = "pub")]
    auth: AuthConfig,
}

/// Cluster configuration object, including cluster relevant configuration fields
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Getters)]
pub struct ClusterConfig {
    /// Get xline server name
    #[getset(get = "pub")]
    name: String,
    /// All the nodes in the xline cluster
    #[getset(get = "pub")]
    members: HashMap<String, String>,
    /// Leader node.
    #[getset(get = "pub")]
    is_leader: bool,
}

impl ClusterConfig {
    /// Generate a new `ClusterConfig` object
    #[must_use]
    #[inline]
    pub fn new(name: String, members: HashMap<String, String>, is_leader: bool) -> Self {
        Self {
            name,
            members,
            is_leader,
        }
    }
}

/// Cluster configuration object
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Getters)]
pub struct LogConfig {
    /// Log file path
    #[getset(get = "pub")]
    path: PathBuf,
    /// Log rotation strategy
    #[getset(get = "pub")]
    rotation: RotationConfig,
    /// Log verbosity level
    #[getset(get = "pub")]
    level: LevelConfig,
}

impl LogConfig {
    /// Generate a new `LogConfig` object
    #[must_use]
    #[inline]
    pub fn new(path: PathBuf, rotation: RotationConfig, level: LevelConfig) -> Self {
        Self {
            path,
            rotation,
            level,
        }
    }
}

/// Xline log rotation strategy
#[non_exhaustive]
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RotationConfig {
    /// Rotate log file in every hour
    Hourly,
    /// Rotate log file every day
    Daily,
    /// Never rotate log file
    Never,
}

impl FromStr for RotationConfig {
    type Err = anyhow::Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hourly" => Ok(RotationConfig::Hourly),
            "daily" => Ok(RotationConfig::Daily),
            "never" => Ok(RotationConfig::Never),
            _ => Err(anyhow!(format!("Failed to parse {s} to RotationConfig"))),
        }
    }
}

/// A verbosity level configuration field, including log and tracing.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::missing_docs_in_private_items, missing_docs)] // The meaning of every variant is quite straightforward, it's ok to ignore doc here.
#[non_exhaustive]
pub enum LevelConfig {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl FromStr for LevelConfig {
    type Err = anyhow::Error;
    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trace" => Ok(LevelConfig::Trace),
            "debug" => Ok(LevelConfig::Debug),
            "info" => Ok(LevelConfig::Info),
            "warn" => Ok(LevelConfig::Warn),
            "error" => Ok(LevelConfig::Error),
            _ => Err(anyhow!(format!("Failed to parse {s} to LevelConfig"))),
        }
    }
}

/// Xline tracing configuration object
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Getters)]
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
    jaeger_level: LevelConfig,
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

/// Xline tracing configuration object
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Getters)]
pub struct AuthConfig {
    /// The public key file
    #[getset(get = "pub")]
    auth_public_key: Option<PathBuf>,
    /// The private key file
    #[getset(get = "pub")]
    auth_private_key: Option<PathBuf>,
}

impl AuthConfig {
    /// Generate a new `AuthConfig` object
    #[must_use]
    #[inline]
    pub fn new(auth_public_key: Option<PathBuf>, auth_private_key: Option<PathBuf>) -> Self {
        Self {
            auth_public_key,
            auth_private_key,
        }
    }
}

#[allow(dead_code)]
impl XlineServerConfig {
    /// Generates a new `XlineServerConfig` object
    #[must_use]
    #[inline]
    pub fn new(
        cluster: ClusterConfig,
        log: LogConfig,
        trace: TraceConfig,
        auth: AuthConfig,
    ) -> Self {
        Self {
            cluster,
            log,
            trace,
            auth,
        }
    }

    /// Load configuration object from a config file
    fn load(path: &str) -> Result<Self> {
        let config = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&config)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_xline_server_config_shoule_be_loaded() {
        let result: Result<XlineServerConfig, toml::de::Error> =
            toml::from_str(include_str!("../config/xline_server.conf"));
        assert!(result.is_ok());
        let config = result.unwrap();

        assert_eq!(
            config.cluster,
            ClusterConfig::new(
                "node1".to_owned(),
                HashMap::from([
                    ("node1".to_owned(), "127, 0, 0, 1:2379".to_owned()),
                    ("node2".to_owned(), "127, 0, 0, 1:2380".to_owned()),
                    ("node3".to_owned(), "127, 0, 0, 1:2381".to_owned())
                ]),
                true
            )
        );
        assert_eq!(
            config.log,
            LogConfig::new(
                PathBuf::from("/tmp/xline"),
                RotationConfig::Daily,
                LevelConfig::Info,
            )
        );
        assert_eq!(
            config.trace,
            TraceConfig::new(
                false,
                false,
                PathBuf::from("./jaeger_jsons"),
                LevelConfig::Info
            )
        );
    }
}
