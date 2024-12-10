use getset::Getters;
use serde::Deserialize;

use super::prelude::EngineConfig;

/// Storage Configuration
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Getters)]
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
pub struct StorageConfig {
    /// Engine Configuration
    #[serde(default = "EngineConfig::default")]
    #[getset(get = "pub")]
    engine: EngineConfig,
    /// Quota
    #[serde(default = "default_quota")]
    #[getset(get = "pub")]
    quota: u64,
}

impl StorageConfig {
    /// Create a builder for `StorageConfig`
    #[inline]
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl Default for StorageConfig {
    #[inline]
    fn default() -> Self {
        Self {
            engine: EngineConfig::default(),
            quota: default_quota(),
        }
    }
}

/// Builder for `StorageConfig`
#[derive(Default, Debug)]
pub struct Builder {
    /// Engine Configuration
    engine: Option<EngineConfig>,
    /// Quota
    quota: Option<u64>,
}

impl Builder {
    /// Set the engine configuration
    #[inline]
    #[must_use]
    pub fn engine(mut self, engine: EngineConfig) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Set the quota
    #[inline]
    #[must_use]
    pub fn quota(mut self, quota: u64) -> Self {
        self.quota = Some(quota);
        self
    }

    /// Build the `StorageConfig` and apply defaults where needed
    #[inline]
    #[must_use]
    pub fn build(self) -> StorageConfig {
        StorageConfig {
            engine: self.engine.unwrap_or_default(),
            quota: self.quota.unwrap_or_else(default_quota),
        }
    }
}

/// Default quota: 8GB
#[inline]
#[must_use]
pub fn default_quota() -> u64 {
    // 8 * 1024 * 1024 * 1024
    0x0002_0000_0000
}
