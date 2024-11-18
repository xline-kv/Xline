use serde::Deserialize;
use std::path::PathBuf;

/// Engine Configuration
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "type",
    content = "data_dir",
    rename_all(deserialize = "lowercase")
)]
pub enum EngineConfig {
    /// Memory Storage Engine
    Memory,
    /// RocksDB Storage Engine
    RocksDB(PathBuf),
}

impl Default for EngineConfig {
    #[inline]
    fn default() -> Self {
        Self::Memory
    }
}
