/// Xline auth configuration module
pub mod auth;
/// Curp client module
pub mod client;
/// Cluster configuration module
pub mod cluster;
/// Compaction configuration module
pub mod compact;
/// Curp server module
pub mod curp;
/// Engine Configuration module
pub mod engine;
/// Log configuration module
pub mod log;
/// Xline metrics configuration module
pub mod metrics;
/// Prelude module
pub mod prelude;
/// Xline server configuration
pub mod server;
/// Storage Configuration module
pub mod storage;
/// Xline tls configuration module
pub mod tls;
/// Xline tracing configuration module
pub mod trace;

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

/// batch size deserialization formatter
pub mod bytes_format {
    use serde::{Deserialize, Deserializer};

    use crate::parse_batch_bytes;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)] //  the false positive case blocks us
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_batch_bytes(&s).map_err(serde::de::Error::custom)
    }
}
