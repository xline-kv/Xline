use getset::Getters;
use serde::Deserialize;

/// Xline metrics configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct MetricsConfig {
   /// Enable or not
   #[getset(get = "pub")]
   #[serde(default = "default_metrics_enable")]
   pub enable: bool,
   /// The http port to expose
   #[getset(get = "pub")]
   #[serde(default = "default_metrics_port")]
   pub port: u16,
   /// The http path to expose
   #[getset(get = "pub")]
   #[serde(default = "default_metrics_path")]
   pub path: String,
   /// Enable push or not
   #[getset(get = "pub")]
   #[serde(default = "default_metrics_push")]
   pub push: bool,
   /// Push endpoint
   #[getset(get = "pub")]
   #[serde(default = "default_metrics_push_endpoint")]
   pub push_endpoint: String,
   /// Push protocol
   #[getset(get = "pub")]
   #[serde(with = "protocol_format", default = "default_metrics_push_protocol")]
   pub push_protocol: MetricsPushProtocol,
}

impl MetricsConfig {
   /// Create a new `MetricsConfig`
   #[must_use]
   #[inline]
   pub fn new(
       enable: bool,
       port: u16,
       path: String,
       push: bool,
       push_endpoint: String,
       push_protocol: MetricsPushProtocol,
   ) -> Self {
       Self {
           enable,
           port,
           path,
           push,
           push_endpoint,
           push_protocol,
       }
   }
}

impl Default for MetricsConfig {
   #[inline]
   fn default() -> Self {
       Self {
           enable: default_metrics_enable(),
           port: default_metrics_port(),
           path: default_metrics_path(),
           push: default_metrics_push(),
           push_endpoint: default_metrics_push_endpoint(),
           push_protocol: default_metrics_push_protocol(),
       }
   }
}

/// Default metrics enable
#[must_use]
#[inline]
pub const fn default_metrics_enable() -> bool {
   true
}

/// Default metrics port
#[must_use]
#[inline]
pub const fn default_metrics_port() -> u16 {
   9100
}

/// Default metrics path
#[must_use]
#[inline]
pub fn default_metrics_path() -> String {
   "/metrics".to_owned()
}

/// Default metrics push option
#[must_use]
#[inline]
pub fn default_metrics_push() -> bool {
   false
}

/// Default metrics push protocol
#[must_use]
#[inline]
pub fn default_metrics_push_protocol() -> MetricsPushProtocol {
   MetricsPushProtocol::GRPC
}

/// Default metrics push endpoint
#[must_use]
#[inline]
pub fn default_metrics_push_endpoint() -> String {
   "http://127.0.0.1:4318".to_owned()
}

/// Xline metrics push protocol
#[non_exhaustive]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum MetricsPushProtocol {
    /// HTTP protocol
    HTTP,
    /// GRPC protocol
    #[default]
    GRPC,
}

impl std::fmt::Display for MetricsPushProtocol {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            MetricsPushProtocol::HTTP => write!(f, "http"),
            MetricsPushProtocol::GRPC => write!(f, "grpc"),
        }
    }
}

/// Metrics push protocol format
pub mod protocol_format {
    use serde::{Deserialize, Deserializer};
    use super::MetricsPushProtocol;
    use crate::parse_metrics_push_protocol;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<MetricsPushProtocol, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_metrics_push_protocol(&s).map_err(serde::de::Error::custom)
    }
}
