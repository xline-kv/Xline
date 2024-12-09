use getset::Getters;
use serde::Deserialize;

/// Metrics push protocol format
pub mod protocol_format {
    use serde::{Deserialize, Deserializer};

    use super::PushProtocol;
    use crate::parse_metrics_push_protocol;

    /// deserializes a cluster duration
    #[allow(single_use_lifetimes)]
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<PushProtocol, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_metrics_push_protocol(&s).map_err(serde::de::Error::custom)
    }
}

/// Xline metrics configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters)]
pub struct MetricsConfig {
    /// Enable or not
    #[serde(default = "default_metrics_enable")]
    #[getset(get = "pub")]
    enable: bool,
    /// The http port to expose
    #[serde(default = "default_metrics_port")]
    #[getset(get = "pub")]
    port: u16,
    /// The http path to expose
    #[serde(default = "default_metrics_path")]
    #[getset(get = "pub")]
    path: String,
    /// Enable push or not
    #[serde(default = "default_metrics_push")]
    #[getset(get = "pub")]
    push: bool,
    /// Push endpoint
    #[serde(default = "default_metrics_push_endpoint")]
    #[getset(get = "pub")]
    push_endpoint: String,
    /// Push protocol
    #[serde(with = "protocol_format", default = "default_metrics_push_protocol")]
    #[getset(get = "pub")]
    push_protocol: PushProtocol,
}

impl MetricsConfig {
    /// Create a new `MetricsConfig` builder
    #[must_use]
    #[inline]
    pub fn builder() -> Builder {
        Builder::default()
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

/// Builder for `MetricsConfig`
#[derive(Debug, Default)]
pub struct Builder {
    /// Enable or not
    enable: Option<bool>,
    /// The http port to expose
    port: Option<u16>,
    /// The http path to expose
    path: Option<String>,
    /// Enable push or not
    push: Option<bool>,
    /// Push endpoint
    push_endpoint: Option<String>,
    /// Push protocol
    push_protocol: Option<PushProtocol>,
}

impl Builder {
    /// Set the `enable` flag
    #[must_use]
    #[inline]
    pub fn enable(mut self, enable: bool) -> Self {
        self.enable = Some(enable);
        self
    }

    /// Set the `port`
    #[must_use]
    #[inline]
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the `path`
    #[must_use]
    #[inline]
    pub fn path<S: Into<String>>(mut self, path: S) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the `push` flag
    #[must_use]
    #[inline]
    pub fn push(mut self, push: bool) -> Self {
        self.push = Some(push);
        self
    }

    /// Set the `push_endpoint`
    #[must_use]
    #[inline]
    pub fn push_endpoint<S: Into<String>>(mut self, push_endpoint: S) -> Self {
        self.push_endpoint = Some(push_endpoint.into());
        self
    }

    /// Set the `push_protocol`
    #[must_use]
    #[inline]
    pub fn push_protocol(mut self, push_protocol: PushProtocol) -> Self {
        self.push_protocol = Some(push_protocol);
        self
    }

    /// Build the `MetricsConfig`
    #[must_use]
    #[inline]
    pub fn build(self) -> MetricsConfig {
        MetricsConfig {
            enable: self.enable.unwrap_or_else(default_metrics_enable),
            port: self.port.unwrap_or_else(default_metrics_port),
            path: self.path.unwrap_or_else(default_metrics_path),
            push: self.push.unwrap_or_else(default_metrics_push),
            push_endpoint: self
                .push_endpoint
                .unwrap_or_else(default_metrics_push_endpoint),
            push_protocol: self
                .push_protocol
                .unwrap_or_else(default_metrics_push_protocol),
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
pub fn default_metrics_push_protocol() -> PushProtocol {
    PushProtocol::GRPC
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
pub enum PushProtocol {
    /// HTTP protocol
    HTTP,
    /// GRPC protocol
    #[default]
    GRPC,
}

impl std::fmt::Display for PushProtocol {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            PushProtocol::HTTP => write!(f, "http"),
            PushProtocol::GRPC => write!(f, "grpc"),
        }
    }
}
