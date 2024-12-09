use std::path::PathBuf;

use getset::Getters;
use serde::Deserialize;

/// Xline tls configuration object
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
pub struct TlsConfig {
    /// The CA certificate file used by peer to verify client certificates
    #[getset(get = "pub")]
    pub peer_ca_cert_path: Option<PathBuf>,
    /// The public key file used by peer
    #[getset(get = "pub")]
    pub peer_cert_path: Option<PathBuf>,
    /// The private key file used by peer
    #[getset(get = "pub")]
    pub peer_key_path: Option<PathBuf>,
    /// The CA certificate file used by client to verify peer certificates
    #[getset(get = "pub")]
    pub client_ca_cert_path: Option<PathBuf>,
    /// The public key file used by client
    #[getset(get = "pub")]
    pub client_cert_path: Option<PathBuf>,
    /// The private key file used by client
    #[getset(get = "pub")]
    pub client_key_path: Option<PathBuf>,
}

impl TlsConfig {
    /// Create a builder for `TlsConfig`
    #[inline]
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Whether the server tls is enabled
    #[must_use]
    #[inline]
    pub fn server_tls_enabled(&self) -> bool {
        self.peer_cert_path.is_some() && self.peer_key_path.is_some()
    }
}

/// Builder for `TlsConfig`
#[derive(Default, Debug)]
pub struct Builder {
    /// The CA certificate file used by peer to verify client certificates
    peer_ca_cert_path: Option<PathBuf>,
    /// The public key file used by peer
    peer_cert_path: Option<PathBuf>,
    /// The private key file used by peer
    peer_key_path: Option<PathBuf>,
    /// The CA certificate file used by client to verify peer c
    client_ca_cert_path: Option<PathBuf>,
    /// The public key file used by client
    client_cert_path: Option<PathBuf>,
    /// The private key file used by client
    client_key_path: Option<PathBuf>,
}

impl Builder {
    /// Set the `peer_ca_cert_path`
    #[inline]
    #[must_use]
    pub fn peer_ca_cert_path(mut self, path: Option<PathBuf>) -> Self {
        self.peer_ca_cert_path = path;
        self
    }

    /// Set the `peer_cert_path`
    #[inline]
    #[must_use]
    pub fn peer_cert_path(mut self, path: Option<PathBuf>) -> Self {
        self.peer_cert_path = path;
        self
    }

    /// Set the `peer_key_path`
    #[inline]
    #[must_use]
    pub fn peer_key_path(mut self, path: Option<PathBuf>) -> Self {
        self.peer_key_path = path;
        self
    }

    /// Set the `client_ca_cert_path`
    #[inline]
    #[must_use]
    pub fn client_ca_cert_path(mut self, path: Option<PathBuf>) -> Self {
        self.client_ca_cert_path = path;
        self
    }

    /// Set the `client_cert_path`
    #[inline]
    #[must_use]
    pub fn client_cert_path(mut self, path: Option<PathBuf>) -> Self {
        self.client_cert_path = path;
        self
    }

    /// Set the `client_key_path`
    #[inline]
    #[must_use]
    pub fn client_key_path(mut self, path: Option<PathBuf>) -> Self {
        self.client_key_path = path;
        self
    }

    /// Build the `TlsConfig` object
    #[inline]
    #[must_use]
    pub fn build(self) -> TlsConfig {
        TlsConfig {
            peer_ca_cert_path: self.peer_ca_cert_path,
            peer_cert_path: self.peer_cert_path,
            peer_key_path: self.peer_key_path,
            client_ca_cert_path: self.client_ca_cert_path,
            client_cert_path: self.client_cert_path,
            client_key_path: self.client_key_path,
        }
    }
}
