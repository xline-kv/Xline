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
    /// Create a new `TlsConfig` object
    #[must_use]
    #[inline]
    pub fn new(
        peer_ca_cert_path: Option<PathBuf>,
        peer_cert_path: Option<PathBuf>,
        peer_key_path: Option<PathBuf>,
        client_ca_cert_path: Option<PathBuf>,
        client_cert_path: Option<PathBuf>,
        client_key_path: Option<PathBuf>,
    ) -> Self {
        Self {
            peer_ca_cert_path,
            peer_cert_path,
            peer_key_path,
            client_ca_cert_path,
            client_cert_path,
            client_key_path,
        }
    }
    
    /// Whether the server tls is enabled
    #[must_use]
    #[inline]
    pub fn server_tls_enabled(&self) -> bool {
        self.peer_cert_path.is_some() && self.peer_key_path.is_some()
    }
}