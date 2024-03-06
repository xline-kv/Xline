use std::path::PathBuf;

use getset::Getters;
use serde::Deserialize;

/// Xline tls configuration object
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
pub struct TlsConfig {
    /// The CA certificate file used by server to verify client certificates
    #[getset(get = "pub")]
    pub server_ca_cert_path: Option<PathBuf>,
    /// The public key file used by server
    #[getset(get = "pub")]
    pub server_cert_path: Option<PathBuf>,
    /// The private key file used by server
    #[getset(get = "pub")]
    pub server_key_path: Option<PathBuf>,
    /// The CA certificate file used by client to verify server certificates
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
        server_ca_cert_path: Option<PathBuf>,
        server_cert_path: Option<PathBuf>,
        server_key_path: Option<PathBuf>,
        client_ca_cert_path: Option<PathBuf>,
        client_cert_path: Option<PathBuf>,
        client_key_path: Option<PathBuf>,
    ) -> Self {
        Self {
            server_ca_cert_path,
            server_cert_path,
            server_key_path,
            client_ca_cert_path,
            client_cert_path,
            client_key_path,
        }
    }
}
