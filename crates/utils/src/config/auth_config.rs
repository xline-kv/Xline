use std::path::PathBuf;

use getset::Getters;
use serde::Deserialize;

/// Xline auth configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
pub struct AuthConfig {
    /// The public key file
    #[getset(get = "pub")]
    pub auth_public_key: Option<PathBuf>,
    /// The private key file
    #[getset(get = "pub")]
    pub auth_private_key: Option<PathBuf>,
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
