use getset::Getters;
use serde::Deserialize;
use std::path::PathBuf;

/// Xline tracing configuration object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Getters, Default)]
pub struct AuthConfig {
    /// The public key file
    #[getset(get = "pub")]
    auth_public_key: Option<PathBuf>,
    /// The private key file
    #[getset(get = "pub")]
    auth_private_key: Option<PathBuf>,
}

impl AuthConfig {
    /// Create a builder for `AuthConfig`
    #[inline]
    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }
}

/// Builder for `AuthConfig`
#[derive(Default, Debug)]
pub struct Builder {
    /// The public key file
    auth_public_key: Option<PathBuf>,
    /// The private key file
    auth_private_key: Option<PathBuf>,
}

impl Builder {
    /// Set the public key file
    #[inline]
    #[must_use]
    pub fn auth_public_key(mut self, path: Option<PathBuf>) -> Self {
        self.auth_public_key = path;
        self
    }

    /// Set the private key file
    #[inline]
    #[must_use]
    pub fn auth_private_key(mut self, path: Option<PathBuf>) -> Self {
        self.auth_private_key = path;
        self
    }

    /// Build the `AuthConfig`
    #[inline]
    #[must_use]
    pub fn build(self) -> AuthConfig {
        AuthConfig {
            auth_public_key: self.auth_public_key,
            auth_private_key: self.auth_private_key,
        }
    }
}
