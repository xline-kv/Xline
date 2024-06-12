use std::path::PathBuf;

use getset::Getters;
use serde::Deserialize;

/// Xline auth configuration object
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
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
    /// Creates a new `AuthConfigBuilder` for building `AuthConfig` objects.
    pub fn new() -> AuthConfigBuilder {
        AuthConfigBuilder {
            auth_public_key: None,
            auth_private_key: None,
        }
    }
}

/// `AuthConfigBuilder` is a builder for `AuthConfig` objects.
#[derive(Debug)]
pub struct AuthConfigBuilder {
    /// The public key file
    auth_public_key: Option<PathBuf>,
    /// The private key file
    auth_private_key: Option<PathBuf>,
}

impl AuthConfigBuilder {
    /// Sets the public key file path for the `AuthConfig`.
    pub fn auth_public_key(&mut self, path: PathBuf) -> &mut Self {
        self.auth_public_key = Some(path);
        self
    }

    /// Sets the private key file path for the `AuthConfig`.
    pub fn auth_private_key(&mut self, path: PathBuf) -> &mut Self {
        self.auth_private_key = Some(path);
        self
    }

    /// Builds the `AuthConfig` object with the provided configurations.
    pub fn build(&mut self) -> AuthConfig {
        AuthConfig {
            auth_public_key: self.auth_public_key.take(),
            auth_private_key: self.auth_private_key.take(),
        }
    }
}
