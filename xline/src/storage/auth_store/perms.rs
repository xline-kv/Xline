use std::{
    collections::HashMap,
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};

use jsonwebtoken::{
    errors::Error as JwtError, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde::{Deserialize, Serialize};

use crate::server::command::KeyRange;

/// default token ttl
const DEFAULT_TOKEN_TTL: u64 = 300;

/// Claims of Token
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenClaims {
    /// Username
    pub(crate) username: String,
    /// Revision
    pub(crate) revision: i64,
    /// Expiration
    exp: u64,
}

/// Operations of token manager
pub(crate) trait TokenOperate {
    /// Claims type
    type Claims;

    /// Error type
    type Error;

    /// Assign a token with claims.
    fn assign(&self, username: &str, revision: i64) -> Result<String, Self::Error>;

    /// Verify token and return claims.
    fn verify(&self, token: &str) -> Result<Self::Claims, Self::Error>;
}

/// `TokenManager` of Json Web Token.
pub(crate) struct JwtTokenManager {
    /// The key used to sign the token.
    encoding_key: EncodingKey,
    /// The key used to verify the token.
    decoding_key: DecodingKey,
}

impl Debug for JwtTokenManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtTokenManager")
            .field("encoding_key", &"EncodingKey")
            .field("decoding_key", &"DecodingKey")
            .finish()
    }
}

impl JwtTokenManager {
    /// New `JwtTokenManager`
    pub(crate) fn new(encoding_key: EncodingKey, decoding_key: DecodingKey) -> Self {
        Self {
            encoding_key,
            decoding_key,
        }
    }
}

impl TokenOperate for JwtTokenManager {
    type Error = JwtError;

    type Claims = TokenClaims;

    fn assign(&self, username: &str, revision: i64) -> Result<String, Self::Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {}", e))
            .as_secs();
        let claims = TokenClaims {
            username: username.to_owned(),
            revision,
            exp: now.wrapping_add(DEFAULT_TOKEN_TTL),
        };
        let token =
            jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key)?;
        Ok(token)
    }

    fn verify(&self, token: &str) -> Result<Self::Claims, Self::Error> {
        jsonwebtoken::decode::<TokenClaims>(
            token,
            &self.decoding_key,
            &Validation::new(Algorithm::RS256),
        )
        .map(|d| d.claims)
    }
}

/// Permissions if a user
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UserPermissions {
    /// `KeyRange` has read permission
    pub(crate) read: Vec<KeyRange>,
    /// `KeyRange` has write permission
    pub(crate) write: Vec<KeyRange>,
}

impl UserPermissions {
    /// New `UserPermissions`
    pub(crate) fn new() -> Self {
        Self {
            read: Vec::new(),
            write: Vec::new(),
        }
    }
}

/// Permissions cache
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PermissionCache {
    /// Permissions of users
    pub(crate) user_permissions: HashMap<String, UserPermissions>,
    /// Role to users map
    pub(crate) role_to_users_map: HashMap<String, Vec<String>>,
}

impl PermissionCache {
    /// New `PermissionCache`
    pub(crate) fn new() -> Self {
        Self {
            user_permissions: HashMap::new(),
            role_to_users_map: HashMap::new(),
        }
    }
}
