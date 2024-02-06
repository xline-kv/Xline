use std::{collections::HashMap, fmt::Debug};

use jsonwebtoken::{
    errors::Error as JwtError, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use merged_range::MergedRange;
use serde::{Deserialize, Serialize};
use utils::timestamp;
use xlineapi::{command::KeyRange, AuthInfo};

use crate::rpc::{Permission, Type};

/// default token ttl
const DEFAULT_TOKEN_TTL: u64 = 300;

/// Claims of Token
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TokenClaims {
    /// Username
    pub(super) username: String,
    /// Revision
    pub(super) revision: i64,
    /// Expiration
    exp: u64,
}

impl From<TokenClaims> for AuthInfo {
    #[inline]
    fn from(value: TokenClaims) -> Self {
        Self {
            username: value.username,
            auth_revision: value.revision,
        }
    }
}

/// Operations of token manager
pub(super) trait TokenOperate {
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
pub(super) struct JwtTokenManager {
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
        let now = timestamp();
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct UserPermissions {
    /// `MergedRange` has read permission
    pub(super) read: MergedRange<Vec<u8>>,
    /// `MergedRange` has write permission
    pub(super) write: MergedRange<Vec<u8>>,
}

impl UserPermissions {
    /// New `UserPermissions`
    pub(super) fn new() -> Self {
        Self {
            read: MergedRange::new(),
            write: MergedRange::new(),
        }
    }

    /// Insert a permission to `UserPermissions`
    pub(super) fn insert(&mut self, perm: Permission) {
        let range = KeyRange::new(perm.key, perm.range_end).unpack();
        #[allow(clippy::unwrap_used)] // safe unwrap
        match Type::try_from(perm.perm_type).unwrap() {
            Type::Readwrite => {
                self.read.insert(range.clone());
                self.write.insert(range);
            }
            Type::Write => {
                self.write.insert(range);
            }
            Type::Read => {
                self.read.insert(range);
            }
        }
    }
}

/// Permissions cache
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PermissionCache {
    /// Permissions of users
    pub(super) user_permissions: HashMap<String, UserPermissions>,
    /// Role to users map
    pub(super) role_to_users_map: HashMap<String, Vec<String>>,
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
