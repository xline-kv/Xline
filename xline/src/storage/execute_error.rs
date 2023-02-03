use thiserror::Error;

/// Error met when executing commands
// #[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
// #[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub(crate) enum ExecuteError {
    /// Kv error
    #[error("kv error: {0}")]
    KvError(String),
    /// Lease error
    #[error("lease error: {0}")]
    LeaseError(String),
    /// Auth error
    #[error("auth error: {0}")]
    AuthError(String),
    /// Db error
    #[error("db error: {0}")]
    DbError(String),
    /// Permission denied
    #[error("permission denied")]
    PermissionDenied,
}

impl ExecuteError {
    /// Key not found
    pub(crate) fn key_not_found() -> Self {
        Self::KvError("key not found".to_owned())
    }

    /// Lease not found
    pub(crate) fn lease_not_found(lease_id: i64) -> Self {
        Self::LeaseError(format!("lease {lease_id} not found"))
    }

    /// Lease is expired
    pub(crate) fn lease_expired(lease_id: i64) -> Self {
        Self::LeaseError(format!("lease {lease_id} is expired"))
    }

    /// Lease ttl is too large
    pub(crate) fn lease_ttl_too_large(ttl: i64) -> Self {
        Self::LeaseError(format!("lease ttl is too large: {ttl}"))
    }

    /// Lease already exists
    pub(crate) fn lease_already_exists(lease_id: i64) -> Self {
        Self::LeaseError(format!("lease {lease_id} already exists"))
    }

    /// Lease current node is not leader
    pub(crate) fn lease_not_leader() -> Self {
        Self::LeaseError("current node is not leader".to_owned())
    }

    /// Auth is not enabled
    pub(crate) fn auth_not_enabled() -> Self {
        Self::AuthError("auth is not enabled".to_owned())
    }

    /// Auth failed
    pub(crate) fn auth_failed() -> Self {
        Self::AuthError("invalid username or password".to_owned())
    }

    /// User not found
    pub(crate) fn user_not_found(username: &str) -> Self {
        Self::AuthError(format!("user {username} not found"))
    }

    /// User already exists
    pub(crate) fn user_already_exists(username: &str) -> Self {
        Self::AuthError(format!("user {username} already exists"))
    }

    /// User already has role
    pub(crate) fn user_already_has_role(username: &str, rolename: &str) -> Self {
        Self::AuthError(format!("user {username} already has role {rolename}"))
    }

    /// Permission already exists
    pub(crate) fn no_password_user() -> Self {
        Self::AuthError("password was given for no password user".to_owned())
    }

    /// Role not found
    pub(crate) fn role_not_found(rolename: &str) -> Self {
        Self::AuthError(format!("role {rolename} not found"))
    }

    /// Role already exists
    pub(crate) fn role_already_exists(rolename: &str) -> Self {
        Self::AuthError(format!("role {rolename} already exists"))
    }

    /// Role not granted
    pub(crate) fn role_not_granted(rolename: &str) -> Self {
        Self::AuthError(format!("role {rolename} is not granted to the user"))
    }
    /// Root role not exist
    pub(crate) fn root_role_not_exist() -> Self {
        Self::AuthError("root user does not have root role".to_owned())
    }

    /// Permission not granted
    pub(crate) fn permission_not_granted() -> Self {
        Self::AuthError("permission not granted to the role".to_owned())
    }

    /// Permission not given
    pub(crate) fn permission_not_given() -> Self {
        Self::AuthError("permission not given".to_owned())
    }

    /// Invalid auth management
    pub(crate) fn invalid_auth_management() -> Self {
        Self::AuthError("invalid auth management".to_owned())
    }

    /// Invalid auth token
    pub(crate) fn invalid_auth_token() -> Self {
        Self::AuthError("invalid auth token".to_owned())
    }

    /// Token manager is not initialized
    pub(crate) fn token_manager_not_init() -> Self {
        Self::AuthError("token manager is not initialized".to_owned())
    }

    /// Token is not provided
    pub(crate) fn token_not_provided() -> Self {
        Self::AuthError("token is not provided".to_owned())
    }

    /// Token is expired
    pub(crate) fn token_old_revision() -> Self {
        Self::AuthError("token's revision is older than current revision".to_owned())
    }
}
