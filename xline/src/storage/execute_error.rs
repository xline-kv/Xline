use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error met when executing commands
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ExecuteError {
    /// Invalid Request Error
    #[error("invalid request")]
    InvalidRequest(String),

    /// Key not found
    #[error("key not found")]
    KeyNotFound,
    /// Revision is higher than current
    #[error("required revision {0} is higher than current revision {1}")]
    RevisionTooLarge(i64, i64),
    /// Revision compacted
    #[error("required revision {0} has been compacted, compacted revision is {1}")]
    RevisionCompacted(i64, i64),

    /// Lease not found
    #[error("lease {0} not found")]
    LeaseNotFound(i64),
    /// Lease is expired
    #[error("lease {0} is expired")]
    LeaseExpired(i64),
    /// Lease ttl is too large
    #[error("lease ttl is too large: {0}")]
    LeaseTtlTooLarge(i64),
    /// Lease already exists
    #[error("lease {0} already exists")]
    LeaseAlreadyExists(i64),

    // AuthErrors
    /// Auth is not enabled
    #[error("auth is not enabled")]
    AuthNotEnabled,
    /// Auth failed
    #[error("invalid username or password")]
    AuthFailed,
    /// User not found
    #[error("user {0} not found")]
    UserNotFound(String),
    /// User already exists
    #[error("user {0} already exists")]
    UserAlreadyExists(String),
    /// User already has role
    #[error("user {0} already has role {1}")]
    UserAlreadyHasRole(String, String),
    /// Password was given for no password user
    #[error("password was given for no password user")]
    NoPasswordUser,
    /// Role not found
    #[error("role {0} not found")]
    RoleNotFound(String),
    /// Role already exists
    #[error("role {0} already exists")]
    RoleAlreadyExists(String),
    /// Role not granted
    #[error("role {0} is not granted to the user")]
    RoleNotGranted(String),
    /// Root role not exist
    #[error("root user does not have root role")]
    RootRoleNotExist,
    /// Permission not granted
    #[error("permission not granted to the role")]
    PermissionNotGranted,
    /// Permission not given
    #[error("permission not given")]
    PermissionNotGiven,
    /// Invalid auth management
    #[error("invalid auth management")]
    InvalidAuthManagement,
    /// Invalid auth token
    #[error("invalid auth token")]
    InvalidAuthToken,
    /// Token manager is not initialized
    #[error("token manager is not initialized")]
    TokenManagerNotInit,
    /// Token is not provided
    #[error("token is not provided")]
    TokenNotProvided,
    /// Token is expired
    #[error("token's revision {0} is older than current revision {1}")]
    TokenOldRevision(i64, i64),

    /// Db error
    #[error("db error: {0}")]
    DbError(String),

    /// Permission denied Error
    #[error("permission denied")]
    PermissionDenied,
}

impl From<ExecuteError> for tonic::Status {
    #[inline]
    fn from(err: ExecuteError) -> Self {
        let code = match err {
            ExecuteError::InvalidRequest(_)
            | ExecuteError::AuthFailed
            | ExecuteError::PermissionNotGiven
            | ExecuteError::InvalidAuthManagement
            | ExecuteError::TokenNotProvided => tonic::Code::InvalidArgument,
            ExecuteError::LeaseExpired(_) => tonic::Code::DeadlineExceeded,
            ExecuteError::KeyNotFound
            | ExecuteError::LeaseNotFound(_)
            | ExecuteError::UserNotFound(_)
            | ExecuteError::RoleNotFound(_) => tonic::Code::NotFound,
            ExecuteError::LeaseAlreadyExists(_)
            | ExecuteError::UserAlreadyExists(_)
            | ExecuteError::RoleAlreadyExists(_) => tonic::Code::AlreadyExists,
            ExecuteError::PermissionDenied => tonic::Code::PermissionDenied,
            ExecuteError::AuthNotEnabled
            | ExecuteError::UserAlreadyHasRole(_, _)
            | ExecuteError::NoPasswordUser
            | ExecuteError::RoleNotGranted(_)
            | ExecuteError::RootRoleNotExist
            | ExecuteError::PermissionNotGranted
            | ExecuteError::TokenManagerNotInit => tonic::Code::FailedPrecondition,
            ExecuteError::LeaseTtlTooLarge(_)
            | ExecuteError::RevisionTooLarge(_, _)
            | ExecuteError::RevisionCompacted(_, _) => tonic::Code::OutOfRange,
            ExecuteError::DbError(_) => tonic::Code::Internal,
            ExecuteError::InvalidAuthToken | ExecuteError::TokenOldRevision(_, _) => {
                tonic::Code::Unauthenticated
            }
        };
        Self::new(code, err.to_string())
    }
}
