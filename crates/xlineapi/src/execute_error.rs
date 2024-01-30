#![allow(clippy::arithmetic_side_effects)] // introduced by `strum_macros::EnumIter`

use curp::cmd::{PbCodec, PbSerializeError};
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{PbExecuteError, PbExecuteErrorOuter, PbRevisions, PbUserRole};

/// Error met when executing commands
#[cfg_attr(test, derive(strum_macros::EnumIter))]
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ExecuteError {
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

    /// no space left in quota
    #[error("no space left in quota")]
    Nospace,
}

impl From<PbExecuteError> for ExecuteError {
    #[inline]
    fn from(err: PbExecuteError) -> Self {
        match err {
            PbExecuteError::KeyNotFound(_) => ExecuteError::KeyNotFound,
            PbExecuteError::RevisionTooLarge(revs) => {
                ExecuteError::RevisionTooLarge(revs.required_revision, revs.current_revision)
            }
            PbExecuteError::RevisionCompacted(revs) => {
                ExecuteError::RevisionCompacted(revs.required_revision, revs.current_revision)
            }
            PbExecuteError::LeaseNotFound(l) => ExecuteError::LeaseNotFound(l),
            PbExecuteError::LeaseExpired(l) => ExecuteError::LeaseExpired(l),
            PbExecuteError::LeaseTtlTooLarge(l) => ExecuteError::LeaseTtlTooLarge(l),
            PbExecuteError::LeaseAlreadyExists(l) => ExecuteError::LeaseAlreadyExists(l),
            PbExecuteError::AuthNotEnabled(_) => ExecuteError::AuthNotEnabled,
            PbExecuteError::AuthFailed(_) => ExecuteError::AuthFailed,
            PbExecuteError::UserNotFound(u) => ExecuteError::UserNotFound(u),
            PbExecuteError::UserAlreadyExists(u) => ExecuteError::UserAlreadyExists(u),
            PbExecuteError::UserAlreadyHasRole(ur) => {
                ExecuteError::UserAlreadyHasRole(ur.user, ur.role)
            }
            PbExecuteError::NoPasswordUser(_) => ExecuteError::NoPasswordUser,
            PbExecuteError::RoleNotFound(r) => ExecuteError::RoleNotFound(r),
            PbExecuteError::RoleAlreadyExists(r) => ExecuteError::RoleAlreadyExists(r),
            PbExecuteError::RoleNotGranted(r) => ExecuteError::RoleNotGranted(r),
            PbExecuteError::RootRoleNotExist(_) => ExecuteError::RootRoleNotExist,
            PbExecuteError::PermissionNotGranted(_) => ExecuteError::PermissionNotGranted,
            PbExecuteError::PermissionNotGiven(_) => ExecuteError::PermissionNotGiven,
            PbExecuteError::InvalidAuthManagement(_) => ExecuteError::InvalidAuthManagement,
            PbExecuteError::InvalidAuthToken(_) => ExecuteError::InvalidAuthToken,
            PbExecuteError::TokenManagerNotInit(_) => ExecuteError::TokenManagerNotInit,
            PbExecuteError::TokenNotProvided(_) => ExecuteError::TokenNotProvided,
            PbExecuteError::TokenOldRevision(revs) => {
                ExecuteError::TokenOldRevision(revs.required_revision, revs.current_revision)
            }
            PbExecuteError::DbError(e) => ExecuteError::DbError(e),
            PbExecuteError::PermissionDenied(_) => ExecuteError::PermissionDenied,
            PbExecuteError::Nospace(_) => ExecuteError::Nospace,
        }
    }
}

impl From<ExecuteError> for PbExecuteError {
    #[inline]
    fn from(err: ExecuteError) -> Self {
        match err {
            ExecuteError::KeyNotFound => PbExecuteError::KeyNotFound(()),
            ExecuteError::RevisionTooLarge(required_revision, current_revision) => {
                PbExecuteError::RevisionTooLarge(PbRevisions {
                    required_revision,
                    current_revision,
                })
            }
            ExecuteError::RevisionCompacted(required_revision, current_revision) => {
                PbExecuteError::RevisionCompacted(PbRevisions {
                    required_revision,
                    current_revision,
                })
            }
            ExecuteError::LeaseNotFound(l) => PbExecuteError::LeaseNotFound(l),
            ExecuteError::LeaseExpired(l) => PbExecuteError::LeaseExpired(l),
            ExecuteError::LeaseTtlTooLarge(l) => PbExecuteError::LeaseTtlTooLarge(l),
            ExecuteError::LeaseAlreadyExists(l) => PbExecuteError::LeaseAlreadyExists(l),
            ExecuteError::AuthNotEnabled => PbExecuteError::AuthNotEnabled(()),
            ExecuteError::AuthFailed => PbExecuteError::AuthFailed(()),
            ExecuteError::UserNotFound(u) => PbExecuteError::UserNotFound(u),
            ExecuteError::UserAlreadyExists(u) => PbExecuteError::UserAlreadyExists(u),
            ExecuteError::UserAlreadyHasRole(user, role) => {
                PbExecuteError::UserAlreadyHasRole(PbUserRole { user, role })
            }
            ExecuteError::NoPasswordUser => PbExecuteError::NoPasswordUser(()),
            ExecuteError::RoleNotFound(r) => PbExecuteError::RoleNotFound(r),
            ExecuteError::RoleAlreadyExists(r) => PbExecuteError::RoleAlreadyExists(r),
            ExecuteError::RoleNotGranted(r) => PbExecuteError::RoleNotGranted(r),
            ExecuteError::RootRoleNotExist => PbExecuteError::RootRoleNotExist(()),
            ExecuteError::PermissionNotGranted => PbExecuteError::PermissionNotGranted(()),
            ExecuteError::PermissionNotGiven => PbExecuteError::PermissionNotGiven(()),
            ExecuteError::InvalidAuthManagement => PbExecuteError::InvalidAuthManagement(()),
            ExecuteError::InvalidAuthToken => PbExecuteError::InvalidAuthToken(()),
            ExecuteError::TokenManagerNotInit => PbExecuteError::TokenManagerNotInit(()),
            ExecuteError::TokenNotProvided => PbExecuteError::TokenNotProvided(()),
            ExecuteError::TokenOldRevision(required_revision, current_revision) => {
                PbExecuteError::TokenOldRevision(PbRevisions {
                    required_revision,
                    current_revision,
                })
            }
            ExecuteError::DbError(e) => PbExecuteError::DbError(e),
            ExecuteError::PermissionDenied => PbExecuteError::PermissionDenied(()),
            ExecuteError::Nospace => PbExecuteError::Nospace(()),
        }
    }
}

impl PbCodec for ExecuteError {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        PbExecuteErrorOuter {
            error: Some(self.clone().into()),
        }
        .encode_to_vec()
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError> {
        Ok(PbExecuteErrorOuter::decode(buf)?
            .error
            .ok_or(PbSerializeError::EmptyField)?
            .into())
    }
}

// The etcd client relies on GRPC error messages for error type interpretation.
// In order to create an etcd-compatible API with Xline, it is necessary to return exact GRPC statuses to the etcd client.
// Refer to `https://github.com/etcd-io/etcd/blob/main/api/v3rpc/rpctypes/error.go` for etcd's error parsing mechanism,
// and refer to `https://github.com/etcd-io/etcd/blob/main/client/v3/doc.go` for how errors are handled by etcd client.
impl From<ExecuteError> for tonic::Status {
    #[inline]
    fn from(err: ExecuteError) -> Self {
        let (code, message) = match err {
            ExecuteError::KeyNotFound => (
                tonic::Code::InvalidArgument,
                "etcdserver: key not found".to_owned(),
            ),
            ExecuteError::RevisionTooLarge(_, _) => (
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision is a future revision".to_owned(),
            ),
            ExecuteError::RevisionCompacted(_, _) => (
                tonic::Code::OutOfRange,
                "etcdserver: mvcc: required revision has been compacted".to_owned(),
            ),
            ExecuteError::LeaseNotFound(_) => (
                tonic::Code::NotFound,
                "etcdserver: requested lease not found".to_owned(),
            ),
            ExecuteError::LeaseTtlTooLarge(_) => (
                tonic::Code::OutOfRange,
                "etcdserver: too large lease TTL".to_owned(),
            ),
            ExecuteError::LeaseAlreadyExists(_) => (
                tonic::Code::FailedPrecondition,
                "etcdserver: lease already exists".to_owned(),
            ),
            ExecuteError::AuthNotEnabled => (
                tonic::Code::FailedPrecondition,
                "etcdserver: authentication is not enabled".to_owned(),
            ),
            ExecuteError::AuthFailed => (
                tonic::Code::InvalidArgument,
                "etcdserver: authentication failed, invalid user ID or password".to_owned(),
            ),
            ExecuteError::UserNotFound(_) => (
                tonic::Code::FailedPrecondition,
                "etcdserver: user name not found".to_owned(),
            ),
            ExecuteError::UserAlreadyExists(_) => (
                tonic::Code::FailedPrecondition,
                "etcdserver: user name already exists".to_owned(),
            ),
            ExecuteError::RoleNotFound(_) => (
                tonic::Code::FailedPrecondition,
                "etcdserver: role name not found".to_owned(),
            ),
            ExecuteError::RoleAlreadyExists(_) => (
                tonic::Code::FailedPrecondition,
                "etcdserver: role name already exists".to_owned(),
            ),
            ExecuteError::RoleNotGranted(_) => (
                tonic::Code::FailedPrecondition,
                "etcdserver: role is not granted to the user".to_owned(),
            ),
            ExecuteError::RootRoleNotExist => (
                tonic::Code::FailedPrecondition,
                "etcdserver: root user does not have root role".to_owned(),
            ),
            ExecuteError::PermissionNotGranted => (
                tonic::Code::FailedPrecondition,
                "etcdserver: permission is not granted to the role".to_owned(),
            ),
            ExecuteError::PermissionNotGiven => (
                tonic::Code::InvalidArgument,
                "etcdserver: permission not given".to_owned(),
            ),
            ExecuteError::InvalidAuthToken | ExecuteError::TokenOldRevision(_, _) => (
                tonic::Code::Unauthenticated,
                "etcdserver: invalid auth token".to_owned(),
            ),
            ExecuteError::PermissionDenied => (
                tonic::Code::PermissionDenied,
                "etcdserver: permission denied".to_owned(),
            ),
            ExecuteError::InvalidAuthManagement => (
                tonic::Code::InvalidArgument,
                "etcdserver: invalid auth management".to_owned(),
            ),
            ExecuteError::Nospace => (
                tonic::Code::ResourceExhausted,
                "etcdserver: mvcc: database space exceeded".to_owned(),
            ),
            ExecuteError::LeaseExpired(_) => (tonic::Code::DeadlineExceeded, err.to_string()),
            ExecuteError::UserAlreadyHasRole(_, _)
            | ExecuteError::NoPasswordUser
            | ExecuteError::TokenManagerNotInit => {
                (tonic::Code::FailedPrecondition, err.to_string())
            }
            ExecuteError::TokenNotProvided => (tonic::Code::InvalidArgument, err.to_string()),
            ExecuteError::DbError(_) => (tonic::Code::Internal, err.to_string()),
        };

        tonic::Status::new(code, message)
    }
}

#[cfg(test)]
mod test {
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn serialization_is_ok() {
        for err in ExecuteError::iter() {
            let _decoded_err =
                <ExecuteError as PbCodec>::decode(&err.encode()).expect("decode should success");
            assert!(matches!(err, _decoded_err));
        }
    }
}
