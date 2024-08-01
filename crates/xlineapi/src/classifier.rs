use super::RequestWrapper;

/// Backend store of request
#[allow(missing_docs)]
#[derive(Debug, PartialEq, Eq)]
pub enum RequestBackend {
    Kv,
    Auth,
    Lease,
    Alarm,
}

impl From<&RequestWrapper> for RequestBackend {
    fn from(value: &RequestWrapper) -> Self {
        match *value {
            RequestWrapper::PutRequest(_)
            | RequestWrapper::RangeRequest(_)
            | RequestWrapper::DeleteRangeRequest(_)
            | RequestWrapper::TxnRequest(_)
            | RequestWrapper::CompactionRequest(_) => RequestBackend::Kv,
            RequestWrapper::AuthEnableRequest(_)
            | RequestWrapper::AuthDisableRequest(_)
            | RequestWrapper::AuthStatusRequest(_)
            | RequestWrapper::AuthRoleAddRequest(_)
            | RequestWrapper::AuthRoleDeleteRequest(_)
            | RequestWrapper::AuthRoleGetRequest(_)
            | RequestWrapper::AuthRoleGrantPermissionRequest(_)
            | RequestWrapper::AuthRoleListRequest(_)
            | RequestWrapper::AuthRoleRevokePermissionRequest(_)
            | RequestWrapper::AuthUserAddRequest(_)
            | RequestWrapper::AuthUserChangePasswordRequest(_)
            | RequestWrapper::AuthUserDeleteRequest(_)
            | RequestWrapper::AuthUserGetRequest(_)
            | RequestWrapper::AuthUserGrantRoleRequest(_)
            | RequestWrapper::AuthUserListRequest(_)
            | RequestWrapper::AuthUserRevokeRoleRequest(_)
            | RequestWrapper::AuthenticateRequest(_) => RequestBackend::Auth,
            RequestWrapper::LeaseGrantRequest(_)
            | RequestWrapper::LeaseRevokeRequest(_)
            | RequestWrapper::LeaseLeasesRequest(_) => RequestBackend::Lease,
            RequestWrapper::AlarmRequest(_) => RequestBackend::Alarm,
        }
    }
}

/// Type of request. This is the extending of [`RequestBackend`].
#[allow(missing_docs)]
#[derive(Debug, PartialEq, Eq)]
pub enum RequestType {
    Put,
    Compaction,
    Txn,
    Range,
    Auth,
    Lease,
    Alarm,
}

impl From<&RequestWrapper> for RequestType {
    fn from(value: &RequestWrapper) -> Self {
        match *value {
            RequestWrapper::PutRequest(_) => RequestType::Put,
            RequestWrapper::RangeRequest(_) | RequestWrapper::DeleteRangeRequest(_) => {
                RequestType::Range
            }
            RequestWrapper::TxnRequest(_) => RequestType::Txn,
            RequestWrapper::CompactionRequest(_) => RequestType::Compaction,
            RequestWrapper::AuthEnableRequest(_)
            | RequestWrapper::AuthDisableRequest(_)
            | RequestWrapper::AuthStatusRequest(_)
            | RequestWrapper::AuthRoleAddRequest(_)
            | RequestWrapper::AuthRoleDeleteRequest(_)
            | RequestWrapper::AuthRoleGetRequest(_)
            | RequestWrapper::AuthRoleGrantPermissionRequest(_)
            | RequestWrapper::AuthRoleListRequest(_)
            | RequestWrapper::AuthRoleRevokePermissionRequest(_)
            | RequestWrapper::AuthUserAddRequest(_)
            | RequestWrapper::AuthUserChangePasswordRequest(_)
            | RequestWrapper::AuthUserDeleteRequest(_)
            | RequestWrapper::AuthUserGetRequest(_)
            | RequestWrapper::AuthUserGrantRoleRequest(_)
            | RequestWrapper::AuthUserListRequest(_)
            | RequestWrapper::AuthUserRevokeRoleRequest(_)
            | RequestWrapper::AuthenticateRequest(_) => RequestType::Auth,
            RequestWrapper::LeaseGrantRequest(_)
            | RequestWrapper::LeaseRevokeRequest(_)
            | RequestWrapper::LeaseLeasesRequest(_) => RequestType::Lease,
            RequestWrapper::AlarmRequest(_) => RequestType::Alarm,
        }
    }
}

/// indicates if the request is readonly or write
#[derive(Debug, PartialEq, Eq)]
pub enum RequestRw {
    /// Read only request
    Read,
    /// Write request.
    ///
    /// NOTE: A `TxnRequest` or a `DeleteRangeRequest` might be read-only, but we
    /// assume they will mutate the state machine to simplify the implementation.
    Write,
}

impl From<&RequestWrapper> for RequestRw {
    fn from(value: &RequestWrapper) -> Self {
        match *value {
            RequestWrapper::RangeRequest(_)
            | RequestWrapper::AuthStatusRequest(_)
            | RequestWrapper::AuthRoleGetRequest(_)
            | RequestWrapper::AuthRoleListRequest(_)
            | RequestWrapper::AuthUserGetRequest(_)
            | RequestWrapper::AuthUserListRequest(_)
            | RequestWrapper::LeaseLeasesRequest(_) => Self::Read,

            RequestWrapper::PutRequest(_)
            | RequestWrapper::DeleteRangeRequest(_)
            | RequestWrapper::TxnRequest(_)
            | RequestWrapper::CompactionRequest(_)
            | RequestWrapper::AuthEnableRequest(_)
            | RequestWrapper::AuthDisableRequest(_)
            | RequestWrapper::AuthRoleAddRequest(_)
            | RequestWrapper::AuthRoleDeleteRequest(_)
            | RequestWrapper::AuthRoleGrantPermissionRequest(_)
            | RequestWrapper::AuthRoleRevokePermissionRequest(_)
            | RequestWrapper::AuthUserAddRequest(_)
            | RequestWrapper::AuthUserChangePasswordRequest(_)
            | RequestWrapper::AuthUserDeleteRequest(_)
            | RequestWrapper::AuthUserGrantRoleRequest(_)
            | RequestWrapper::AuthUserRevokeRoleRequest(_)
            | RequestWrapper::AuthenticateRequest(_)
            | RequestWrapper::LeaseGrantRequest(_)
            | RequestWrapper::LeaseRevokeRequest(_)
            | RequestWrapper::AlarmRequest(_) => Self::Write,
        }
    }
}
