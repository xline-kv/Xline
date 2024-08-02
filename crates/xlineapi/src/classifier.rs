use super::RequestWrapper;

/// because RequestWrapper do not repr u8, we need to convert it manually.
impl From<&RequestWrapper> for u8 {
    fn from(value: &RequestWrapper) -> Self {
        match *value {
            RequestWrapper::PutRequest(_) => 0,
            RequestWrapper::RangeRequest(_) => 1,
            RequestWrapper::DeleteRangeRequest(_) => 2,
            RequestWrapper::TxnRequest(_) => 3,
            RequestWrapper::CompactionRequest(_) => 4,
            RequestWrapper::AuthEnableRequest(_) => 5,
            RequestWrapper::AuthDisableRequest(_) => 6,
            RequestWrapper::AuthStatusRequest(_) => 7,
            RequestWrapper::AuthRoleAddRequest(_) => 8,
            RequestWrapper::AuthRoleDeleteRequest(_) => 9,
            RequestWrapper::AuthRoleGetRequest(_) => 10,
            RequestWrapper::AuthRoleGrantPermissionRequest(_) => 11,
            RequestWrapper::AuthRoleListRequest(_) => 12,
            RequestWrapper::AuthRoleRevokePermissionRequest(_) => 13,
            RequestWrapper::AuthUserAddRequest(_) => 14,
            RequestWrapper::AuthUserChangePasswordRequest(_) => 15,
            RequestWrapper::AuthUserDeleteRequest(_) => 16,
            RequestWrapper::AuthUserGetRequest(_) => 17,
            RequestWrapper::AuthUserGrantRoleRequest(_) => 18,
            RequestWrapper::AuthUserListRequest(_) => 19,
            RequestWrapper::AuthUserRevokeRoleRequest(_) => 20,
            RequestWrapper::AuthenticateRequest(_) => 21,
            RequestWrapper::LeaseGrantRequest(_) => 22,
            RequestWrapper::LeaseRevokeRequest(_) => 23,
            RequestWrapper::LeaseLeasesRequest(_) => 24,
            RequestWrapper::AlarmRequest(_) => 25,
        }
    }
}

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
