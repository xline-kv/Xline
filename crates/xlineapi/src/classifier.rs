use super::RequestWrapper;

pub trait RequestClassifier {
    fn is_kv_backend(&self) -> bool;
    fn is_lease_backend(&self) -> bool;
    fn is_auth_backend(&self) -> bool;
    fn is_alarm_backend(&self) -> bool;
    fn is_put(&self) -> bool;
    fn is_compaction(&self) -> bool;
    fn is_txn(&self) -> bool;
    fn is_range(&self) -> bool;
    fn is_read_only(&self) -> bool;
    fn is_write(&self) -> bool;
}

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

impl RequestClassifier for RequestWrapper {
    #[inline]
    fn is_kv_backend(&self) -> bool {
        matches!(
            *self,
            RequestWrapper::PutRequest(_)
                | RequestWrapper::RangeRequest(_)
                | RequestWrapper::DeleteRangeRequest(_)
                | RequestWrapper::TxnRequest(_)
                | RequestWrapper::CompactionRequest(_)
        )
    }
    #[inline]
    fn is_auth_backend(&self) -> bool {
        matches!(
            self,
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
                | RequestWrapper::AuthenticateRequest(_)
        )
    }
    #[inline]
    fn is_lease_backend(&self) -> bool {
        matches!(
            self,
            RequestWrapper::LeaseGrantRequest(_)
                | RequestWrapper::LeaseRevokeRequest(_)
                | RequestWrapper::LeaseLeasesRequest(_)
        )
    }
    #[inline]
    fn is_alarm_backend(&self) -> bool {
        matches!(self, RequestWrapper::AlarmRequest(_))
    }
    #[inline]
    fn is_put(&self) -> bool {
        matches!(self, RequestWrapper::PutRequest(_))
    }
    #[inline]
    fn is_range(&self) -> bool {
        matches!(
            self,
            RequestWrapper::RangeRequest(_) | RequestWrapper::DeleteRangeRequest(_)
        )
    }
    #[inline]
    fn is_txn(&self) -> bool {
        matches!(self, RequestWrapper::TxnRequest(_))
    }
    #[inline]
    fn is_compaction(&self) -> bool {
        matches!(self, RequestWrapper::CompactionRequest(_))
    }

    #[inline]
    /// Read only request
    fn is_read_only(&self) -> bool {
        matches!(
            self,
            RequestWrapper::RangeRequest(_)
                | RequestWrapper::AuthStatusRequest(_)
                | RequestWrapper::AuthRoleGetRequest(_)
                | RequestWrapper::AuthRoleListRequest(_)
                | RequestWrapper::AuthUserGetRequest(_)
                | RequestWrapper::AuthUserListRequest(_)
                | RequestWrapper::LeaseLeasesRequest(_)
        )
    }

    #[inline]
    /// Write request.
    ///
    /// NOTE: A `TxnRequest` or a `DeleteRangeRequest` might be read-only, but we
    /// assume they will mutate the state machine to simplify the implementation.
    fn is_write(&self) -> bool {
        !self.is_read_only()
    }
}
