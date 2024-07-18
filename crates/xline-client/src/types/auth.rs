pub use xlineapi::{
    AuthDisableResponse, AuthEnableResponse, AuthRoleAddResponse, AuthRoleDeleteResponse,
    AuthRoleGetResponse, AuthRoleGrantPermissionResponse, AuthRoleListResponse,
    AuthRoleRevokePermissionResponse, AuthStatusResponse, AuthUserAddResponse,
    AuthUserChangePasswordResponse, AuthUserDeleteResponse, AuthUserGetResponse,
    AuthUserGrantRoleResponse, AuthUserListResponse, AuthUserRevokeRoleResponse,
    AuthenticateResponse, Type as PermissionType,
};

use super::range_end::RangeOption;

/// Role access permission.
#[derive(Debug, Clone)]
pub struct Permission {
    /// The inner Permission
    inner: xlineapi::Permission,
    /// The range option
    range_option: Option<RangeOption>,
}

impl Permission {
    /// Creates a permission with operation type and key
    ///
    /// `perm_type` is the permission type,
    /// `key` is the key to grant with the permission.
    /// `range_option` is the range option of how to get `range_end` from key.
    #[inline]
    #[must_use]
    pub fn new(
        perm_type: PermissionType,
        key: impl Into<Vec<u8>>,
        range_option: Option<RangeOption>,
    ) -> Self {
        Self::from((perm_type, key.into(), range_option))
    }
}

impl From<Permission> for xlineapi::Permission {
    #[inline]
    fn from(mut perm: Permission) -> Self {
        perm.inner.range_end = perm
            .range_option
            .unwrap_or_default()
            .get_range_end(&mut perm.inner.key);
        perm.inner
    }
}

impl PartialEq for Permission {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner && self.range_option == other.range_option
    }
}

impl Eq for Permission {}

impl From<(PermissionType, Vec<u8>, Option<RangeOption>)> for Permission {
    #[inline]
    fn from(
        (perm_type, key, range_option): (PermissionType, Vec<u8>, Option<RangeOption>),
    ) -> Self {
        Permission {
            inner: xlineapi::Permission {
                perm_type: perm_type.into(),
                key,
                ..Default::default()
            },
            range_option,
        }
    }
}

impl From<(PermissionType, &str, Option<RangeOption>)> for Permission {
    #[inline]
    fn from(value: (PermissionType, &str, Option<RangeOption>)) -> Self {
        Self::from((value.0, value.1.as_bytes().to_vec(), value.2))
    }
}
