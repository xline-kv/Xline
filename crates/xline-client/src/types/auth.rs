use xlineapi::command::KeyRange;
pub use xlineapi::{
    AuthDisableResponse, AuthEnableResponse, AuthRoleAddResponse, AuthRoleDeleteResponse,
    AuthRoleGetResponse, AuthRoleGrantPermissionResponse, AuthRoleListResponse,
    AuthRoleRevokePermissionResponse, AuthStatusResponse, AuthUserAddResponse,
    AuthUserChangePasswordResponse, AuthUserDeleteResponse, AuthUserGetResponse,
    AuthUserGrantRoleResponse, AuthUserListResponse, AuthUserRevokeRoleResponse,
    AuthenticateResponse, Type as PermissionType,
};

use super::range_end::RangeOption;

/// Request for `AuthRoleRevokePermission`
#[derive(Debug, PartialEq)]
pub struct AuthRoleRevokePermissionRequest {
    /// Inner request
    pub(crate) inner: xlineapi::AuthRoleRevokePermissionRequest,
}

impl AuthRoleRevokePermissionRequest {
    /// Creates a new `RoleRevokePermissionOption` from pb role revoke permission.
    ///
    /// `role` is the name of the role to revoke permission,
    /// `key` is the key to revoke from the role.
    #[inline]
    pub fn new(role: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::AuthRoleRevokePermissionRequest {
                role: role.into(),
                key: key.into(),
                ..Default::default()
            },
        }
    }

    /// If set, Xline will return all keys with the matching prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
            self.inner.range_end = vec![0];
        } else {
            self.inner.range_end = KeyRange::get_prefix(&self.inner.key);
        }
        self
    }

    /// If set, Xline will return all keys that are equal or greater than the given key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// `range_end` is the upper bound on the requested range \[key,` range_en`d).
    /// If `range_end` is '\0', the range is all keys >= key.
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }
}

impl From<AuthRoleRevokePermissionRequest> for xlineapi::AuthRoleRevokePermissionRequest {
    #[inline]
    fn from(req: AuthRoleRevokePermissionRequest) -> Self {
        req.inner
    }
}

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
