use xlineapi::command::KeyRange;
pub use xlineapi::{
    AuthDisableResponse, AuthEnableResponse, AuthRoleAddResponse, AuthRoleDeleteResponse,
    AuthRoleGetResponse, AuthRoleGrantPermissionResponse, AuthRoleListResponse,
    AuthRoleRevokePermissionResponse, AuthStatusResponse, AuthUserAddResponse,
    AuthUserChangePasswordResponse, AuthUserDeleteResponse, AuthUserGetResponse,
    AuthUserGrantRoleResponse, AuthUserListResponse, AuthUserRevokeRoleResponse,
    AuthenticateResponse, Type as PermissionType,
};

/// Request for `AuthRoleAdd`
#[derive(Debug, PartialEq)]
pub struct AuthRoleAddRequest {
    /// Inner request
    pub(crate) inner: xlineapi::AuthRoleAddRequest,
}

impl AuthRoleAddRequest {
    /// Creates a new `AuthRoleAddRequest`
    ///
    /// `role` is the name of the role to add.
    #[inline]
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthRoleAddRequest { name: role.into() },
        }
    }
}

impl From<AuthRoleAddRequest> for xlineapi::AuthRoleAddRequest {
    #[inline]
    fn from(req: AuthRoleAddRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleDelete`
#[derive(Debug, PartialEq)]
pub struct AuthRoleDeleteRequest {
    /// Inner request
    pub(crate) inner: xlineapi::AuthRoleDeleteRequest,
}

impl AuthRoleDeleteRequest {
    /// Creates a new `AuthRoleDeleteRequest`
    ///
    /// `role` is the name of the role to delete.
    #[inline]
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthRoleDeleteRequest { role: role.into() },
        }
    }
}

impl From<AuthRoleDeleteRequest> for xlineapi::AuthRoleDeleteRequest {
    #[inline]
    fn from(req: AuthRoleDeleteRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleGrantPermission`
#[derive(Debug, PartialEq)]
pub struct AuthRoleGrantPermissionRequest {
    /// Inner request
    pub(crate) inner: xlineapi::AuthRoleGrantPermissionRequest,
}

impl AuthRoleGrantPermissionRequest {
    /// Creates a new `AuthRoleGrantPermissionRequest`
    ///
    /// `role` is the name of the role to grant permission,
    /// `perm` is the permission name to grant.
    #[inline]
    pub fn new(role: impl Into<String>, perm: Permission) -> Self {
        Self {
            inner: xlineapi::AuthRoleGrantPermissionRequest {
                name: role.into(),
                perm: Some(perm.into()),
            },
        }
    }
}

impl From<AuthRoleGrantPermissionRequest> for xlineapi::AuthRoleGrantPermissionRequest {
    #[inline]
    fn from(req: AuthRoleGrantPermissionRequest) -> Self {
        req.inner
    }
}

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
}

impl Permission {
    /// Creates a permission with operation type and key
    ///
    /// `perm_type` is the permission type,
    /// `key` is the key to grant with the permission.
    #[inline]
    #[must_use]
    pub fn new(perm_type: PermissionType, key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::Permission {
                perm_type: perm_type.into(),
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

impl From<Permission> for xlineapi::Permission {
    #[inline]
    fn from(perm: Permission) -> Self {
        perm.inner
    }
}
