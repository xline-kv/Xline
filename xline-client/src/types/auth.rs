use xline::server::KeyRange;
use xlineapi::Type as PermissionType;

/// Request for `Authenticate`
#[derive(Debug)]
pub struct AuthenticateRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthenticateRequest,
}

impl AuthenticateRequest {
    /// New `AuthenticateRequest`
    #[inline]
    pub fn new(name: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthenticateRequest {
                name: name.into(),
                password: password.into(),
            },
        }
    }
}

impl From<AuthenticateRequest> for xlineapi::AuthenticateRequest {
    #[inline]
    fn from(req: AuthenticateRequest) -> Self {
        req.inner
    }
}

/// Request for `Authenticate`
#[derive(Debug)]
pub struct AuthUserAddRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthUserAddRequest,
}

impl AuthUserAddRequest {
    /// New `AuthUserAddRequest`
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserAddRequest {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    /// Set password.
    #[inline]
    #[must_use]
    pub fn with_pwd(mut self, password: impl Into<String>) -> Self {
        self.inner.password = password.into();
        self
    }

    /// Set no password.
    #[inline]
    #[must_use]
    pub const fn with_no_pwd(mut self) -> Self {
        self.inner.options = Some(xlineapi::UserAddOptions { no_password: true });
        self
    }
}

impl From<AuthUserAddRequest> for xlineapi::AuthUserAddRequest {
    #[inline]
    fn from(req: AuthUserAddRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserGet`
#[derive(Debug)]
pub struct AuthUserGetRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthUserGetRequest,
}

impl AuthUserGetRequest {
    /// New `AuthUserGetRequest`
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserGetRequest { name: name.into() },
        }
    }
}

impl From<AuthUserGetRequest> for xlineapi::AuthUserGetRequest {
    #[inline]
    fn from(req: AuthUserGetRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserDelete`
#[derive(Debug)]
pub struct AuthUserDeleteRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthUserDeleteRequest,
}

impl AuthUserDeleteRequest {
    /// New `AuthUserDeleteRequest`
    #[inline]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserDeleteRequest { name: name.into() },
        }
    }
}

impl From<AuthUserDeleteRequest> for xlineapi::AuthUserDeleteRequest {
    #[inline]
    fn from(req: AuthUserDeleteRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserChangePassword`
#[derive(Debug)]
pub struct AuthUserChangePasswordRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthUserChangePasswordRequest,
}

impl AuthUserChangePasswordRequest {
    /// New `AuthUserChangePasswordRequest`
    #[inline]
    pub fn new(name: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserChangePasswordRequest {
                name: name.into(),
                password: password.into(),
                hashed_password: String::new(),
            },
        }
    }
}

impl From<AuthUserChangePasswordRequest> for xlineapi::AuthUserChangePasswordRequest {
    #[inline]
    fn from(req: AuthUserChangePasswordRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserGrantRole`
#[derive(Debug)]
pub struct AuthUserGrantRoleRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthUserGrantRoleRequest,
}

impl AuthUserGrantRoleRequest {
    /// New `AuthUserGrantRoleRequest`
    #[inline]
    pub fn new(name: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserGrantRoleRequest {
                user: name.into(),
                role: role.into(),
            },
        }
    }
}

impl From<AuthUserGrantRoleRequest> for xlineapi::AuthUserGrantRoleRequest {
    #[inline]
    fn from(req: AuthUserGrantRoleRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthUserRevokeRole`
#[derive(Debug)]
pub struct AuthUserRevokeRoleRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthUserRevokeRoleRequest,
}

impl AuthUserRevokeRoleRequest {
    /// New `AuthUserRevokeRoleRequest`
    #[inline]
    pub fn new(name: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthUserRevokeRoleRequest {
                name: name.into(),
                role: role.into(),
            },
        }
    }
}

impl From<AuthUserRevokeRoleRequest> for xlineapi::AuthUserRevokeRoleRequest {
    #[inline]
    fn from(req: AuthUserRevokeRoleRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleAdd`
#[derive(Debug)]
pub struct AuthRoleAddRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthRoleAddRequest,
}

impl AuthRoleAddRequest {
    /// New `AuthRoleAddRequest`
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

/// Request for `AuthRoleGet`
#[derive(Debug)]
pub struct AuthRoleGetRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthRoleGetRequest,
}

impl AuthRoleGetRequest {
    /// New `AuthRoleGetRequest`
    #[inline]
    pub fn new(role: impl Into<String>) -> Self {
        Self {
            inner: xlineapi::AuthRoleGetRequest { role: role.into() },
        }
    }
}

impl From<AuthRoleGetRequest> for xlineapi::AuthRoleGetRequest {
    #[inline]
    fn from(req: AuthRoleGetRequest) -> Self {
        req.inner
    }
}

/// Request for `AuthRoleDelete`
#[derive(Debug)]
pub struct AuthRoleDeleteRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthRoleDeleteRequest,
}

impl AuthRoleDeleteRequest {
    /// New `AuthRoleDeleteRequest`
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
#[derive(Debug)]
pub struct AuthRoleGrantPermissionRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthRoleGrantPermissionRequest,
}

impl AuthRoleGrantPermissionRequest {
    /// New `AuthRoleGrantPermissionRequest`
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
#[derive(Debug)]
pub struct AuthRoleRevokePermissionRequest {
    /// inner request
    pub(crate) inner: xlineapi::AuthRoleRevokePermissionRequest,
}

impl AuthRoleRevokePermissionRequest {
    /// Create a new `RoleRevokePermissionOption` from pb role revoke permission.
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

    /// Set `key` and `range_end` when with prefix
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

    /// Set `key` and `range_end` when with from key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// Set `range_end`
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
    /// Set `key` and `range_end` when with prefix
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

    /// Set `key` and `range_end` when with from key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// Set `range_end`
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
