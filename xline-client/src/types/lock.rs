/// Default session ttl
const DEFAULT_SESSION_TTL: i64 = 60;

/// Request for `Lock`
#[derive(Debug, PartialEq)]
pub struct LockRequest {
    /// The inner request
    pub(crate) inner: xlineapi::LockRequest,
    /// The ttl of the lease that attached to the lock
    pub(crate) ttl: i64,
}

impl LockRequest {
    /// Creates a new `LockRequest`
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        LockRequest::default()
    }

    /// Set name.
    #[inline]
    #[must_use]
    pub fn with_name(mut self, name: impl Into<Vec<u8>>) -> Self {
        self.inner.name = name.into();
        self
    }

    /// Set lease.
    #[inline]
    #[must_use]
    pub const fn with_lease(mut self, lease: i64) -> Self {
        self.inner.lease = lease;
        self
    }

    /// Set session TTL.
    /// Will be ignored when lease id is set
    #[inline]
    #[must_use]
    pub const fn with_ttl(mut self, ttl: i64) -> Self {
        self.ttl = ttl;
        self
    }
}

impl Default for LockRequest {
    #[inline]
    fn default() -> Self {
        Self {
            inner: xlineapi::LockRequest {
                name: Vec::new(),
                lease: 0,
            },
            ttl: DEFAULT_SESSION_TTL,
        }
    }
}

impl From<LockRequest> for xlineapi::LockRequest {
    #[inline]
    fn from(req: LockRequest) -> Self {
        req.inner
    }
}

/// Request for `Unlock`
#[derive(Debug)]
pub struct UnlockRequest {
    /// The inner request
    pub(crate) inner: xlineapi::UnlockRequest,
}

impl UnlockRequest {
    /// Creates a new `UnlockRequest`
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: xlineapi::UnlockRequest { key: Vec::new() },
        }
    }

    /// Set key.
    #[inline]
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.inner.key = key.into();
        self
    }
}

impl From<UnlockRequest> for xlineapi::UnlockRequest {
    #[inline]
    fn from(req: UnlockRequest) -> Self {
        req.inner
    }
}
