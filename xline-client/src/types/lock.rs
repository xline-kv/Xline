pub use xlineapi::{LockResponse, UnlockResponse};

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
    pub fn new(name: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::LockRequest {
                name: name.into(),
                lease: 0,
            },
            ttl: DEFAULT_SESSION_TTL,
        }
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
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::UnlockRequest { key: key.into() },
        }
    }
}

impl From<UnlockRequest> for xlineapi::UnlockRequest {
    #[inline]
    fn from(req: UnlockRequest) -> Self {
        req.inner
    }
}
