use futures::channel::mpsc::Sender;
pub use xlineapi::{
    LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse, LeaseRevokeResponse,
    LeaseStatus, LeaseTimeToLiveResponse,
};

use crate::error::{Result, XlineClientError};

/// The lease keep alive handle.
#[derive(Debug)]
pub struct LeaseKeeper {
    /// lease id
    id: i64,
    /// sender to send keep alive request
    sender: Sender<xlineapi::LeaseKeepAliveRequest>,
}

impl LeaseKeeper {
    /// Creates a new `LeaseKeeper`.
    #[inline]
    #[must_use]
    pub fn new(id: i64, sender: Sender<xlineapi::LeaseKeepAliveRequest>) -> Self {
        Self { id, sender }
    }

    /// The lease id which user want to keep alive.
    #[inline]
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Sends a keep alive request and receive response
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner channel is closed
    #[inline]
    pub fn keep_alive(&mut self) -> Result<()> {
        self.sender
            .try_send(LeaseKeepAliveRequest::new(self.id).into())
            .map_err(|e| XlineClientError::LeaseError(e.to_string()))
    }
}

/// Request for `LeaseGrant`
#[derive(Debug, PartialEq)]
pub struct LeaseGrantRequest {
    /// Inner request
    pub(crate) inner: xlineapi::LeaseGrantRequest,
}

impl LeaseGrantRequest {
    /// Creates a new `LeaseGrantRequest`
    ///
    /// `ttl` is the advisory time-to-live in seconds. Expired lease will return -1.
    #[inline]
    #[must_use]
    pub fn new(ttl: i64) -> Self {
        Self {
            inner: xlineapi::LeaseGrantRequest {
                ttl,
                ..Default::default()
            },
        }
    }

    /// `id` is the requested ID for the lease. If ID is set to 0, the lessor chooses an ID.
    #[inline]
    #[must_use]
    pub fn with_id(mut self, id: i64) -> Self {
        self.inner.id = id;
        self
    }
}

impl From<LeaseGrantRequest> for xlineapi::LeaseGrantRequest {
    #[inline]
    fn from(req: LeaseGrantRequest) -> Self {
        req.inner
    }
}

/// Request for `LeaseRevoke`
#[derive(Debug, PartialEq)]
pub struct LeaseRevokeRequest {
    /// Inner request
    pub(crate) inner: xlineapi::LeaseRevokeRequest,
}

impl LeaseRevokeRequest {
    /// Creates a new `LeaseRevokeRequest`
    ///
    /// `id` is the lease ID to revoke. When the ID is revoked, all associated keys will be deleted.
    #[inline]
    #[must_use]
    pub fn new(id: i64) -> Self {
        Self {
            inner: xlineapi::LeaseRevokeRequest { id },
        }
    }
}

impl From<LeaseRevokeRequest> for xlineapi::LeaseRevokeRequest {
    #[inline]
    fn from(req: LeaseRevokeRequest) -> Self {
        req.inner
    }
}

/// Request for `LeaseKeepAlive`
#[derive(Debug, PartialEq)]
pub struct LeaseKeepAliveRequest {
    /// Inner request
    pub(crate) inner: xlineapi::LeaseKeepAliveRequest,
}

impl LeaseKeepAliveRequest {
    /// Creates a new `LeaseKeepAliveRequest`
    ///
    /// `id` is the lease ID for the lease to keep alive.
    #[inline]
    #[must_use]
    pub fn new(id: i64) -> Self {
        Self {
            inner: xlineapi::LeaseKeepAliveRequest { id },
        }
    }
}

impl From<LeaseKeepAliveRequest> for xlineapi::LeaseKeepAliveRequest {
    #[inline]
    fn from(req: LeaseKeepAliveRequest) -> Self {
        req.inner
    }
}

/// Request for `LeaseTimeToLive`
#[derive(Debug, PartialEq)]
pub struct LeaseTimeToLiveRequest {
    /// Inner request
    pub(crate) inner: xlineapi::LeaseTimeToLiveRequest,
}

impl LeaseTimeToLiveRequest {
    /// Creates a new `LeaseTimeToLiveRequest`
    ///
    /// `id` is the lease ID for the lease.
    #[inline]
    #[must_use]
    pub fn new(id: i64) -> Self {
        Self {
            inner: xlineapi::LeaseTimeToLiveRequest {
                id,
                ..Default::default()
            },
        }
    }

    /// `keys` is true to query all the keys attached to this lease.
    #[inline]
    #[must_use]
    pub fn with_keys(mut self, keys: bool) -> Self {
        self.inner.keys = keys;
        self
    }
}

impl From<LeaseTimeToLiveRequest> for xlineapi::LeaseTimeToLiveRequest {
    #[inline]
    fn from(req: LeaseTimeToLiveRequest) -> Self {
        req.inner
    }
}
