pub use xlineapi::{
    Cluster, Member, MemberAddResponse, MemberListResponse, MemberPromoteResponse,
    MemberRemoveResponse, MemberUpdateResponse,
};

/// Request for `MemberAdd`
#[derive(Debug, PartialEq)]
pub struct MemberAddRequest {
    /// The inner request
    inner: xlineapi::MemberAddRequest,
}

impl MemberAddRequest {
    /// Creates a new `MemberAddRequest`
    #[inline]
    pub fn new(peer_ur_ls: impl Into<Vec<String>>, is_learner: bool) -> Self {
        Self {
            inner: xlineapi::MemberAddRequest {
                peer_ur_ls: peer_ur_ls.into(),
                is_learner,
            },
        }
    }
}

impl From<MemberAddRequest> for xlineapi::MemberAddRequest {
    #[inline]
    fn from(req: MemberAddRequest) -> Self {
        req.inner
    }
}

/// Request for `MemberList`
#[derive(Debug, PartialEq)]
pub struct MemberListRequest {
    /// The inner request
    inner: xlineapi::MemberListRequest,
}

impl MemberListRequest {
    /// Creates a new `MemberListRequest`
    #[inline]
    #[must_use]
    pub fn new(linearizable: bool) -> Self {
        Self {
            inner: xlineapi::MemberListRequest { linearizable },
        }
    }
}

impl From<MemberListRequest> for xlineapi::MemberListRequest {
    #[inline]
    fn from(req: MemberListRequest) -> Self {
        req.inner
    }
}

/// Request for `MemberPromote`
#[derive(Debug, PartialEq)]
pub struct MemberPromoteRequest {
    /// The inner request
    inner: xlineapi::MemberPromoteRequest,
}

impl MemberPromoteRequest {
    /// Creates a new `MemberPromoteRequest`
    #[inline]
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self {
            inner: xlineapi::MemberPromoteRequest { id },
        }
    }
}

impl From<MemberPromoteRequest> for xlineapi::MemberPromoteRequest {
    #[inline]
    fn from(req: MemberPromoteRequest) -> Self {
        req.inner
    }
}

/// Request for `MemberRemove`
#[derive(Debug, PartialEq)]
pub struct MemberRemoveRequest {
    /// The inner request
    inner: xlineapi::MemberRemoveRequest,
}

impl MemberRemoveRequest {
    /// Creates a new `MemberRemoveRequest`
    #[inline]
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self {
            inner: xlineapi::MemberRemoveRequest { id },
        }
    }
}

impl From<MemberRemoveRequest> for xlineapi::MemberRemoveRequest {
    #[inline]
    fn from(req: MemberRemoveRequest) -> Self {
        req.inner
    }
}

/// Request for `MemberUpdate`
#[derive(Debug, PartialEq)]
pub struct MemberUpdateRequest {
    /// The inner request
    inner: xlineapi::MemberUpdateRequest,
}

impl MemberUpdateRequest {
    /// Creates a new `MemberUpdateRequest`
    #[inline]
    pub fn new(id: u64, peer_ur_ls: impl Into<Vec<String>>) -> Self {
        Self {
            inner: xlineapi::MemberUpdateRequest {
                id,
                peer_ur_ls: peer_ur_ls.into(),
            },
        }
    }
}

impl From<MemberUpdateRequest> for xlineapi::MemberUpdateRequest {
    #[inline]
    fn from(req: MemberUpdateRequest) -> Self {
        req.inner
    }
}
