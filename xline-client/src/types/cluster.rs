pub use xlineapi::{
    Cluster, Member, MemberAddResponse, MemberListResponse, MemberPromoteResponse,
    MemberRemoveResponse, MemberUpdateResponse,
};

/// Request for `MemberAdd`
#[derive(Debug)]
pub struct MemberAddRequest {
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
#[derive(Debug)]
pub struct MemberListRequest {
    inner: xlineapi::MemberListRequest,
}

impl MemberListRequest {
    /// Creates a new `MemberListRequest`
    #[inline]
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
#[derive(Debug)]
pub struct MemberPromoteRequest {
    inner: xlineapi::MemberPromoteRequest,
}

impl MemberPromoteRequest {
    /// Creates a new `MemberPromoteRequest`
    #[inline]
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
#[derive(Debug)]
pub struct MemberRemoveRequest {
    inner: xlineapi::MemberRemoveRequest,
}

impl MemberRemoveRequest {
    /// Creates a new `MemberRemoveRequest`
    #[inline]
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
#[derive(Debug)]
pub struct MemberUpdateRequest {
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
