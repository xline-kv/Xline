pub use crate::rpc::{SortOrder, SortTarget};
use crate::server::command::KeyRange;

/// Request for `Put`
#[derive(Debug)]
pub struct PutRequest {
    /// inner request
    inner: crate::rpc::PutRequest,
}

impl PutRequest {
    /// New `PutRequest`
    #[inline]
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: crate::rpc::PutRequest {
                key: key.into(),
                value: value.into(),
                ..Default::default()
            },
        }
    }

    /// Set `lease`
    #[inline]
    #[must_use]
    pub fn with_lease(mut self, lease: i64) -> Self {
        self.inner.lease = lease;
        self
    }

    /// Set `prev_kv`
    #[inline]
    #[must_use]
    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.inner.prev_kv = prev_kv;
        self
    }

    /// Set `ignore_value`
    #[inline]
    #[must_use]
    pub fn with_ignore_value(mut self, ignore_value: bool) -> Self {
        self.inner.ignore_value = ignore_value;
        self
    }

    /// Set `ignore_lease`
    #[inline]
    #[must_use]
    pub fn with_ignore_lease(mut self, ignore_lease: bool) -> Self {
        self.inner.ignore_lease = ignore_lease;
        self
    }

    /// Get `key`
    #[inline]
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }

    /// Get `value`
    #[inline]
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.inner.value
    }

    /// Get `lease`
    #[inline]
    #[must_use]
    pub fn lease(&self) -> i64 {
        self.inner.lease
    }

    /// Get `prev_kv`
    #[inline]
    #[must_use]
    pub fn prev_kv(&self) -> bool {
        self.inner.prev_kv
    }

    /// Get `ignore_value`
    #[inline]
    #[must_use]
    pub fn ignore_value(&self) -> bool {
        self.inner.ignore_value
    }

    /// Get `ignore_lease`
    #[inline]
    #[must_use]
    pub fn ignore_lease(&self) -> bool {
        self.inner.ignore_lease
    }
}

impl From<PutRequest> for crate::rpc::PutRequest {
    #[inline]
    fn from(req: PutRequest) -> Self {
        req.inner
    }
}

/// Request for `Range`
#[derive(Debug)]
pub struct RangeRequest {
    /// Inner request
    inner: crate::rpc::RangeRequest,
}

impl RangeRequest {
    /// New `RangeRequest`
    #[inline]
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: crate::rpc::RangeRequest {
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

    /// Set `limit`
    #[inline]
    #[must_use]
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.inner.limit = limit;
        self
    }

    /// Set `revision`
    #[inline]
    #[must_use]
    pub fn with_revision(mut self, revision: i64) -> Self {
        self.inner.revision = revision;
        self
    }

    /// Set `sort_order`
    #[inline]
    #[must_use]
    #[allow(clippy::as_conversions)] // this case is always safe
    pub fn with_sort_order(mut self, sort_order: SortOrder) -> Self {
        self.inner.sort_order = sort_order as i32;
        self
    }

    /// Set `sort_target`
    #[inline]
    #[must_use]
    #[allow(clippy::as_conversions)] // this case is always safe
    pub fn with_sort_target(mut self, sort_target: SortTarget) -> Self {
        self.inner.sort_target = sort_target as i32;
        self
    }

    /// Set `serializable`
    #[inline]
    #[must_use]
    pub fn with_serializable(mut self, serializable: bool) -> Self {
        self.inner.serializable = serializable;
        self
    }

    /// Set `keys_only`
    #[inline]
    #[must_use]
    pub fn with_keys_only(mut self, keys_only: bool) -> Self {
        self.inner.keys_only = keys_only;
        self
    }

    /// Set `count_only`
    #[inline]
    #[must_use]
    pub fn with_count_only(mut self, count_only: bool) -> Self {
        self.inner.count_only = count_only;
        self
    }

    /// Set `min_mod_revision`
    #[inline]
    #[must_use]
    pub fn with_min_mod_revision(mut self, min_mod_revision: i64) -> Self {
        self.inner.min_mod_revision = min_mod_revision;
        self
    }

    /// Set `max_mod_revision`
    #[inline]
    #[must_use]
    pub fn with_max_mod_revision(mut self, max_mod_revision: i64) -> Self {
        self.inner.max_mod_revision = max_mod_revision;
        self
    }

    /// Set `min_create_revision`
    #[inline]
    #[must_use]
    pub fn with_min_create_revision(mut self, min_create_revision: i64) -> Self {
        self.inner.min_create_revision = min_create_revision;
        self
    }

    /// Set `max_create_revision`
    #[inline]
    #[must_use]
    pub fn with_max_create_revision(mut self, max_create_revision: i64) -> Self {
        self.inner.max_create_revision = max_create_revision;
        self
    }

    /// Get `key`
    #[inline]
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }

    /// Get `range_end`
    #[inline]
    #[must_use]
    pub fn range_end(&self) -> &[u8] {
        &self.inner.range_end
    }

    /// Get `limit`
    #[inline]
    #[must_use]
    pub fn limit(&self) -> i64 {
        self.inner.limit
    }

    /// Get `revision`
    #[inline]
    #[must_use]
    pub fn revision(&self) -> i64 {
        self.inner.revision
    }

    /// Get `sort_order`
    #[inline]
    #[must_use]
    pub fn sort_order(&self) -> i32 {
        self.inner.sort_order
    }

    /// Get `sort_target`
    #[inline]
    #[must_use]
    pub fn sort_target(&self) -> i32 {
        self.inner.sort_target
    }

    /// Get `serializable`
    #[inline]
    #[must_use]
    pub fn serializable(&self) -> bool {
        self.inner.serializable
    }

    /// Get `keys_only`
    #[inline]
    #[must_use]
    pub fn keys_only(&self) -> bool {
        self.inner.keys_only
    }

    /// Get `count_only`
    #[inline]
    #[must_use]
    pub fn count_only(&self) -> bool {
        self.inner.count_only
    }

    /// Get `min_mod_revision`
    #[inline]
    #[must_use]
    pub fn min_mod_revision(&self) -> i64 {
        self.inner.min_mod_revision
    }

    /// Get `max_mod_revision`
    #[inline]
    #[must_use]
    pub fn max_mod_revision(&self) -> i64 {
        self.inner.max_mod_revision
    }

    /// Get `min_create_revision`
    #[inline]
    #[must_use]
    pub fn min_create_revision(&self) -> i64 {
        self.inner.min_create_revision
    }

    /// Get `max_create_revision`
    #[inline]
    #[must_use]
    pub fn max_create_revision(&self) -> i64 {
        self.inner.max_create_revision
    }
}

impl From<RangeRequest> for crate::rpc::RangeRequest {
    #[inline]
    fn from(req: RangeRequest) -> Self {
        req.inner
    }
}

/// Request for `DeleteRange`
#[derive(Debug)]
pub struct DeleteRangeRequest {
    /// Inner request
    inner: crate::rpc::DeleteRangeRequest,
}

impl DeleteRangeRequest {
    /// New `DeleteRangeRequest`
    #[inline]
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: crate::rpc::DeleteRangeRequest {
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

    /// Set `prev_kv`
    #[inline]
    #[must_use]
    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.inner.prev_kv = prev_kv;
        self
    }

    /// Get `key`
    #[inline]
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }

    /// Get `range_end`
    #[inline]
    #[must_use]
    pub fn range_end(&self) -> &[u8] {
        &self.inner.range_end
    }

    /// Get `prev_kv`
    #[inline]
    #[must_use]
    pub fn prev_kv(&self) -> bool {
        self.inner.prev_kv
    }
}

impl From<DeleteRangeRequest> for crate::rpc::DeleteRangeRequest {
    #[inline]
    fn from(req: DeleteRangeRequest) -> Self {
        req.inner
    }
}

/// Request for `LeaseGrant`
#[derive(Debug)]
pub struct LeaseGrantRequest {
    /// Inner request
    inner: crate::rpc::LeaseGrantRequest,
}

impl LeaseGrantRequest {
    /// New `LeaseGrantRequest`
    #[inline]
    #[must_use]
    pub fn new(ttl: i64) -> Self {
        Self {
            inner: crate::rpc::LeaseGrantRequest {
                ttl,
                ..Default::default()
            },
        }
    }

    /// Set `id`
    #[inline]
    #[must_use]
    pub fn with_id(mut self, id: i64) -> Self {
        self.inner.id = id;
        self
    }

    /// Get `id`
    #[inline]
    #[must_use]
    pub fn id(&self) -> i64 {
        self.inner.id
    }

    /// Get `ttl`
    #[inline]
    #[must_use]
    pub fn ttl(&self) -> i64 {
        self.inner.ttl
    }
}

impl From<LeaseGrantRequest> for crate::rpc::LeaseGrantRequest {
    #[inline]
    fn from(req: LeaseGrantRequest) -> Self {
        req.inner
    }
}

/// Request for `LeaseRevoke`
#[derive(Debug)]
pub struct LeaseRevokeRequest {
    /// Inner request
    inner: crate::rpc::LeaseRevokeRequest,
}

impl LeaseRevokeRequest {
    /// New `LeaseRevokeRequest`
    #[inline]
    #[must_use]
    pub fn new(id: i64) -> Self {
        Self {
            inner: crate::rpc::LeaseRevokeRequest { id },
        }
    }

    /// Get `id`
    #[inline]
    #[must_use]
    pub fn id(&self) -> i64 {
        self.inner.id
    }
}

impl From<LeaseRevokeRequest> for crate::rpc::LeaseRevokeRequest {
    #[inline]
    fn from(req: LeaseRevokeRequest) -> Self {
        req.inner
    }
}

/// Request for `LeaseKeepAlive`
#[derive(Debug)]
pub struct LeaseKeepAliveRequest {
    /// Inner request
    inner: crate::rpc::LeaseKeepAliveRequest,
}

impl LeaseKeepAliveRequest {
    /// New `LeaseKeepAliveRequest`
    #[inline]
    #[must_use]
    pub fn new(id: i64) -> Self {
        Self {
            inner: crate::rpc::LeaseKeepAliveRequest { id },
        }
    }

    /// Get `id`
    #[inline]
    #[must_use]
    pub fn id(&self) -> i64 {
        self.inner.id
    }
}

impl From<LeaseKeepAliveRequest> for crate::rpc::LeaseKeepAliveRequest {
    #[inline]
    fn from(req: LeaseKeepAliveRequest) -> Self {
        req.inner
    }
}

/// Request for `LeaseTimeToLive`
#[derive(Debug)]
pub struct LeaseTimeToLiveRequest {
    /// Inner request
    inner: crate::rpc::LeaseTimeToLiveRequest,
}

impl LeaseTimeToLiveRequest {
    /// New `LeaseTimeToLiveRequest`
    #[inline]
    #[must_use]
    pub fn new(id: i64) -> Self {
        Self {
            inner: crate::rpc::LeaseTimeToLiveRequest {
                id,
                ..Default::default()
            },
        }
    }

    /// Set `keys`
    #[inline]
    #[must_use]
    pub fn with_keys(mut self, keys: bool) -> Self {
        self.inner.keys = keys;
        self
    }

    /// Get `id`
    #[inline]
    #[must_use]
    pub fn id(&self) -> i64 {
        self.inner.id
    }

    /// Get `keys`
    #[inline]
    #[must_use]
    pub fn keys(&self) -> bool {
        self.inner.keys
    }
}

impl From<LeaseTimeToLiveRequest> for crate::rpc::LeaseTimeToLiveRequest {
    #[inline]
    fn from(req: LeaseTimeToLiveRequest) -> Self {
        req.inner
    }
}
