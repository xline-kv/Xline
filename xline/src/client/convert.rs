use etcd_client::{
    DeleteOptions, GetOptions, LeaseGrantOptions, LeaseTimeToLiveOptions, PutOptions, SortOrder,
    SortTarget,
};

use super::kv_types::{
    DeleteRangeRequest, LeaseGrantRequest, LeaseTimeToLiveRequest, PutRequest, RangeRequest,
};

impl From<&PutRequest> for PutOptions {
    #[inline]
    fn from(req: &PutRequest) -> Self {
        let mut opts = PutOptions::new().with_lease(req.lease());
        if req.prev_kv() {
            opts = opts.with_prev_key();
        }
        if req.ignore_value() {
            opts = opts.with_ignore_value();
        }
        if req.ignore_lease() {
            opts = opts.with_ignore_lease();
        }
        opts
    }
}

impl From<&RangeRequest> for GetOptions {
    #[inline]
    #[allow(clippy::unwrap_used)] // those unwrap are safe
    fn from(req: &RangeRequest) -> Self {
        let mut opts = GetOptions::new()
            .with_range(req.range_end())
            .with_limit(req.limit())
            .with_revision(req.revision())
            .with_sort(
                SortTarget::from_i32(req.sort_target()).unwrap(),
                SortOrder::from_i32(req.sort_order()).unwrap(),
            )
            .with_min_create_revision(req.min_create_revision())
            .with_max_create_revision(req.max_create_revision())
            .with_min_mod_revision(req.min_mod_revision())
            .with_max_mod_revision(req.max_mod_revision());

        if req.serializable() {
            opts = opts.with_serializable();
        }
        if req.keys_only() {
            opts = opts.with_keys_only();
        }
        if req.count_only() {
            opts = opts.with_count_only();
        }
        opts
    }
}

impl From<&DeleteRangeRequest> for DeleteOptions {
    #[inline]
    fn from(req: &DeleteRangeRequest) -> Self {
        let mut opts = DeleteOptions::new().with_range(req.range_end());
        if req.prev_kv() {
            opts = opts.with_prev_key();
        }
        opts
    }
}

impl From<&LeaseGrantRequest> for LeaseGrantOptions {
    #[inline]
    fn from(req: &LeaseGrantRequest) -> Self {
        let mut opts = LeaseGrantOptions::new();
        if req.id() != 0 {
            opts = opts.with_id(req.id());
        }
        opts
    }
}

impl From<&LeaseTimeToLiveRequest> for LeaseTimeToLiveOptions {
    #[inline]
    fn from(req: &LeaseTimeToLiveRequest) -> Self {
        let mut opts = LeaseTimeToLiveOptions::new();
        if req.keys() {
            opts = opts.with_keys();
        }
        opts
    }
}
