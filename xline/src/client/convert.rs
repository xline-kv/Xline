use etcd_client::{
    DeleteOptions, DeleteResponse as EtcdDeleteResponse, GetOptions,
    GetResponse as EtcdGetResponse, PutOptions, PutResponse as EtcdPutResponse, SortOrder,
    SortTarget,
};

use super::kv_types::{DeleteRangeRequest, PutRequest, RangeRequest};
use crate::rpc::{DeleteRangeResponse, KeyValue, PutResponse, RangeResponse, ResponseHeader};

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

impl From<EtcdPutResponse> for PutResponse {
    fn from(res: EtcdPutResponse) -> PutResponse {
        let mut res = res;
        PutResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            prev_kv: res.take_prev_key().map(|kv| KeyValue {
                key: kv.key().to_vec(),
                create_revision: kv.create_revision(),
                mod_revision: kv.mod_revision(),
                version: kv.version(),
                value: kv.value().to_vec(),
                lease: kv.lease(),
            }),
        }
    }
}

impl From<EtcdGetResponse> for RangeResponse {
    fn from(res: EtcdGetResponse) -> RangeResponse {
        let mut res = res;
        RangeResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            kvs: res
                .kvs()
                .iter()
                .map(|kv| KeyValue {
                    key: kv.key().to_vec(),
                    create_revision: kv.create_revision(),
                    mod_revision: kv.mod_revision(),
                    version: kv.version(),
                    value: kv.value().to_vec(),
                    lease: kv.lease(),
                })
                .collect(),
            count: res.count(),
            more: res.more(),
        }
    }
}

impl From<EtcdDeleteResponse> for DeleteRangeResponse {
    fn from(res: EtcdDeleteResponse) -> DeleteRangeResponse {
        let mut res = res;
        DeleteRangeResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            deleted: res.deleted(),
            prev_kvs: res
                .prev_kvs()
                .iter()
                .map(|kv| KeyValue {
                    key: kv.key().to_vec(),
                    create_revision: kv.create_revision(),
                    mod_revision: kv.mod_revision(),
                    version: kv.version(),
                    value: kv.value().to_vec(),
                    lease: kv.lease(),
                })
                .collect(),
        }
    }
}
