use etcd_client::{
    DeleteResponse as EtcdDeleteResponse, GetResponse as EtcdGetResponse,
    LeaseGrantResponse as EtcdLeaseGrantResponse,
    LeaseKeepAliveResponse as EtcdLeaseKeepAliveResponse,
    LeaseLeasesResponse as EtcdLeaseLeasesResponse, LeaseRevokeResponse as EtcdLeaseRevokeResponse,
    LeaseTimeToLiveResponse as EtcdLeaseTimeToLiveResponse, PutResponse as EtcdPutResponse,
};

use crate::{
    DeleteRangeResponse, KeyValue, LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse,
    LeaseRevokeResponse, LeaseStatus, LeaseTimeToLiveResponse, PutResponse, RangeResponse,
    ResponseHeader,
};

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

impl From<EtcdLeaseGrantResponse> for LeaseGrantResponse {
    fn from(res: EtcdLeaseGrantResponse) -> LeaseGrantResponse {
        let mut res = res;
        LeaseGrantResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            id: res.id(),
            ttl: res.ttl(),
            error: res.error().to_owned(),
        }
    }
}

impl From<EtcdLeaseRevokeResponse> for LeaseRevokeResponse {
    fn from(res: EtcdLeaseRevokeResponse) -> LeaseRevokeResponse {
        let mut res = res;
        LeaseRevokeResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
        }
    }
}

impl From<EtcdLeaseKeepAliveResponse> for LeaseKeepAliveResponse {
    fn from(res: EtcdLeaseKeepAliveResponse) -> LeaseKeepAliveResponse {
        let mut res = res;
        LeaseKeepAliveResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            id: res.id(),
            ttl: res.ttl(),
        }
    }
}

impl From<EtcdLeaseTimeToLiveResponse> for LeaseTimeToLiveResponse {
    fn from(res: EtcdLeaseTimeToLiveResponse) -> LeaseTimeToLiveResponse {
        let mut res = res;
        LeaseTimeToLiveResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            id: res.id(),
            ttl: res.ttl(),
            granted_ttl: res.granted_ttl(),
            keys: res.keys().to_vec(),
        }
    }
}

impl From<EtcdLeaseLeasesResponse> for LeaseLeasesResponse {
    fn from(res: EtcdLeaseLeasesResponse) -> LeaseLeasesResponse {
        let mut res = res;
        LeaseLeasesResponse {
            header: res.take_header().map(|h| ResponseHeader {
                cluster_id: h.cluster_id(),
                member_id: h.member_id(),
                revision: h.revision(),
                raft_term: h.raft_term(),
            }),
            leases: res
                .leases()
                .iter()
                .map(|l| LeaseStatus { id: l.id() })
                .collect(),
        }
    }
}
