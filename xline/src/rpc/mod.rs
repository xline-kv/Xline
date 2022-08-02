// Skip for generated code
#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences
)]
mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}
#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences
)]
mod authpb {
    tonic::include_proto!("authpb");
}
#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences
)]
mod mvccpb {
    tonic::include_proto!("mvccpb");
}

#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences
)]
mod v3lockpb {
    tonic::include_proto!("v3lockpb");
}

#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences
)]
mod leasepb {
    tonic::include_proto!("leasepb");
}

pub(crate) use self::etcdserverpb::{
    compare::{CompareResult, CompareTarget, TargetUnion},
    kv_server::{Kv, KvServer},
    lease_server::{Lease, LeaseServer},
    range_request::{SortOrder, SortTarget},
    request_op::Request,
    response_op::Response,
    CompactionRequest, CompactionResponse, Compare, DeleteRangeRequest, DeleteRangeResponse,
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
    LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse, RequestOp, ResponseHeader, ResponseOp, TxnRequest, TxnResponse,
};
pub(crate) use self::mvccpb::KeyValue;
pub(crate) use self::v3lockpb::{
    lock_server::{Lock, LockServer},
    LockRequest, LockResponse, UnlockRequest, UnlockResponse,
};
