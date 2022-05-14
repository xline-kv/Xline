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

pub(crate) use self::etcdserverpb::{
    kv_server::{Kv, KvServer},
    request_op::Request,
    response_op::Response,
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, RequestOp, ResponseOp, TxnRequest, TxnResponse,
};
pub(crate) use self::mvccpb::KeyValue;
