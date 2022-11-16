// Skip for generated code
#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences,
    missing_copy_implementations,
    missing_docs
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

use serde::{Deserialize, Serialize};

pub(crate) use self::authpb::{permission::Type, Permission, Role, User};
pub(crate) use self::etcdserverpb::{
    auth_server::{Auth, AuthServer},
    compare::{CompareResult, CompareTarget, TargetUnion},
    kv_server::{Kv, KvServer},
    lease_server::{Lease, LeaseServer},
    request_op::Request,
    response_op::Response,
    watch_request::RequestUnion,
    watch_server::{Watch, WatchServer},
    AuthDisableRequest, AuthDisableResponse, AuthEnableRequest, AuthEnableResponse,
    AuthRoleAddRequest, AuthRoleAddResponse, AuthRoleDeleteRequest, AuthRoleDeleteResponse,
    AuthRoleGetRequest, AuthRoleGetResponse, AuthRoleGrantPermissionRequest,
    AuthRoleGrantPermissionResponse, AuthRoleListRequest, AuthRoleListResponse,
    AuthRoleRevokePermissionRequest, AuthRoleRevokePermissionResponse, AuthStatusRequest,
    AuthStatusResponse, AuthUserAddRequest, AuthUserAddResponse, AuthUserChangePasswordRequest,
    AuthUserChangePasswordResponse, AuthUserDeleteRequest, AuthUserDeleteResponse,
    AuthUserGetRequest, AuthUserGetResponse, AuthUserGrantRoleRequest, AuthUserGrantRoleResponse,
    AuthUserListRequest, AuthUserListResponse, AuthUserRevokeRoleRequest,
    AuthUserRevokeRoleResponse, AuthenticateRequest, AuthenticateResponse, CompactionRequest,
    CompactionResponse, Compare, DeleteRangeRequest, DeleteRangeResponse, LeaseGrantRequest,
    LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse, LeaseLeasesRequest,
    LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse, LeaseTimeToLiveRequest,
    LeaseTimeToLiveResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, RequestOp,
    ResponseHeader, ResponseOp, TxnRequest, TxnResponse, WatchCancelRequest, WatchCreateRequest,
    WatchRequest, WatchResponse,
};
pub(crate) use self::mvccpb::{event::EventType, Event, KeyValue};
pub(crate) use self::v3lockpb::{
    lock_server::{Lock, LockServer},
    LockRequest, LockResponse, UnlockRequest, UnlockResponse,
};

pub use self::etcdserverpb::range_request::{SortOrder, SortTarget};

impl User {
    /// Check if user has the given role
    pub(crate) fn has_role(&self, role: &str) -> bool {
        self.roles.binary_search(&role.to_owned()).is_ok()
    }
}

/// Wrapper for requests
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct RequestWithToken {
    /// token for authentication
    pub(crate) token: Option<String>,
    /// Internal request
    pub(crate) request: RequestWrapper,
}

/// Internal request
#[derive(Serialize, Deserialize, Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum RequestWrapper {
    /// `RangeRequest`
    RangeRequest(RangeRequest),
    /// `PutRequest`
    PutRequest(PutRequest),
    /// `DeleteRangeRequest`
    DeleteRangeRequest(DeleteRangeRequest),
    /// `TxnRequest`
    TxnRequest(TxnRequest),
    /// `CompactionRequest`
    CompactionRequest(CompactionRequest),
    /// `AuthEnableRequest`
    AuthEnableRequest(AuthEnableRequest),
    /// `AuthDisableRequest`
    AuthDisableRequest(AuthDisableRequest),
    /// `AuthStatusRequest`
    AuthStatusRequest(AuthStatusRequest),
    /// `AuthRoleAddRequest`
    AuthRoleAddRequest(AuthRoleAddRequest),
    /// `AuthRoleDeleteRequest`
    AuthRoleDeleteRequest(AuthRoleDeleteRequest),
    /// `AuthRoleGetRequest`
    AuthRoleGetRequest(AuthRoleGetRequest),
    /// `AuthRoleGrantPermissionRequest`
    AuthRoleGrantPermissionRequest(AuthRoleGrantPermissionRequest),
    /// `AuthRoleListRequest`
    AuthRoleListRequest(AuthRoleListRequest),
    /// `AuthRoleRevokePermissionRequest`
    AuthRoleRevokePermissionRequest(AuthRoleRevokePermissionRequest),
    /// `AuthUserAddRequest`
    AuthUserAddRequest(AuthUserAddRequest),
    /// `AuthUserChangePasswordRequest`
    AuthUserChangePasswordRequest(AuthUserChangePasswordRequest),
    /// `AuthUserDeleteRequest`
    AuthUserDeleteRequest(AuthUserDeleteRequest),
    /// `AuthUserGetRequest`
    AuthUserGetRequest(AuthUserGetRequest),
    /// `AuthUserGrantRoleRequest`
    AuthUserGrantRoleRequest(AuthUserGrantRoleRequest),
    /// `AuthUserListRequest`
    AuthUserListRequest(AuthUserListRequest),
    /// `AuthUserRevokeRoleRequest`
    AuthUserRevokeRoleRequest(AuthUserRevokeRoleRequest),
    /// `AuthenticateRequest`
    AuthenticateRequest(AuthenticateRequest),
}

/// Wrapper for responses
#[derive(Serialize, Deserialize, Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ResponseWrapper {
    /// `RangeResponse`
    RangeResponse(RangeResponse),
    /// `PutResponse`
    PutResponse(PutResponse),
    /// `DeleteRangeResponse`
    DeleteRangeResponse(DeleteRangeResponse),
    /// `TxnResponse`
    TxnResponse(TxnResponse),
    /// `CompactionResponse`
    CompactionResponse(CompactionResponse),
    /// `AuthEnableResponse`
    AuthEnableResponse(AuthEnableResponse),
    /// `AuthDisableResponse`
    AuthDisableResponse(AuthDisableResponse),
    /// `AuthStatusResponse`
    AuthStatusResponse(AuthStatusResponse),
    /// `AuthRoleAddResponse`
    AuthRoleAddResponse(AuthRoleAddResponse),
    /// `AuthRoleDeleteResponse`
    AuthRoleDeleteResponse(AuthRoleDeleteResponse),
    /// `AuthRoleGetResponse`
    AuthRoleGetResponse(AuthRoleGetResponse),
    /// `AuthRoleGrantPermissionResponse`
    AuthRoleGrantPermissionResponse(AuthRoleGrantPermissionResponse),
    /// `AuthRoleListResponse`
    AuthRoleListResponse(AuthRoleListResponse),
    /// `AuthRoleRevokePermissionResponse`
    AuthRoleRevokePermissionResponse(AuthRoleRevokePermissionResponse),
    /// `AuthUserAddResponse`
    AuthUserAddResponse(AuthUserAddResponse),
    /// `AuthUserChangePasswordResponse`
    AuthUserChangePasswordResponse(AuthUserChangePasswordResponse),
    /// `AuthUserDeleteResponse`
    AuthUserDeleteResponse(AuthUserDeleteResponse),
    /// `AuthUserGetResponse`
    AuthUserGetResponse(AuthUserGetResponse),
    /// `AuthUserGrantRoleResponse`
    AuthUserGrantRoleResponse(AuthUserGrantRoleResponse),
    /// `AuthUserListResponse`
    AuthUserListResponse(AuthUserListResponse),
    /// `AuthUserRevokeRoleResponse`
    AuthUserRevokeRoleResponse(AuthUserRevokeRoleResponse),
    /// `AuthenticateResponse`
    AuthenticateResponse(AuthenticateResponse),
}

/// impl `From` trait for all request types
macro_rules! impl_from_requests {
    ($($req:ident),*) => {
        $(
            impl From<$req> for RequestWrapper {
                fn from(req: $req) -> Self {
                    RequestWrapper::$req(req)
                }
            }

            impl From<RequestWrapper> for $req {
                fn from(req: RequestWrapper) -> Self {
                    match req {
                        RequestWrapper::$req(req) => req,
                        _ => panic!("wrong request type"),
                    }
                }
            }
        )*
    };
}

/// impl `From` trait for all response types
macro_rules! impl_from_responses {
    ($($resp:ident),*) => {
        $(
            impl From<$resp> for ResponseWrapper {
                fn from(resp: $resp) -> Self {
                    ResponseWrapper::$resp(resp)
                }
            }

            impl From<ResponseWrapper> for $resp {
                fn from(resp: ResponseWrapper) -> Self {
                    match resp {
                        ResponseWrapper::$resp(resp) => resp,
                        _ => panic!("wrong response type"),
                    }
                }
            }
        )*
    };
}

impl_from_requests!(
    RangeRequest,
    PutRequest,
    DeleteRangeRequest,
    TxnRequest,
    CompactionRequest,
    AuthEnableRequest,
    AuthDisableRequest,
    AuthStatusRequest,
    AuthRoleAddRequest,
    AuthRoleDeleteRequest,
    AuthRoleGetRequest,
    AuthRoleGrantPermissionRequest,
    AuthRoleListRequest,
    AuthRoleRevokePermissionRequest,
    AuthUserAddRequest,
    AuthUserChangePasswordRequest,
    AuthUserDeleteRequest,
    AuthUserGetRequest,
    AuthUserGrantRoleRequest,
    AuthUserListRequest,
    AuthUserRevokeRoleRequest,
    AuthenticateRequest
);

impl_from_responses!(
    RangeResponse,
    PutResponse,
    DeleteRangeResponse,
    TxnResponse,
    CompactionResponse,
    AuthEnableResponse,
    AuthDisableResponse,
    AuthStatusResponse,
    AuthRoleAddResponse,
    AuthRoleDeleteResponse,
    AuthRoleGetResponse,
    AuthRoleGrantPermissionResponse,
    AuthRoleListResponse,
    AuthRoleRevokePermissionResponse,
    AuthUserAddResponse,
    AuthUserChangePasswordResponse,
    AuthUserDeleteResponse,
    AuthUserGetResponse,
    AuthUserGrantRoleResponse,
    AuthUserListResponse,
    AuthUserRevokeRoleResponse,
    AuthenticateResponse
);

impl From<RequestOp> for RequestWrapper {
    fn from(request_op: RequestOp) -> Self {
        match request_op.request {
            Some(Request::RequestRange(req)) => RequestWrapper::RangeRequest(req),
            Some(Request::RequestPut(req)) => RequestWrapper::PutRequest(req),
            Some(Request::RequestDeleteRange(req)) => RequestWrapper::DeleteRangeRequest(req),
            Some(Request::RequestTxn(req)) => RequestWrapper::TxnRequest(req),
            None => panic!("request is not set"),
        }
    }
}

impl From<ResponseWrapper> for ResponseOp {
    fn from(response_wrapper: ResponseWrapper) -> Self {
        #[allow(clippy::wildcard_enum_match_arm)]
        match response_wrapper {
            ResponseWrapper::RangeResponse(resp) => ResponseOp {
                response: Some(Response::ResponseRange(resp)),
            },
            ResponseWrapper::PutResponse(resp) => ResponseOp {
                response: Some(Response::ResponsePut(resp)),
            },
            ResponseWrapper::DeleteRangeResponse(resp) => ResponseOp {
                response: Some(Response::ResponseDeleteRange(resp)),
            },
            ResponseWrapper::TxnResponse(resp) => ResponseOp {
                response: Some(Response::ResponseTxn(resp)),
            },
            _ => panic!("wrong response type"),
        }
    }
}

impl RequestWithToken {
    /// New `RequestWithToken`
    pub(crate) fn new(request: RequestWrapper) -> Self {
        RequestWithToken {
            token: None,
            request,
        }
    }

    /// New `RequestWithToken` with token
    pub(crate) fn new_with_token(request: RequestWrapper, token: Option<String>) -> Self {
        RequestWithToken { token, request }
    }
}
