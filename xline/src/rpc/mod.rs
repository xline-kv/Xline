// Skip for generated code
#![allow(
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

mod authpb {
    tonic::include_proto!("authpb");
}

mod mvccpb {
    tonic::include_proto!("mvccpb");
}

mod v3lockpb {
    tonic::include_proto!("v3lockpb");
}

mod leasepb {
    tonic::include_proto!("leasepb");
}

use serde::{Deserialize, Serialize};

pub use self::etcdserverpb::range_request::{SortOrder, SortTarget};
pub(crate) use self::{
    authpb::{permission::Type, Permission, Role, User},
    etcdserverpb::{
        auth_server::{Auth, AuthServer},
        compare::{CompareResult, CompareTarget, TargetUnion},
        kv_client::KvClient,
        kv_server::{Kv, KvServer},
        lease_client::LeaseClient,
        lease_server::{Lease, LeaseServer},
        maintenance_server::{Maintenance, MaintenanceServer},
        request_op::Request,
        response_op::Response,
        watch_client::WatchClient,
        watch_request::RequestUnion,
        watch_server::{Watch, WatchServer},
        AlarmRequest, AlarmResponse, AuthDisableRequest, AuthDisableResponse, AuthEnableRequest,
        AuthEnableResponse, AuthRoleAddRequest, AuthRoleAddResponse, AuthRoleDeleteRequest,
        AuthRoleDeleteResponse, AuthRoleGetRequest, AuthRoleGetResponse,
        AuthRoleGrantPermissionRequest, AuthRoleGrantPermissionResponse, AuthRoleListRequest,
        AuthRoleListResponse, AuthRoleRevokePermissionRequest, AuthRoleRevokePermissionResponse,
        AuthStatusRequest, AuthStatusResponse, AuthUserAddRequest, AuthUserAddResponse,
        AuthUserChangePasswordRequest, AuthUserChangePasswordResponse, AuthUserDeleteRequest,
        AuthUserDeleteResponse, AuthUserGetRequest, AuthUserGetResponse, AuthUserGrantRoleRequest,
        AuthUserGrantRoleResponse, AuthUserListRequest, AuthUserListResponse,
        AuthUserRevokeRoleRequest, AuthUserRevokeRoleResponse, AuthenticateRequest,
        AuthenticateResponse, CompactionRequest, CompactionResponse, Compare, DefragmentRequest,
        DefragmentResponse, DeleteRangeRequest, DeleteRangeResponse, DowngradeRequest,
        DowngradeResponse, HashKvRequest, HashKvResponse, HashRequest, HashResponse,
        LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
        LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
        LeaseStatus, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, MoveLeaderRequest,
        MoveLeaderResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, RequestOp,
        ResponseHeader, ResponseOp, SnapshotRequest, SnapshotResponse, StatusRequest,
        StatusResponse, TxnRequest, TxnResponse, WatchCancelRequest, WatchCreateRequest,
        WatchRequest, WatchResponse,
    },
    leasepb::Lease as PbLease,
    mvccpb::{event::EventType, Event, KeyValue},
    v3lockpb::{
        lock_server::{Lock, LockServer},
        LockRequest, LockResponse, UnlockRequest, UnlockResponse,
    },
};

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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)] // in order to quickly implement trait by macro
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
    /// `LeaseGrantRequest`
    LeaseGrantRequest(LeaseGrantRequest),
    /// `LeaseRevokeRequest`
    LeaseRevokeRequest(LeaseRevokeRequest),
}

/// Wrapper for responses
#[derive(Serialize, Deserialize, Debug, Clone)]
#[allow(clippy::enum_variant_names)] // in order to quickly implement trait by macro
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
    /// `LeaseGrantResponse`
    LeaseGrantResponse(LeaseGrantResponse),
    /// `LeaseRevokeResponse`
    LeaseRevokeResponse(LeaseRevokeResponse),
}

impl ResponseWrapper {
    /// Update response revision
    pub(crate) fn update_revision(&mut self, revision: i64) {
        let header = match *self {
            ResponseWrapper::RangeResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::PutResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::DeleteRangeResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::TxnResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::CompactionResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthEnableResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthDisableResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthStatusResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthRoleAddResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthRoleDeleteResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthRoleGetResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthRoleGrantPermissionResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthRoleListResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthRoleRevokePermissionResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthUserAddResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthUserChangePasswordResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthUserDeleteResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthUserGetResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthUserGrantRoleResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthUserListResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthUserRevokeRoleResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AuthenticateResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::LeaseGrantResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::LeaseRevokeResponse(ref mut resp) => &mut resp.header,
        };
        if let Some(ref mut header) = *header {
            header.revision = revision;
        }
    }
}

/// Backend store of request
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RequestBackend {
    /// Kv backend
    Kv,
    /// Auth backend
    Auth,
    /// Lease backend
    Lease,
}

impl RequestWrapper {
    /// Get the backend of the request
    pub(crate) fn backend(&self) -> RequestBackend {
        match *self {
            RequestWrapper::PutRequest(_)
            | RequestWrapper::RangeRequest(_)
            | RequestWrapper::DeleteRangeRequest(_)
            | RequestWrapper::TxnRequest(_)
            | RequestWrapper::CompactionRequest(_) => RequestBackend::Kv,
            RequestWrapper::AuthEnableRequest(_)
            | RequestWrapper::AuthDisableRequest(_)
            | RequestWrapper::AuthStatusRequest(_)
            | RequestWrapper::AuthRoleAddRequest(_)
            | RequestWrapper::AuthRoleDeleteRequest(_)
            | RequestWrapper::AuthRoleGetRequest(_)
            | RequestWrapper::AuthRoleGrantPermissionRequest(_)
            | RequestWrapper::AuthRoleListRequest(_)
            | RequestWrapper::AuthRoleRevokePermissionRequest(_)
            | RequestWrapper::AuthUserAddRequest(_)
            | RequestWrapper::AuthUserChangePasswordRequest(_)
            | RequestWrapper::AuthUserDeleteRequest(_)
            | RequestWrapper::AuthUserGetRequest(_)
            | RequestWrapper::AuthUserGrantRoleRequest(_)
            | RequestWrapper::AuthUserListRequest(_)
            | RequestWrapper::AuthUserRevokeRoleRequest(_)
            | RequestWrapper::AuthenticateRequest(_) => RequestBackend::Auth,
            RequestWrapper::LeaseGrantRequest(_) | RequestWrapper::LeaseRevokeRequest(_) => {
                RequestBackend::Lease
            }
        }
    }

    /// Check if this request is a auth read request
    pub(crate) fn is_auth_read_request(&self) -> bool {
        matches!(
            *self,
            RequestWrapper::AuthStatusRequest(_)
                | RequestWrapper::AuthRoleGetRequest(_)
                | RequestWrapper::AuthRoleListRequest(_)
                | RequestWrapper::AuthUserGetRequest(_)
                | RequestWrapper::AuthUserListRequest(_)
        )
    }

    /// Check if this request is a auth request
    pub(crate) fn is_auth_request(&self) -> bool {
        self.backend() == RequestBackend::Auth
    }

    /// Check if this request is a kv request
    pub(crate) fn is_kv_request(&self) -> bool {
        self.backend() == RequestBackend::Kv
    }

    /// Check if this request is a lease request
    pub(crate) fn is_lease_request(&self) -> bool {
        self.backend() == RequestBackend::Lease
    }
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
    AuthenticateRequest,
    LeaseGrantRequest,
    LeaseRevokeRequest
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
    AuthenticateResponse,
    LeaseGrantResponse,
    LeaseRevokeResponse
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
    pub(crate) fn new_with_token(request: RequestWrapper, token: String) -> Self {
        RequestWithToken {
            token: Some(token),
            request,
        }
    }
}
