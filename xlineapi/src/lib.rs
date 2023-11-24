//! xlineapi
#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html

    absolute_paths_not_starting_with_crate,
    // box_pointers, async trait must use it
    // elided_lifetimes_in_paths,  // allow anonymous lifetime
    explicit_outlives_requirements,
    keyword_idents,
    macro_use_extern_crate,
    meta_variable_misuse,
    missing_abi,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    // must_not_suspend, unstable
    non_ascii_idents,
    // non_exhaustive_omitted_patterns, unstable
    noop_method_call,
    pointer_structural_match,
    rust_2021_incompatible_closure_captures,
    rust_2021_incompatible_or_patterns,
    rust_2021_prefixes_incompatible_syntax,
    rust_2021_prelude_collisions,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    unstable_features,
    // unused_crate_dependencies, the false positive case blocks us
    unused_extern_crates,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    unused_results,
    variant_size_differences,

    warnings, // treat all warnings as errors

    clippy::all,
    clippy::pedantic,
    clippy::cargo,

    // The followings are selected restriction lints for rust 1.57
    clippy::as_conversions,
    clippy::clone_on_ref_ptr,
    clippy::create_dir,
    clippy::dbg_macro,
    clippy::decimal_literal_representation,
    // clippy::default_numeric_fallback, too verbose when dealing with numbers
    clippy::disallowed_script_idents,
    clippy::else_if_without_else,
    clippy::exhaustive_enums,
    clippy::exhaustive_structs,
    clippy::exit,
    clippy::expect_used,
    clippy::filetype_is_file,
    clippy::float_arithmetic,
    clippy::float_cmp_const,
    clippy::get_unwrap,
    clippy::if_then_some_else_none,
    // clippy::implicit_return, it's idiomatic Rust code.
    clippy::indexing_slicing,
    // clippy::inline_asm_x86_att_syntax, stick to intel syntax
    clippy::inline_asm_x86_intel_syntax,
    clippy::integer_arithmetic,
    // clippy::integer_division, required in the project
    clippy::let_underscore_must_use,
    clippy::lossy_float_literal,
    clippy::map_err_ignore,
    clippy::mem_forget,
    clippy::missing_docs_in_private_items,
    clippy::missing_enforced_import_renames,
    clippy::missing_inline_in_public_items,
    // clippy::mod_module_files, mod.rs file is used
    clippy::modulo_arithmetic,
    clippy::multiple_inherent_impl,
    // clippy::panic, allow in application code
    // clippy::panic_in_result_fn, not necessary as panic is banned
    clippy::pattern_type_mismatch,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::rc_buffer,
    clippy::rc_mutex,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::same_name_method,
    clippy::self_named_module_files,
    // clippy::shadow_reuse, it’s a common pattern in Rust code
    // clippy::shadow_same, it’s a common pattern in Rust code
    clippy::shadow_unrelated,
    clippy::str_to_string,
    clippy::string_add,
    clippy::string_to_string,
    clippy::todo,
    clippy::unimplemented,
    clippy::unnecessary_self_imports,
    clippy::unneeded_field_pattern,
    // clippy::unreachable, allow unreachable panic, which is out of expectation
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    // clippy::use_debug, debug is allow for debug log
    clippy::verbose_file_reads,
    clippy::wildcard_enum_match_arm,

    // The followings are selected lints from 1.61.0 to 1.67.1
    clippy::as_ptr_cast_mut,
    clippy::derive_partial_eq_without_eq,
    clippy::empty_drop,
    clippy::empty_structs_with_brackets,
    clippy::format_push_string,
    clippy::iter_on_empty_collections,
    clippy::iter_on_single_items,
    clippy::large_include_file,
    clippy::manual_clamp,
    clippy::suspicious_xor_used_as_pow,
    clippy::unnecessary_safety_comment,
    clippy::unnecessary_safety_doc,
    clippy::unused_peekable,
    clippy::unused_rounding,

    // The followings are selected restriction lints from rust 1.68.0 to 1.70.0
    // clippy::allow_attributes, still unstable
    clippy::impl_trait_in_params,
    clippy::let_underscore_untyped,
    clippy::missing_assert_message,
    clippy::multiple_unsafe_ops_per_block,
    clippy::semicolon_inside_block,
    // clippy::semicolon_outside_block, already used `semicolon_inside_block`
    clippy::tests_outside_test_module
)]
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
    missing_docs,
    unused_results,
    trivial_casts
)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        unused_results,
        clippy::unwrap_used,
        clippy::as_conversions,
        clippy::shadow_unrelated,
        clippy::integer_arithmetic
    )
)]

pub mod command;
pub mod execute_error;
pub mod request_validation;

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

mod commandpb {
    tonic::include_proto!("commandpb");
}

mod errorpb {
    tonic::include_proto!("errorpb");
}

use curp_external_api::cmd::PbSerializeError;
use serde::{Deserialize, Serialize};

pub use self::{
    authpb::{permission::Type, Permission, Role, User, UserAddOptions},
    commandpb::{
        command_response::ResponseWrapper, request_with_token::RequestWrapper,
        Command as PbCommand, CommandResponse as PbCommandResponse, KeyRange as PbKeyRange,
        RequestWithToken as PbRequestWithToken, SyncResponse as PbSyncResponse,
    },
    errorpb::{
        execute_error::Error as PbExecuteError, ExecuteError as PbExecuteErrorOuter,
        Revisions as PbRevisions, UserRole as PbUserRole,
    },
    etcdserverpb::{
        alarm_request::AlarmAction,
        auth_client::AuthClient,
        auth_server::{Auth, AuthServer},
        cluster_client::ClusterClient,
        cluster_server::{Cluster, ClusterServer},
        compare::{CompareResult, CompareTarget, TargetUnion},
        kv_client::KvClient,
        kv_server::{Kv, KvServer},
        lease_client::LeaseClient,
        lease_server::{Lease, LeaseServer},
        maintenance_client::MaintenanceClient,
        maintenance_server::{Maintenance, MaintenanceServer},
        range_request::{SortOrder, SortTarget},
        request_op::Request,
        response_op::Response,
        watch_client::WatchClient,
        watch_request::RequestUnion,
        watch_server::{Watch, WatchServer},
        AlarmMember, AlarmRequest, AlarmResponse, AlarmType, AuthDisableRequest,
        AuthDisableResponse, AuthEnableRequest, AuthEnableResponse, AuthRoleAddRequest,
        AuthRoleAddResponse, AuthRoleDeleteRequest, AuthRoleDeleteResponse, AuthRoleGetRequest,
        AuthRoleGetResponse, AuthRoleGrantPermissionRequest, AuthRoleGrantPermissionResponse,
        AuthRoleListRequest, AuthRoleListResponse, AuthRoleRevokePermissionRequest,
        AuthRoleRevokePermissionResponse, AuthStatusRequest, AuthStatusResponse,
        AuthUserAddRequest, AuthUserAddResponse, AuthUserChangePasswordRequest,
        AuthUserChangePasswordResponse, AuthUserDeleteRequest, AuthUserDeleteResponse,
        AuthUserGetRequest, AuthUserGetResponse, AuthUserGrantRoleRequest,
        AuthUserGrantRoleResponse, AuthUserListRequest, AuthUserListResponse,
        AuthUserRevokeRoleRequest, AuthUserRevokeRoleResponse, AuthenticateRequest,
        AuthenticateResponse, CompactionRequest, CompactionResponse, Compare, DefragmentRequest,
        DefragmentResponse, DeleteRangeRequest, DeleteRangeResponse, DowngradeRequest,
        DowngradeResponse, HashKvRequest, HashKvResponse, HashRequest, HashResponse,
        LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
        LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
        LeaseStatus, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, Member, MemberAddRequest,
        MemberAddResponse, MemberListRequest, MemberListResponse, MemberPromoteRequest,
        MemberPromoteResponse, MemberRemoveRequest, MemberRemoveResponse, MemberUpdateRequest,
        MemberUpdateResponse, MoveLeaderRequest, MoveLeaderResponse, PutRequest, PutResponse,
        RangeRequest, RangeResponse, RequestOp, ResponseHeader, ResponseOp, SnapshotRequest,
        SnapshotResponse, StatusRequest, StatusResponse, TxnRequest, TxnResponse,
        WatchCancelRequest, WatchCreateRequest, WatchProgressRequest, WatchRequest, WatchResponse,
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
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.binary_search(&role.to_owned()).is_ok()
    }
}

/// Wrapper for requests
#[derive(PartialEq)] // used only in tests, doesn't work with a `cfg_attr` macro.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestWithToken {
    /// token for authentication
    pub token: Option<String>,
    /// Internal request
    pub request: RequestWrapper,
}

impl TryFrom<PbRequestWithToken> for RequestWithToken {
    type Error = PbSerializeError;

    #[inline]
    fn try_from(req: PbRequestWithToken) -> Result<Self, Self::Error> {
        let request = req.request_wrapper.ok_or(PbSerializeError::EmptyField)?;
        Ok(Self {
            token: req.token,
            request,
        })
    }
}

impl From<RequestWithToken> for PbRequestWithToken {
    #[inline]
    fn from(req: RequestWithToken) -> Self {
        PbRequestWithToken {
            token: req.token,
            request_wrapper: Some(req.request),
        }
    }
}

impl ResponseWrapper {
    /// Update response revision
    pub fn update_revision(&mut self, revision: i64) {
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
            ResponseWrapper::LeaseLeasesResponse(ref mut resp) => &mut resp.header,
            ResponseWrapper::AlarmResponse(ref mut resp) => &mut resp.header,
        };
        if let Some(ref mut header) = *header {
            header.revision = revision;
        }
    }
}

/// Backend store of request
#[derive(Debug, PartialEq, Eq)]
pub enum RequestBackend {
    /// Kv backend
    Kv,
    /// Auth backend
    Auth,
    /// Lease backend
    Lease,
    /// Alarm backend
    Alarm,
}

impl RequestWrapper {
    /// Get the backend of the request
    pub fn backend(&self) -> RequestBackend {
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
            RequestWrapper::LeaseGrantRequest(_)
            | RequestWrapper::LeaseRevokeRequest(_)
            | RequestWrapper::LeaseLeasesRequest(_) => RequestBackend::Lease,
            RequestWrapper::AlarmRequest(_) => RequestBackend::Alarm,
        }
    }

    /// Check if this request is a auth read request
    pub fn is_auth_read_request(&self) -> bool {
        matches!(
            *self,
            RequestWrapper::AuthStatusRequest(_)
                | RequestWrapper::AuthRoleGetRequest(_)
                | RequestWrapper::AuthRoleListRequest(_)
                | RequestWrapper::AuthUserGetRequest(_)
                | RequestWrapper::AuthUserListRequest(_)
        )
    }

    /// Check whether this auth request should skip the revision or not
    pub fn skip_auth_revision(&self) -> bool {
        self.is_auth_read_request()
            || matches!(
                *self,
                RequestWrapper::AuthEnableRequest(_) | RequestWrapper::AuthenticateRequest(_)
            )
    }

    /// Check whether the kv request or lease request should skip the revision or not
    pub fn skip_general_revision(&self) -> bool {
        match self {
            RequestWrapper::RangeRequest(_)
            | RequestWrapper::LeaseGrantRequest(_)
            | RequestWrapper::CompactionRequest(_) => true,
            RequestWrapper::TxnRequest(req) => req.is_read_only(),
            _ => false,
        }
    }

    /// Check if this request is a auth request
    pub fn is_auth_request(&self) -> bool {
        self.backend() == RequestBackend::Auth
    }

    /// Check if this request is a kv request
    pub fn is_kv_request(&self) -> bool {
        self.backend() == RequestBackend::Kv
    }

    pub fn is_compaction_request(&self) -> bool {
        matches!(*self, RequestWrapper::CompactionRequest(_))
    }

    pub fn is_txn_request(&self) -> bool {
        matches!(*self, RequestWrapper::TxnRequest(_))
    }

    pub fn is_lease_read_request(&self) -> bool {
        matches!(*self, RequestWrapper::LeaseLeasesRequest(_))
    }

    pub fn is_lease_write_request(&self) -> bool {
        matches!(
            *self,
            RequestWrapper::LeaseGrantRequest(_) | RequestWrapper::LeaseRevokeRequest(_)
        )
    }

    pub fn is_alarm_request(&self) -> bool {
        matches!(*self, RequestWrapper::AlarmRequest(_))
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
    LeaseRevokeRequest,
    LeaseLeasesRequest,
    AlarmRequest
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
    LeaseRevokeResponse,
    LeaseLeasesResponse,
    AlarmResponse
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
    pub fn new(request: RequestWrapper) -> Self {
        RequestWithToken {
            token: None,
            request,
        }
    }

    /// New `RequestWithToken` with token
    pub fn new_with_token(request: RequestWrapper, token: Option<String>) -> Self {
        RequestWithToken { token, request }
    }
}

impl Event {
    pub fn is_create(&self) -> bool {
        let kv = self
            .kv
            .as_ref()
            .unwrap_or_else(|| panic!("kv must be Some"));
        matches!(self.r#type(), EventType::Put) && kv.create_revision == kv.mod_revision
    }
}

impl TxnRequest {
    /// Checks whether a given `TxnRequest` is read-only or not.
    pub fn is_read_only(&self) -> bool {
        let read_only_checker = |req: &RequestOp| {
            if let Some(ref request) = req.request {
                match request {
                    Request::RequestRange(_) => true,
                    Request::RequestDeleteRange(_) | Request::RequestPut(_) => false,
                    Request::RequestTxn(req) => req.is_read_only(),
                }
            } else {
                true
            }
        };
        self.success.iter().all(read_only_checker) && self.failure.iter().all(read_only_checker)
    }

    /// Checks whether a given `TxnRequest` is serializable or not.
    pub fn is_serializable(&self) -> bool {
        let serializable_checker = |req: &RequestOp| {
            if let Some(ref request) = req.request {
                match request {
                    Request::RequestRange(req) => req.serializable,
                    Request::RequestDeleteRange(_) | Request::RequestPut(_) => false,
                    Request::RequestTxn(req) => req.is_serializable(),
                }
            } else {
                false
            }
        };
        self.success.iter().all(serializable_checker)
            && self.failure.iter().all(serializable_checker)
    }

    /// Checks whether a `TxnRequest` is conflict with a given revision
    pub fn is_conflict_with_rev(&self, revision: i64) -> bool {
        let conflict_checker = |req: &RequestOp| {
            if let Some(ref request) = req.request {
                match request {
                    Request::RequestRange(req) => req.revision > 0 && req.revision < revision,
                    Request::RequestDeleteRange(_) | Request::RequestPut(_) => false,
                    Request::RequestTxn(req) => req.is_conflict_with_rev(revision),
                }
            } else {
                false
            }
        };
        self.success.iter().any(conflict_checker) || self.failure.iter().any(conflict_checker)
    }
}

impl AlarmRequest {
    pub fn new(action: AlarmAction, member_id: u64, alarm: AlarmType) -> Self {
        Self {
            action: i32::from(action),
            member_id,
            alarm: i32::from(alarm),
        }
    }
}

impl AlarmMember {
    pub fn new(member_id: u64, alarm: AlarmType) -> Self {
        Self {
            member_id,
            alarm: i32::from(alarm),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn txn_request_is_read_only_should_success() {
        let read_only_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest::default())),
            }],
            failure: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest::default())),
            }],
        };

        assert!(read_only_txn_req.is_read_only());

        let read_write_mixed_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest::default())),
            }],
            failure: vec![RequestOp {
                request: Some(Request::RequestPut(PutRequest::default())),
            }],
        };

        assert!(!read_write_mixed_txn_req.is_read_only());

        let read_only_nested_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestTxn(TxnRequest {
                    compare: vec![],
                    success: vec![RequestOp {
                        request: Some(Request::RequestRange(RangeRequest::default())),
                    }],
                    failure: vec![],
                })),
            }],
            failure: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest::default())),
            }],
        };

        assert!(read_only_nested_txn_req.is_read_only());

        let read_write_nested_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestTxn(TxnRequest {
                    compare: vec![],
                    success: vec![RequestOp {
                        request: Some(Request::RequestTxn(TxnRequest {
                            compare: vec![],
                            success: vec![RequestOp {
                                request: Some(Request::RequestRange(RangeRequest::default())),
                            }],
                            failure: vec![RequestOp {
                                request: Some(Request::RequestPut(PutRequest::default())),
                            }],
                        })),
                    }],
                    failure: vec![RequestOp {
                        request: Some(Request::RequestRange(RangeRequest::default())),
                    }],
                })),
            }],
            failure: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest::default())),
            }],
        };

        assert!(!read_write_nested_txn_req.is_read_only());
    }

    #[test]
    fn txn_request_is_serializable_should_success() {
        let serializable_req = Some(Request::RequestRange(RangeRequest {
            serializable: true,
            ..Default::default()
        }));

        let serializable_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: serializable_req.clone(),
            }],
            failure: vec![RequestOp {
                request: serializable_req.clone(),
            }],
        };

        assert!(serializable_txn_req.is_serializable());

        let mixed_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: serializable_req.clone(),
            }],
            failure: vec![RequestOp {
                request: Some(Request::RequestPut(PutRequest::default())),
            }],
        };

        assert!(!mixed_txn_req.is_serializable());

        let serializable_nested_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestTxn(TxnRequest {
                    compare: vec![],
                    success: vec![RequestOp {
                        request: serializable_req.clone(),
                    }],
                    failure: vec![],
                })),
            }],
            failure: vec![RequestOp {
                request: serializable_req.clone(),
            }],
        };

        assert!(serializable_nested_txn_req.is_serializable());

        let mixed_nested_txn_req = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Request::RequestTxn(TxnRequest {
                    compare: vec![],
                    success: vec![RequestOp {
                        request: Some(Request::RequestTxn(TxnRequest {
                            compare: vec![],
                            success: vec![RequestOp {
                                request: serializable_req.clone(),
                            }],
                            failure: vec![RequestOp {
                                request: Some(Request::RequestPut(PutRequest::default())),
                            }],
                        })),
                    }],
                    failure: vec![RequestOp {
                        request: serializable_req.clone(),
                    }],
                })),
            }],
            failure: vec![RequestOp {
                request: serializable_req,
            }],
        };

        assert!(!mixed_nested_txn_req.is_serializable());
    }
}
