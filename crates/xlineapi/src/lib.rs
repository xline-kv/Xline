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
    clippy::arithmetic_side_effects,
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

    // The followings are selected restriction lints from rust 1.68.0 to 1.71.0
    // clippy::allow_attributes, still unstable
    clippy::impl_trait_in_params,
    clippy::let_underscore_untyped,
    clippy::missing_assert_message,
    clippy::multiple_unsafe_ops_per_block,
    clippy::semicolon_inside_block,
    // clippy::semicolon_outside_block, already used `semicolon_inside_block`
    clippy::tests_outside_test_module,
    // 1.71.0
    clippy::default_constructed_unit_structs,
    clippy::items_after_test_module,
    clippy::manual_next_back,
    clippy::manual_while_let_some,
    clippy::needless_bool_assign,
    clippy::non_minimal_cfg,
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
        clippy::arithmetic_side_effects
    )
)]

pub mod command;
pub mod execute_error;
pub mod interval;
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

use std::fmt::Display;

use command::KeyRange;
use utils::write_vec;

pub use self::{
    authpb::{permission::Type, Permission, Role, User, UserAddOptions},
    commandpb::{
        command::{AuthInfo, RequestWrapper},
        command_response::ResponseWrapper,
        Command as PbCommand, CommandResponse as PbCommandResponse, KeyRange as PbKeyRange,
        SyncResponse as PbSyncResponse,
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

/// Get command keys from a Request for conflict check
pub trait CommandKeys {
    /// Key ranges
    fn keys(&self) -> Vec<KeyRange>;
}

impl CommandKeys for RangeRequest {
    fn keys(&self) -> Vec<KeyRange> {
        vec![KeyRange::new(
            self.key.as_slice(),
            self.range_end.as_slice(),
        )]
    }
}

impl CommandKeys for PutRequest {
    fn keys(&self) -> Vec<KeyRange> {
        vec![KeyRange::new_one_key(self.key.as_slice())]
    }
}

impl CommandKeys for DeleteRangeRequest {
    fn keys(&self) -> Vec<KeyRange> {
        vec![KeyRange::new(
            self.key.as_slice(),
            self.range_end.as_slice(),
        )]
    }
}

impl CommandKeys for TxnRequest {
    fn keys(&self) -> Vec<KeyRange> {
        let mut keys: Vec<_> = self
            .compare
            .iter()
            .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
            .collect();

        for op in self
            .success
            .iter()
            .chain(self.failure.iter())
            .map(|op| &op.request)
            .flatten()
        {
            match *op {
                Request::RequestRange(ref req) => {
                    keys.push(KeyRange::new(req.key.as_slice(), req.range_end.as_slice()));
                }
                Request::RequestPut(ref req) => {
                    keys.push(KeyRange::new_one_key(req.key.as_slice()))
                }
                Request::RequestDeleteRange(ref req) => {
                    keys.push(KeyRange::new(req.key.as_slice(), req.range_end.as_slice()))
                }
                Request::RequestTxn(ref req) => keys.append(&mut req.keys()),
            }
        }

        keys
    }
}

impl RequestWrapper {
    /// Get keys of the request
    pub fn keys(&self) -> Vec<KeyRange> {
        match *self {
            RequestWrapper::RangeRequest(ref req) => req.keys(),
            RequestWrapper::PutRequest(ref req) => req.keys(),
            RequestWrapper::DeleteRangeRequest(ref req) => req.keys(),
            RequestWrapper::TxnRequest(ref req) => req.keys(),
            _ => vec![],
        }
    }

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

impl Display for AlarmMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let alarm =
            AlarmType::try_from(self.alarm).unwrap_or_else(|e| panic!("invalid alarm, err: {e}"));
        let alarm_str = match alarm {
            AlarmType::Nospace => "NOSPACE",
            AlarmType::Corrupt => "CORRUPT",
            AlarmType::None => "NONE",
        };
        write!(f, "memberID:{} alarm:{} ", self.member_id, alarm_str)
    }
}

/// Since the tokio-rs/prost will automatically derive Debug trait for all the structures it generates, we have to
/// override the Display trait for these requests and responses.
/// FYI: https://github.com/tokio-rs/prost/issues/334
impl Display for PutRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PutRequest {{ key: {:?}, value: {:?}, lease: {:?}, prev_kv: {:?}, ignore_value: {:?}, ignore_lease: {:?} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value),
            self.lease, self.prev_kv,
            self.ignore_value,
            self.ignore_lease)
    }
}

impl Display for PutResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref prev_kv) = self.prev_kv {
            write!(
                f,
                "PutResponse {{ header: {:?}, prev_kv: {} }}",
                self.header, prev_kv
            )
        } else {
            write!(
                f,
                "PutResponse {{ header: {:?}, prev_kv: None }}",
                self.header
            )
        }
    }
}

impl Display for RangeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RangeRequest {{ key: {:?}, range_end: {:?}, limit: {:?}, revision: {:?}, sort_order: {:?}, ",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.range_end),
            self.limit,
            self.revision,
            self.sort_order,
        )?;
        write!(
            f,
            "sort_target: {:?}, serializable: {:?}, keys_only: {:?}, count_only: {:?}, min_mod_revision: {:?}, ",
            self.sort_target,
            self.serializable,
            self.keys_only,
            self.count_only,
            self.min_mod_revision
        )?;
        write!(
            f,
            "max_mod_revision: {:?}, min_create_revision: {:?}, max_create_revision: {:?}, key_only: {:?}, count_only: {:?}, ",
            self.max_mod_revision,
            self.min_create_revision,
            self.max_create_revision,
            self.keys_only,
            self.count_only,
        )?;
        write!(
            f,
            "min_mod_revision: {:?}, max_mod_revision: {:?}, min_create_revision: {:?}, max_create_revision: {:?} }}",
            self.min_mod_revision,
            self.max_mod_revision,
            self.min_create_revision,
            self.max_create_revision
        )
    }
}

impl Display for RangeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangeResponse {{ header: {:?},", self.header)?;
        write_vec!(f, "kvs", self.kvs);
        write!(f, ", more: {:?}, count: {:?} }}", self.more, self.count)
    }
}

impl Display for DeleteRangeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeleteRangeRequest {{ key: {:?}, range_end: {:?}, prev_kv: {:?} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.range_end),
            self.prev_kv
        )
    }
}

impl Display for DeleteRangeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeleteRangeResponse {{ header: {:?}, deleted: {:?}, ",
            self.header, self.deleted
        )?;
        write_vec!(f, "prev_kvs", self.prev_kvs);
        write!(f, "}}")
    }
}

impl Display for RequestOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RequestOp {{")?;
        if let Some(ref request) = self.request {
            match request {
                Request::RequestRange(req) => write!(f, "{}", req)?,
                Request::RequestDeleteRange(req) => write!(f, "{}", req)?,
                Request::RequestPut(req) => write!(f, "{}", req)?,
                Request::RequestTxn(req) => write!(f, "{}", req)?,
            }
        }
        write!(f, "}}")
    }
}

impl Display for ResponseOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResponseOp {{")?;
        if let Some(ref request) = self.response {
            match request {
                Response::ResponseRange(req) => write!(f, "{}", req)?,
                Response::ResponseDeleteRange(req) => write!(f, "{}", req)?,
                Response::ResponsePut(req) => write!(f, "{}", req)?,
                Response::ResponseTxn(req) => write!(f, "{}", req)?,
            }
        }
        write!(f, "}}")
    }
}

impl Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let perm = match self.perm_type {
            0 => "Read",
            1 => "Write",
            2 => "Readwrite",
            _ => "Unknown",
        };
        write!(
            f,
            "Permission {{ permType: {:?}, key: {:?}, range_end: {:?} }}",
            perm,
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.range_end)
        )
    }
}

impl Display for Compare {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let result = match self.result {
            0 => "Equal",
            1 => "Greater",
            2 => "Less",
            3 => "NotEqual",
            _ => "Unknown Result",
        };
        let target = match self.result {
            0 => "Version",
            1 => "Create",
            2 => "Mod",
            3 => "Value",
            4 => "Lease",
            _ => "Unknown Target",
        };
        write!(
            f,
            "Compare {{ result: {:?}, target: {:?}, key: {}, range_end: {}, target_union: {:?} }}",
            result,
            target,
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.range_end),
            self.target_union
        )
    }
}

impl Display for AuthUserAddRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref option) = self.options {
            write!(
                f,
                "AuthUserAddRequest {{ name: {},  no_password: {} }}",
                self.name, option.no_password
            )
        } else {
            write!(f, "AuthUserAddRequest {{ name: {} }}", self.name)
        }
    }
}

impl Display for AuthRoleGrantPermissionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref perm) = self.perm {
            write!(
                f,
                "AuthRoleGrantPermissionRequest {{ role: {}, perm: {} }}",
                self.name, perm
            )
        } else {
            write!(
                f,
                "AuthRoleGrantPermissionRequest {{ role: {}, perm: None }}",
                self.name
            )
        }
    }
}

impl Display for AuthRoleRevokePermissionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AuthRoleRevokePermissionRequest {{ role: {}, key: {}, range_end: {} }}",
            self.role,
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.key),
        )
    }
}

impl Display for TxnRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TxnRequest {{ ")?;
        write_vec!(f, "compare", self.compare);
        write_vec!(f, ", success", self.success);
        write_vec!(f, ", failure", self.failure);
        write!(f, "}}")
    }
}

impl Display for TxnResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TxnResponse {{ header: {:?}, succeeded: {} ",
            self.header, self.succeeded
        )?;
        write_vec!(f, "responses", self.responses);
        write!(f, "}}")
    }
}

impl Display for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KeyValue {{ key: {:?}, create_revision: {}, mod_revision: {}, version: {}, value: {:?}, lease: {} }}",
            String::from_utf8_lossy(&self.key),
            self.create_revision,
            self.mod_revision,
            self.version,
            String::from_utf8_lossy(&self.value),
            self.lease,
        )
    }
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Event {{ type: {:?}, kv: ", self.r#type())?;
        if let Some(kv) = &self.kv {
            write!(f, "{}", kv)?;
        }
        write!(f, ", prev_kv: ")?;
        if let Some(prev_kv) = &self.prev_kv {
            write!(f, "{}", prev_kv)?;
        }
        write!(f, " }}")
    }
}

impl Display for AuthRoleGetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuthRoleGetResponse {{ header: {:?}", self.header)?;
        write_vec!(f, "perm", self.perm);
        write!(f, " }}")
    }
}

impl Display for AuthRoleListResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AuthRoleListResponse {{ header: {:?}", self.header)?;
        write_vec!(f, "roles", self.roles);
        write!(f, " }}")
    }
}

impl Display for AlarmResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlarmResponse {{ header: {:?}, ", self.header)?;
        write_vec!(f, "alarms", self.alarms);
        write!(f, " }}")
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

    #[test]
    fn test_alarm_member_display() {
        let am = AlarmMember::new(10276657743932975437, AlarmType::Nospace);
        let expect = "memberID:10276657743932975437 alarm:NOSPACE ";
        assert_eq!(expect, am.to_string());
    }
}
