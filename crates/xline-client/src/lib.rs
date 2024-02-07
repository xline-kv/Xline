#![doc = include_str!("../README.md")]
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
#![allow(
    clippy::multiple_crate_versions, // caused by the dependency, can't be fixed
    clippy::module_name_repetitions, // It will be more easy to use for the type name prefixed by module name
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
use std::{
    fmt::Debug,
    sync::Arc,
    task::{Context, Poll},
};

use curp::client::ClientBuilder as CurpClientBuilder;
use http::{header::AUTHORIZATION, HeaderValue, Request};
use tonic::transport::Channel;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tower::Service;
#[cfg(madsim)]
use utils::ClientTlsConfig;
use utils::{build_endpoint, config::ClientConfig};
use xlineapi::command::{Command, CurpClient};

use crate::{
    clients::{
        AuthClient, ClusterClient, ElectionClient, KvClient, LeaseClient, LockClient,
        MaintenanceClient, WatchClient,
    },
    error::XlineClientBuildError,
};

/// Sub-clients for each type of API
pub mod clients;
/// Lease Id generator
mod lease_gen;
/// Request type definitions.
pub mod types;

/// Error definitions for `xline-client`.
pub mod error;

/// Xline client
#[derive(Clone, Debug)]
pub struct Client {
    /// Kv client
    kv: KvClient,
    /// Lease client
    lease: LeaseClient,
    /// Lock client
    lock: LockClient,
    /// Auth client
    auth: AuthClient,
    /// Maintenance client
    maintenance: MaintenanceClient,
    /// Watch client
    watch: WatchClient,
    /// Cluster client
    cluster: ClusterClient,
    /// Election client
    election: ElectionClient,
}

impl Client {
    /// New `Client`
    ///
    /// # Errors
    ///
    /// If `Self::build_channel` fails.
    #[inline]
    #[allow(clippy::pattern_type_mismatch)] // allow mismatch in map
    #[allow(clippy::as_conversions)] // cast to dyn
    #[allow(trivial_casts)] // same as above
    pub async fn connect<E, S>(
        all_members: S,
        options: ClientOptions,
    ) -> Result<Self, XlineClientBuildError>
    where
        E: AsRef<str>,
        S: IntoIterator<Item = E>,
    {
        let addrs: Vec<_> = all_members
            .into_iter()
            .map(|addr| addr.as_ref().to_owned())
            .collect();
        let channel = Self::build_channel(addrs.clone(), options.tls_config.as_ref()).await?;
        let curp_client = Arc::new(
            CurpClientBuilder::new(options.client_config, false)
                .tls_config(options.tls_config)
                .discover_from(addrs)
                .await?
                .build::<Command>()
                .await?,
        ) as Arc<CurpClient>;
        let id_gen = Arc::new(lease_gen::LeaseIdGenerator::new());

        let token = match options.user {
            Some((username, password)) => {
                let mut tmp_auth = AuthClient::new(Arc::clone(&curp_client), channel.clone(), None);
                let resp = tmp_auth
                    .authenticate(types::auth::AuthenticateRequest::new(username, password))
                    .await
                    .map_err(|err| XlineClientBuildError::AuthError(err.to_string()))?;

                Some(resp.token)
            }
            None => None,
        };

        let kv = KvClient::new(Arc::clone(&curp_client), channel.clone(), token.clone());
        let lease = LeaseClient::new(
            Arc::clone(&curp_client),
            channel.clone(),
            token.clone(),
            Arc::clone(&id_gen),
        );
        let lock = LockClient::new(
            Arc::clone(&curp_client),
            channel.clone(),
            token.clone(),
            id_gen,
        );
        let auth = AuthClient::new(curp_client, channel.clone(), token.clone());
        let maintenance = MaintenanceClient::new(channel.clone(), token.clone());
        let cluster = ClusterClient::new(channel.clone(), token.clone());
        let watch = WatchClient::new(channel, token);
        let election = ElectionClient::new();

        Ok(Self {
            kv,
            lease,
            lock,
            auth,
            maintenance,
            watch,
            cluster,
            election,
        })
    }

    /// Build a tonic load balancing channel.
    async fn build_channel(
        addrs: Vec<String>,
        tls_config: Option<&ClientTlsConfig>,
    ) -> Result<Channel, XlineClientBuildError> {
        let (channel, tx) = Channel::balance_channel(64);

        for addr in addrs {
            let endpoint = build_endpoint(&addr, tls_config)?;
            tx.send(tower::discover::Change::Insert(addr, endpoint))
                .await
                .unwrap_or_else(|_| unreachable!("The channel will not closed"));
        }

        Ok(channel)
    }

    /// Gets a KV client.
    #[inline]
    #[must_use]
    pub fn kv_client(&self) -> KvClient {
        self.kv.clone()
    }

    /// Gets a lease client.
    #[inline]
    #[must_use]
    pub fn lease_client(&self) -> LeaseClient {
        self.lease.clone()
    }

    /// Gets a lock client.
    #[inline]
    #[must_use]
    pub fn lock_client(&self) -> LockClient {
        self.lock.clone()
    }

    /// Gets a auth client.
    #[inline]
    #[must_use]
    pub fn auth_client(&self) -> AuthClient {
        self.auth.clone()
    }

    /// Gets a watch client.
    #[inline]
    #[must_use]
    pub fn watch_client(&self) -> WatchClient {
        self.watch.clone()
    }

    /// Gets a maintenance client.
    #[inline]
    #[must_use]
    pub fn maintenance_client(&self) -> MaintenanceClient {
        self.maintenance.clone()
    }

    /// Gets a cluster client.
    #[inline]
    #[must_use]
    pub fn cluster_client(&self) -> ClusterClient {
        self.cluster.clone()
    }

    /// Gets a election client.
    #[inline]
    #[must_use]
    pub fn election_client(&self) -> ElectionClient {
        self.election.clone()
    }
}

/// Options for a client connection
#[derive(Clone, Debug, Default)]
pub struct ClientOptions {
    /// User is a pair values of name and password
    user: Option<(String, String)>,
    /// Client tls config
    tls_config: Option<ClientTlsConfig>,
    /// config for the curp client
    client_config: ClientConfig,
}

impl ClientOptions {
    /// Create a new `ClientOptions`
    #[inline]
    #[must_use]
    pub fn new(
        user: Option<(String, String)>,
        tls_config: Option<ClientTlsConfig>,
        client_config: ClientConfig,
    ) -> Self {
        Self {
            user,
            tls_config,
            client_config,
        }
    }

    /// Get `user`
    #[inline]
    #[must_use]
    pub fn user(&self) -> Option<(String, String)> {
        self.user.clone()
    }

    /// Get `tls_config`
    #[inline]
    #[must_use]
    pub fn tls_config(&self) -> Option<&ClientTlsConfig> {
        self.tls_config.as_ref()
    }

    /// Get `client_config`
    #[inline]
    #[must_use]
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    /// Set `user`
    #[inline]
    #[must_use]
    pub fn with_user(self, name: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: Some((name.into(), password.into())),
            ..self
        }
    }

    /// Set `client_config`
    #[inline]
    #[must_use]
    pub fn with_client_config(self, client_config: ClientConfig) -> Self {
        Self {
            client_config,
            ..self
        }
    }

    /// Set `tls_config`
    #[inline]
    #[must_use]
    pub fn with_tls_config(self, tls_config: ClientTlsConfig) -> Self {
        Self {
            tls_config: Some(tls_config),
            ..self
        }
    }
}

/// Authentication service.
#[derive(Debug, Clone)]
struct AuthService<S> {
    /// A `Service` trait object
    inner: S,
    /// Auth token
    token: Option<Arc<HeaderValue>>,
}

impl<S> AuthService<S> {
    /// Create a new `AuthService`
    #[inline]
    #[cfg(not(madsim))]
    fn new(inner: S, token: Option<Arc<HeaderValue>>) -> Self {
        Self { inner, token }
    }

    /// Create a new `AuthService`
    #[inline]
    #[cfg(madsim)]
    #[allow(clippy::needless_pass_by_value, clippy::new_ret_no_self)]
    fn new(inner: S, _token: Option<Arc<HeaderValue>>) -> S {
        inner
    }
}

impl<S, Body, Response> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        if let Some(token) = self.token.as_ref() {
            let _: Option<HeaderValue> = request
                .headers_mut()
                .insert(AUTHORIZATION, token.as_ref().clone());
        }

        self.inner.call(request)
    }
}
