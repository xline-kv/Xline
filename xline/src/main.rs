//! Xline
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
)]
#![allow(
    clippy::panic, // allow debug_assert, panic in production code
    clippy::multiple_crate_versions, // caused by the dependency, can't be fixed
)]

use std::{collections::HashMap, env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use clap::Parser;
use engine::rocksdb_engine::RocksEngine;
use jsonwebtoken::{DecodingKey, EncodingKey};
use opentelemetry::{global, runtime::Tokio, sdk::propagation::TraceContextPropagator};
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use tokio::fs;
use tracing::{debug, error};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt::format, prelude::*};
use utils::{
    config::{
        default_candidate_timeout_ticks, default_follower_timeout_ticks, file_appender, AuthConfig,
        ClientTimeout, ClusterConfig, LevelConfig, LogConfig, RotationConfig, ServerTimeout,
        TraceConfig, XlineServerConfig,
    },
    parse_duration, parse_log_level, parse_members, parse_rotation,
};
use xline::{
    server::XlineServer,
    storage::db::{DB, XLINETABLES},
};

/// Command line arguments
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ServerArgs {
    /// Node name
    #[clap(long)]
    name: String,
    /// Cluster peers. eg: 192.168.x.x:8080 192.168.x.x:8080
    #[clap(long,value_parser = parse_members)]
    members: HashMap<String, String>,
    /// If node is leader
    #[clap(long)]
    is_leader: bool,
    /// Private key used to sign the token
    #[clap(long)]
    auth_private_key: Option<PathBuf>,
    /// Public key used to verify the token
    #[clap(long)]
    auth_public_key: Option<PathBuf>,
    /// Open jaeger offline
    #[clap(long)]
    jaeger_offline: bool,
    /// output dir for jaeger offline
    #[clap(long, default_value = "./jaeger_jsons")]
    jaeger_output_dir: PathBuf,
    /// Open jaeger online
    #[clap(long)]
    jaeger_online: bool,
    /// Trace level of jaeger
    #[clap(long, value_parser = parse_log_level, default_value = "info")]
    jaeger_level: LevelConfig,
    /// Log file path
    #[clap(long, default_value = "/var/log/xline")]
    log_file: PathBuf,
    /// Log rotate strategy, eg: never, hourly, daily
    #[clap(long, value_parser = parse_rotation, default_value = "daily")]
    log_rotate: RotationConfig,
    /// Log verbosity level, eg: trace, debug, info, warn, error
    #[clap(long, value_parser = parse_log_level, default_value = "info")]
    log_level: LevelConfig,
    /// Heartbeat interval between curp server nodes
    #[clap(long, value_parser = parse_duration, default_value = "300ms")]
    heartbeat_interval: Duration,
    /// Curp wait sync timeout
    #[clap(long, value_parser = parse_duration, default_value = "5s")]
    server_wait_synced_timeout: Duration,
    /// Curp propose retry timeout
    #[clap(long, value_parser = parse_duration, default_value = "800ms")]
    retry_timeout: Duration,
    /// Curp rpc timeout
    #[clap(long, value_parser = parse_duration, default_value = "50ms")]
    rpc_timeout: Duration,
    /// Follower election timeout ticks
    #[clap(long, default_value_t = default_follower_timeout_ticks())]
    follower_timeout_ticks: u8,
    /// Candidate election timeout ticks
    #[clap(long, default_value_t = default_candidate_timeout_ticks())]
    candidate_timeout_ticks: u8,
    /// Curp client timeout
    #[clap(long, value_parser = parse_duration, default_value = "1s")]
    client_timeout: Duration,
    /// Curp client wait synced timeout
    #[clap(long, value_parser = parse_duration, default_value = "2s")]
    client_wait_synced_timeout: Duration,
    /// Propose request timeout
    #[clap(long, value_parser = parse_duration, default_value = "1s")]
    client_propose_timeout: Duration,
    /// Curp client retry timeout
    #[clap(long, value_parser = parse_duration, default_value = "50ms")]
    client_retry_timeout: Duration,
}

impl From<ServerArgs> for XlineServerConfig {
    fn from(args: ServerArgs) -> Self {
        let server_timeout = ServerTimeout::new(
            args.heartbeat_interval,
            args.server_wait_synced_timeout,
            args.retry_timeout,
            args.rpc_timeout,
            args.follower_timeout_ticks,
            args.candidate_timeout_ticks,
        );
        let client_timeout = ClientTimeout::new(
            args.client_wait_synced_timeout,
            args.client_propose_timeout,
            args.client_retry_timeout,
        );
        let cluster = ClusterConfig::new(
            args.name,
            args.members,
            args.is_leader,
            server_timeout,
            client_timeout,
        );
        let log = LogConfig::new(args.log_file, args.log_rotate, args.log_level);
        let trace = TraceConfig::new(
            args.jaeger_online,
            args.jaeger_offline,
            args.jaeger_output_dir,
            args.jaeger_level,
        );
        let auth = AuthConfig::new(args.auth_public_key, args.auth_private_key);
        XlineServerConfig::new(cluster, log, trace, auth)
    }
}

/// init tracing subscriber
fn init_subscriber(
    name: &str,
    log_config: &LogConfig,
    trace_config: &TraceConfig,
) -> Result<WorkerGuard> {
    let file_appender = file_appender(*log_config.rotation(), log_config.path(), name);

    // `WorkerGuard` should be assigned in the `main` function or whatever the entrypoint of the program is.
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let log_file_layer = tracing_subscriber::fmt::layer()
        .event_format(format().compact())
        .with_writer(non_blocking)
        .with_filter(*log_config.level());

    let jaeger_level = *trace_config.jaeger_level();
    let jaeger_online_layer = trace_config
        .jaeger_online()
        .then(|| {
            opentelemetry_jaeger::new_agent_pipeline()
                .with_service_name(name)
                .install_batch(Tokio)
                .ok()
        })
        .flatten()
        .map(|tracer| {
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(jaeger_level)
        });
    let jaeger_offline_layer = trace_config.jaeger_offline().then(|| {
        tracing_opentelemetry::layer().with_tracer(
            JaegerJsonExporter::new(
                trace_config.jaeger_output_dir().clone(),
                name.to_owned(),
                name.to_owned(),
                Tokio,
            )
            .install_batch(),
        )
    });

    let jaeger_fmt_layer = tracing_subscriber::fmt::layer()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());

    tracing_subscriber::registry()
        .with(log_file_layer)
        .with(jaeger_fmt_layer)
        .with(jaeger_online_layer)
        .with(jaeger_offline_layer)
        .try_init()?;
    Ok(guard)
}

/// Read key pair from file
async fn read_key_pair(
    private_key_path: Option<PathBuf>,
    public_key_path: Option<PathBuf>,
) -> Option<(EncodingKey, DecodingKey)> {
    let encoding_key = match fs::read(private_key_path?).await {
        Ok(key) => match EncodingKey::from_rsa_pem(&key) {
            Ok(key) => key,
            Err(e) => {
                error!("parse private key failed: {:?}", e);
                return None;
            }
        },
        Err(e) => {
            error!("read private key failed: {:?}", e);
            return None;
        }
    };
    let decoding_key = match fs::read(public_key_path?).await {
        Ok(key) => match DecodingKey::from_rsa_pem(&key) {
            Ok(key) => key,
            Err(e) => {
                error!("parse public key failed: {:?}", e);
                return None;
            }
        },
        Err(e) => {
            error!("read public key failed: {:?}", e);
            return None;
        }
    };
    Some((encoding_key, decoding_key))
}

#[tokio::main]
async fn main() -> Result<()> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let config: XlineServerConfig = if env::args_os().len() == 1 {
        let path =
            env::var("XLINE_SERVER_CONFIG").unwrap_or_else(|_| "/etc/xline_server.conf".to_owned());
        let config_file = fs::read_to_string(&path).await?;
        toml::from_str(&config_file)?
    } else {
        let server_args: ServerArgs = ServerArgs::parse();
        server_args.into()
    };

    let log_config = config.log();
    let trace_config = config.trace();
    let cluster_config = config.cluster();
    let auth_config = config.auth();

    let _guard = init_subscriber(cluster_config.name(), log_config, trace_config)?;

    let key_pair = read_key_pair(
        auth_config.auth_private_key().clone(),
        auth_config.auth_public_key().clone(),
    )
    .await;

    let self_addr = cluster_config
        .members()
        .get(cluster_config.name())
        .ok_or_else(|| {
            anyhow!(
                "node name {} not found in cluster peers",
                cluster_config.name()
            )
        })?
        .parse()?;

    let is_leader = cluster_config.is_leader();
    debug!("name = {:?}", cluster_config.name());
    debug!("server_addr = {:?}", self_addr);
    debug!("cluster_peers = {:?}", cluster_config.members());
    let mem_engine = RocksEngine::new("/tmp/xline/xlinedb", &XLINETABLES)?;
    let server = XlineServer::new(
        cluster_config.name().clone(),
        cluster_config.members().clone(),
        *is_leader,
        key_pair,
        *cluster_config.server_timeout(),
        *cluster_config.client_timeout(),
        Arc::new(DB::new(mem_engine)),
    )
    .await;
    debug!("{:?}", server);
    server.start(self_addr).await?;
    global::shutdown_tracer_provider();
    Ok(())
}
