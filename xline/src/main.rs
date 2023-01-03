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

    warnings, // treat all wanings as errors

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

use std::{collections::HashMap, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use jsonwebtoken::{DecodingKey, EncodingKey};
use log::debug;
use opentelemetry::{global, runtime::Tokio, sdk::propagation::TraceContextPropagator};
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use tokio::fs;
use tracing::{error, metadata::LevelFilter};
use tracing_subscriber::prelude::*;
use xline::server::XlineServer;

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
    /// Private key uesd to sign the token
    #[clap(long)]
    auth_private_key: Option<PathBuf>,
    /// Public key uesd to verify the token
    #[clap(long)]
    auth_public_key: Option<PathBuf>,
    /// Open jaeger offline
    #[clap(long)]
    jaeger_offline: bool,
    /// ouput dir for jaeger offline
    #[clap(long)]
    jaeger_output_dir: Option<PathBuf>,
    /// Open jaeger online
    #[clap(long)]
    jaeger_online: bool,
    /// Trace level of jaeger
    #[clap(long)]
    jaeger_level: Option<LevelFilter>,
}

/// parse members from string
fn parse_members(s: &str) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    for pair in s.split(',') {
        if let Some((id, addr)) = pair.split_once('=') {
            let _ignore = map.insert(id.to_owned(), addr.to_owned());
        } else {
            return Err("parse members error".to_owned());
        }
    }
    Ok(map)
}

/// init tracing subscriber
fn init_subscriber(
    jaeger_online: bool,
    jaeger_offline: bool,
    jaeger_output_dir: Option<PathBuf>,
    name: &str,
    level: Option<LevelFilter>,
) -> Result<()> {
    let jaeger_online_layer = jaeger_online
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
                .with_filter(level.unwrap_or(LevelFilter::INFO))
        });
    let jaeger_offline_layer = jaeger_offline.then(|| {
        tracing_opentelemetry::layer().with_tracer(
            JaegerJsonExporter::new(
                jaeger_output_dir.unwrap_or_else(|| PathBuf::from("./jaeger_jsons")),
                name.to_owned(),
                name.to_owned(),
                Tokio,
            )
            .install_batch(),
        )
    });
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(jaeger_online_layer)
        .with(jaeger_offline_layer)
        .try_init()?;

    Ok(())
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
    let server_args: ServerArgs = ServerArgs::parse();
    init_subscriber(
        server_args.jaeger_online,
        server_args.jaeger_offline,
        server_args.jaeger_output_dir,
        &server_args.name,
        server_args.jaeger_level,
    )?;
    debug!("name = {:?}", server_args.name);
    let self_addr = server_args
        .members
        .get(&server_args.name)
        .unwrap_or_else(|| panic!("node name {} not found in cluster peers", server_args.name))
        .parse()?;
    let key_pair = read_key_pair(server_args.auth_private_key, server_args.auth_public_key).await;
    let server = XlineServer::new(
        server_args.name,
        server_args.members,
        server_args.is_leader,
        key_pair,
    )
    .await;
    debug!("{:?}", server);
    server.start(self_addr).await?;
    global::shutdown_tracer_provider();
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_parse_members() -> Result<(), String> {
        let s1 = "";
        assert!(parse_members(s1).is_err());

        let s2 = "a=1";
        let m2 = HashMap::from_iter(vec![("a".to_owned(), "1".to_owned())]);
        assert_eq!(parse_members(s2)?, m2);

        let s3 = "a=1,b=2,c=3";
        let m3 = HashMap::from_iter(vec![
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "2".to_owned()),
            ("c".to_owned(), "3".to_owned()),
        ]);
        assert_eq!(parse_members(s3)?, m3);

        let s4 = "abcde";
        assert!(parse_members(s4).is_err());

        Ok(())
    }
}
