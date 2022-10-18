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

use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use log::debug;

use xline::server::XlineServer;

/// Command line arguments
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ServerArgs {
    /// Node name
    #[clap(long)]
    name: String,
    /// Cluster peers. eg: 192.168.x.x:8080 192.168.x.x:8080
    #[clap(long, multiple = true, required = true)]
    cluster_peers: Vec<SocketAddr>,
    /// If node is leader
    #[clap(long)]
    is_leader: bool,
    /// Leader's ip and port. eg: 192.168.x.x:8080
    #[clap(long)]
    leader_ip_port: SocketAddr,
    /// Current node ip and port. eg: 192.168.x.x:8080
    #[clap(long)]
    self_ip_port: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let server_args = ServerArgs::parse();
    debug!("name = {:?}", server_args.name);
    debug!("server_addr = {:?}", server_args.self_ip_port);
    debug!("cluster_peers = {:?}", server_args.cluster_peers);
    let server = XlineServer::new(
        server_args.name,
        server_args.cluster_peers,
        server_args.is_leader,
        server_args.leader_ip_port,
        server_args.self_ip_port,
    )
    .await;
    debug!("{:?}", server);
    server.start(server_args.self_ip_port).await?;
    Ok(())
}
