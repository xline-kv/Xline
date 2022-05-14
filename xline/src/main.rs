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
    clippy::restriction,
    clippy::pedantic,
    clippy::cargo
)]
#![allow(
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::panic, // allow debug_assert, panic in production code
    clippy::shadow_same, // TODO: False alarm for async function
)]

use std::net::{AddrParseError, SocketAddr};

use anyhow::Result;
use clap::Parser;
use log::debug;

/// rpc definition module
mod rpc;
/// Xline server
mod server;
/// Storage module
mod storage;
use server::XlineServer;

/// Command line arguments
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ServerArgs {
    /// Node name
    #[clap(long)]
    name: String,
    /// Cluster members including current node. eg: node1=192.168.x.x:8080;node2=192.168.x.x:8080
    #[clap(long)]
    cluster_members: String,
    /// Current node ip and port. eg: 192.168.x.x:8080
    #[clap(long, parse(try_from_str=parse_ip_port))]
    ip_port: SocketAddr,
}

/// Parse server address
fn parse_ip_port(ip_port: &str) -> Result<SocketAddr, String> {
    ip_port
        .parse()
        .map_err(|e| format!("failed to parse {:?}, error is {:?}", ip_port, e))
}

/// Parse member list
fn parse_members(member_list: &str) -> Result<Vec<SocketAddr>, AddrParseError> {
    member_list.split(';').map(str::parse).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let server_args = ServerArgs::parse();
    debug!("name = {:?}", server_args.name);
    debug!("server_addr = {:?}", server_args.ip_port);
    let members = parse_members(&server_args.cluster_members)
        .unwrap_or_else(|e| panic!("Failed to parse member list, error is {:?}", e));
    debug!("cluster_members = {:?}", members);
    let server = XlineServer::new(server_args.name, server_args.ip_port, members);
    debug!("{:?}", server);
    server.start().await?;
    Ok(())
}
