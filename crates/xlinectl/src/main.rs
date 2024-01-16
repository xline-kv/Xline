//! xlinectl
#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html

    absolute_paths_not_starting_with_crate,
    // box_pointers, async trait must use it
    elided_lifetimes_in_paths,
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
    clippy::panic,
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
    clippy::expect_used, // allow panic on invalid inputs
    clippy::print_stderr, // allow in command line tool
    clippy::print_stdout, // allow in command line tool
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

extern crate utils as ext_utils;

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use clap::{arg, value_parser, Command};
use command::compaction;
use ext_utils::config::ClientConfig;
use tokio::fs;
use tonic::transport::{Certificate, ClientTlsConfig};
use xline_client::{Client, ClientOptions};

use crate::{
    command::{auth, delete, get, lease, lock, member, put, role, snapshot, txn, user, watch},
    utils::{
        parser::parse_user,
        printer::{set_printer_type, PrinterType},
    },
};

/// Command definitions and parsers
mod command;
/// Utils
mod utils;

/// Global heading string
const GLOBAL_HEADING: &str = "Global Options";

/// The top level cli command
fn cli() -> Command {
    Command::new("xlinectl")
        .about("A command line client for Xline")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .arg(
            arg!(--endpoints <"SERVER_NAME ADDR"> "Xline endpoints, using the format of [addr0, addr1, ...]")
                .num_args(1..)
                .default_values(["127.0.0.1:2379"])
                .value_delimiter(',')
                .global(true)
                .help_heading(GLOBAL_HEADING),
        )
        .arg(
            arg!(--user <"USERNAME[:PASSWD]"> "The name of the user, this provide a shorthand to set password")
                .global(true)
                .help_heading(GLOBAL_HEADING),
        )
        .arg(
            arg!(--password <"PASSWD"> "The password of the user, should exist if password not set in `--user`")
                .global(true)
                .help_heading(GLOBAL_HEADING),
        )
        .arg(arg!(--wait_synced_timeout <TIMEOUT> "The timeout for Curp client waiting synced(in secs)")
            .global(true)
            .help_heading(GLOBAL_HEADING)
            .value_parser(value_parser!(u64))
            .default_value("2"))
        .arg(arg!(--propose_timeout <TIMEOUT> "The timeout for Curp client proposing request(in secs)")
            .global(true)
            .help_heading(GLOBAL_HEADING)
            .value_parser(value_parser!(u64))
            .default_value("1"))
        .arg(arg!(--initial_retry_timeout <TIMEOUT> "The initial timeout for Curp client retry interval(in millis)")
            .global(true)
            .help_heading(GLOBAL_HEADING)
            .value_parser(value_parser!(u64))
            .default_value("50"))
        .arg(arg!(--max_retry_timeout <TIMEOUT> "The max retry timeout(used in the exponential backoff algorithm) for Curp client retry interval(in millis)")
            .global(true)
            .help_heading(GLOBAL_HEADING)
            .value_parser(value_parser!(u64))
            .default_value("10000"))
        .arg(arg!(--retry_count <COUNT> "The count of Curp client retry times")
            .global(true)
            .help_heading(GLOBAL_HEADING)
            .value_parser(value_parser!(usize))
            .default_value("3"))
        .arg(arg!(--keep_alive_interval <INTERVAL> "The interval used to keep client id alive")
            .global(true)
            .help_heading(GLOBAL_HEADING)
            .value_parser(value_parser!(u64))
            .default_value("1000"))
        .arg(arg!(--printer_type <TYPE> "The format of the result that will be printed")
            .global(true)
            .help_heading(GLOBAL_HEADING)
            .value_parser(["SIMPLE", "FIELD", "JSON"])
            .default_value("SIMPLE"))
        .arg(arg!(--ca_cert_pem_path <PATH> "The path of the CA certificate PEM file")
            .global(true)
            .value_parser(value_parser!(PathBuf))
            .help_heading(GLOBAL_HEADING))

        .subcommand(get::command())
        .subcommand(put::command())
        .subcommand(delete::command())
        .subcommand(lease::command())
        .subcommand(snapshot::command())
        .subcommand(auth::command())
        .subcommand(user::command())
        .subcommand(role::command())
        .subcommand(txn::command())
        .subcommand(compaction::command())
        .subcommand(watch::command())
        .subcommand(lock::command())
        .subcommand(member::command())
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = cli().get_matches();
    let user_opt = parse_user(&matches)?;
    let endpoints = matches.get_many::<String>("endpoints").expect("Required");
    let client_config = ClientConfig::new(
        Duration::from_secs(*matches.get_one("wait_synced_timeout").expect("Required")),
        Duration::from_secs(*matches.get_one("propose_timeout").expect("Required")),
        Duration::from_millis(*matches.get_one("initial_retry_timeout").expect("Required")),
        Duration::from_millis(*matches.get_one("max_retry_timeout").expect("Required")),
        *matches.get_one("retry_count").expect("Required"),
        true,
        Duration::from_millis(*matches.get_one("keep_alive_interval").expect("Required")),
    );
    let ca_path: Option<PathBuf> = matches.get_one("ca_cert_pem_path").cloned();
    let tls_config = match ca_path {
        Some(path) => {
            let ca = Certificate::from_pem(fs::read_to_string(path).await?);
            let tls_config = ClientTlsConfig::new().ca_certificate(ca);
            Some(tls_config)
        }
        None => None,
    };
    let options = ClientOptions::new(user_opt, tls_config, client_config);
    let printer_type = match matches
        .get_one::<String>("printer_type")
        .expect("Required")
        .as_str()
    {
        "SIMPLE" => PrinterType::Simple,
        "FIELD" => PrinterType::Field,
        "JSON" => PrinterType::Json,
        _ => unreachable!("already checked by clap"),
    };
    set_printer_type(printer_type);

    let mut client = Client::connect(endpoints, options).await?;
    handle_matches!(matches, client, { get, put, delete, txn, compaction, lease, snapshot, auth, user, role, watch, lock, member });

    Ok(())
}
