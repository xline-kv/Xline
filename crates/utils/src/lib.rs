//! `utils`
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

    warnings, // treat all warns as errors

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
)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        unused_results,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::as_conversions,
        clippy::shadow_unrelated,
        clippy::arithmetic_side_effects,
        clippy::let_underscore_untyped,
        clippy::too_many_lines,
    )
)]

use std::str::FromStr;

#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;

/// Fake `ClientTlsConfig` for `madsim`
#[cfg(madsim)]
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // Same as real config
#[non_exhaustive]
pub struct ClientTlsConfig;

/// Fake `ServerTlsConfig` for `madsim`
#[cfg(madsim)]
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)] // Same as real config
#[non_exhaustive]
pub struct ServerTlsConfig;

/// configuration
pub mod config;
/// Interval tree implementation
pub mod interval_map;
/// utils for metrics
pub mod metrics;
/// utils of `parking_lot` lock
#[cfg(feature = "parking_lot")]
pub mod parking_lot_lock;
/// utils for parse config
pub mod parser;
/// utils of `std` lock
#[cfg(feature = "std")]
pub mod std_lock;
/// table names
pub mod table_names;
/// task manager
pub mod task_manager;
/// utils of `tokio` lock
#[cfg(feature = "tokio")]
pub mod tokio_lock;
/// utils for pass span context
pub mod tracing;

use ::tracing::debug;
pub use parser::*;
use pbkdf2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Params, Pbkdf2,
};

/// display all elements for the given vector
#[macro_export]
macro_rules! write_vec {
    ($f:expr, $name:expr, $vector:expr) => {
        write!($f, "{}: [ ", { $name })?;
        let last_idx = if $vector.is_empty() {
            0
        } else {
            $vector.len() - 1
        };
        for (idx, element) in $vector.iter().enumerate() {
            write!($f, "{}", element)?;
            if idx != last_idx {
                write!($f, ",")?;
            }
        }
        write!($f, "]")?;
    };
}

/// Get current timestamp in seconds
#[must_use]
#[inline]
pub fn timestamp() -> u64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| unreachable!("Time went backwards"))
        .as_secs()
}

/// Create a new endpoint from addr
/// # Errors
/// Return error if addr or tls config is invalid
#[inline]
#[allow(unused_variables)]
pub fn build_endpoint(
    addr: &str,
    tls_config: Option<&ClientTlsConfig>,
) -> Result<Endpoint, tonic::transport::Error> {
    debug!(
        "connect to {addr}{}",
        if tls_config.is_some() {
            " with tls_config"
        } else {
            ""
        }
    );
    let scheme_str = addr.split_once("://").map(|(scheme, _)| scheme);
    let endpoint = match scheme_str {
        Some(_scheme) => Endpoint::from_str(addr)?,
        None => Endpoint::from_shared(format!("http://{addr}"))?,
    };
    #[cfg(not(madsim))]
    match scheme_str {
        Some("http") | None => {}
        Some("https") => {
            let tls_config = tls_config.cloned().unwrap_or_default();
            return endpoint.tls_config(tls_config);
        }
        _ => {
            if let Some(tls_config) = tls_config {
                return endpoint.tls_config(tls_config.clone());
            }
        }
    };
    Ok(endpoint)
}

/// Hash password
///
/// # Errors
///
/// return `Error` when hash password failed
#[inline]
pub fn hash_password(password: &[u8]) -> Result<String, pbkdf2::password_hash::errors::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let simple_para = Params {
        // The recommended rounds is 600,000 or more
        // [OWASP cheat sheet]: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
        rounds: 200_000,
        output_length: 32,
    };
    let hashed_password =
        Pbkdf2.hash_password_customized(password, None, None, simple_para, &salt)?;
    Ok(hashed_password.to_string())
}
