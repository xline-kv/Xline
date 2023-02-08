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
)]
#![allow(
    clippy::multiple_crate_versions, // caused by the dependency, can't be fixed
)]
// When we use rust version 1.65 or later, refactor this with GAT

use std::{collections::HashMap, time::Duration};

use thiserror::Error;

use crate::config::{ClusterRange, LevelConfig, RotationConfig};

/// configuration
pub mod config;
/// utils of `parking_lot` lock
#[cfg(feature = "parking_lot")]
pub mod parking_lot_lock;
/// utils of `std` lock
#[cfg(feature = "std")]
pub mod std_lock;
/// utils of `tokio` lock
#[cfg(feature = "tokio")]
pub mod tokio_lock;
/// utils for pass span context
pub mod tracing;

/// Config Parse Error
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigParseError {
    /// Invalid number when parsing `Duration`
    #[error("Invalid Value: {0}")]
    InvalidNumber(#[from] std::num::ParseIntError),
    /// Invalid time unit
    #[error("Invalid Unit: {0}")]
    InvalidUnit(String),
    /// Invalid values
    #[error("Invalid Value: {0}")]
    InvalidValue(String),
}

/// parse members from string
/// # Errors
/// Return error when pass wrong args
#[inline]
pub fn parse_members(s: &str) -> Result<HashMap<String, String>, ConfigParseError> {
    let mut map = HashMap::new();
    for pair in s.split(',') {
        if let Some((id, addr)) = pair.split_once('=') {
            let _ignore = map.insert(id.to_owned(), addr.to_owned());
        } else {
            return Err(ConfigParseError::InvalidValue(
                "parse members error".to_owned(),
            ));
        }
    }
    Ok(map)
}

/// Parse `ClusterRange` from the given string
/// # Errors
/// Return error when parsing the given string to `ClusterRange` failed
#[inline]
pub fn parse_range(s: &str) -> Result<ClusterRange, ConfigParseError> {
    if let Some((start, end)) = s.split_once("..") {
        Ok(ClusterRange {
            start: start.parse::<u64>()?,
            end: end.parse::<u64>()?,
        })
    } else {
        Err(ConfigParseError::InvalidValue(format!(
            "Invalid cluster range:{s}"
        )))
    }
}

/// Parse `Duration` from string
/// # Errors
/// Return error when parsing the given string to `Duration` failed
#[inline]
pub fn parse_duration(s: &str) -> Result<Duration, ConfigParseError> {
    if s.ends_with("us") {
        if let Some(dur) = s.strip_suffix("us") {
            Ok(Duration::from_micros(dur.parse()?))
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of time should not be empty. ({s})"
            )))
        }
    } else if s.ends_with("ms") {
        if let Some(dur) = s.strip_suffix("ms") {
            Ok(Duration::from_millis(dur.parse()?))
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of time should not be empty ({s})"
            )))
        }
    } else if s.ends_with('s') {
        if let Some(dur) = s.strip_suffix('s') {
            Ok(Duration::from_secs(dur.parse()?))
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of time should not be empty ({s})"
            )))
        }
    } else {
        Err(ConfigParseError::InvalidUnit(format!(
            "the unit of time should be one of 'us', 'ms' or 's'({s})"
        )))
    }
}

/// Parse `LevelConfig` from string
/// # Errors
/// Return error when parsing the given string to `LevelConfig` failed
#[inline]
pub fn parse_log_level(s: &str) -> Result<LevelConfig, ConfigParseError> {
    match s {
        "trace" => Ok(LevelConfig::TRACE),
        "debug" => Ok(LevelConfig::DEBUG),
        "info" => Ok(LevelConfig::INFO),
        "warn" => Ok(LevelConfig::WARN),
        "error" => Ok(LevelConfig::ERROR),
        _ => Err(ConfigParseError::InvalidValue(format!(
            "the log level should be one of 'trace', 'debug', 'info', 'warn' or 'error' ({s})"
        ))),
    }
}

/// Parse `RotationConfig` from string
/// # Errors
/// Return error when parsing the given string to `RotationConfig` failed
#[inline]
pub fn parse_rotation(s: &str) -> Result<RotationConfig, ConfigParseError> {
    match s {
        "hourly" => Ok(RotationConfig::Hourly),
        "daily" => Ok(RotationConfig::Daily),
        "never" => Ok(RotationConfig::Never),
        _ => Err(ConfigParseError::InvalidValue(format!(
            "the rotation config should be one of 'hourly', 'daily' or 'never' ({s})"
        ))),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("3ms").unwrap(), Duration::from_millis(3));
        assert_eq!(parse_duration("1us").unwrap(), Duration::from_micros(1));
        let results = vec![
            parse_duration("hello world"),
            parse_duration("5x"),
            parse_duration("helloms"),
        ];

        for res in results {
            assert!(res.is_err());
        }
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range("1000..2000").unwrap(), 1000..2000);
        assert!(parse_range("5,,10").is_err());
        assert!(parse_range("a..b").is_err());
        assert!(parse_range("6c..10a").is_err());
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_parse_members() {
        let s1 = "";
        assert!(parse_members(s1).is_err());

        let s2 = "a=1";
        let m2 = HashMap::from_iter(vec![("a".to_owned(), "1".to_owned())]);
        assert_eq!(parse_members(s2).unwrap(), m2);

        let s3 = "a=1,b=2,c=3";
        let m3 = HashMap::from_iter(vec![
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "2".to_owned()),
            ("c".to_owned(), "3".to_owned()),
        ]);
        assert_eq!(parse_members(s3).unwrap(), m3);

        let s4 = "abcde";
        assert!(parse_members(s4).is_err());
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_parse_log_level() {
        assert_eq!(parse_log_level("trace").unwrap(), LevelConfig::TRACE);
        assert_eq!(parse_log_level("debug").unwrap(), LevelConfig::DEBUG);
        assert_eq!(parse_log_level("info").unwrap(), LevelConfig::INFO);
        assert_eq!(parse_log_level("warn").unwrap(), LevelConfig::WARN);
        assert_eq!(parse_log_level("error").unwrap(), LevelConfig::ERROR);
        let res = parse_log_level("hello world");
        assert!(res.is_err());
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_parse_rotation() {
        assert_eq!(parse_rotation("daily").unwrap(), RotationConfig::Daily);
        assert_eq!(parse_rotation("hourly").unwrap(), RotationConfig::Hourly);
        assert_eq!(parse_rotation("never").unwrap(), RotationConfig::Never);
        let res = parse_rotation("hello world");
        assert!(res.is_err());
    }
}
