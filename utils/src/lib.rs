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
#![allow(
    clippy::multiple_crate_versions, // caused by the dependency, can't be fixed
)]
// When we use rust version 1.65 or later, refactor this with GAT

use std::{collections::HashMap, time::Duration};

use clippy_utilities::OverflowArithmetic;
use config::InitialClusterState;
use thiserror::Error;

use crate::config::{ClusterRange, LevelConfig, RotationConfig};

/// configuration
pub mod config;
/// utils of `parking_lot` lock
#[cfg(feature = "parking_lot")]
pub mod parking_lot_lock;
/// utils of `shutdown`
pub mod shutdown;
/// utils of `std` lock
#[cfg(feature = "std")]
pub mod std_lock;
/// utils of `tokio` lock
#[cfg(feature = "tokio")]
pub mod tokio_lock;
/// utils for pass span context
pub mod tracing;

/// seconds per minute
const SECS_PER_MINUTE: u64 = 60;
/// seconds per hour
const SECS_PER_HOUR: u64 = 3600;
/// seconds per day, equals to 24 * 60 * 60 = 86400
const SECS_PER_DAY: u64 = 86400;

/// Config Parse Error
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigParseError {
    /// Invalid number when parsing `Duration`
    #[error("Invalid Number: {0}")]
    InvalidNumber(#[from] std::num::ParseIntError),
    /// Invalid time unit
    #[error("Invalid Unit: {0}")]
    InvalidUnit(String),
    /// Invalid values
    #[error("Invalid Value: {0}")]
    InvalidValue(String),
}

/// Config File Error
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigFileError {
    /// Invalid number when parsing `Duration`
    #[error("Couldn't read config file {0}")]
    FileError(String, #[source] std::io::Error),
}

/// parse members from string like "node1=addr1,addr2,node2=add3,addr4,addr5,node3=addr6"
/// # Errors
/// Return error when pass wrong args
#[inline]
pub fn parse_members(s: &str) -> Result<HashMap<String, Vec<String>>, ConfigParseError> {
    let mut map = HashMap::new();
    let mut last_node = "";
    for item in s.split(',') {
        let terms = item.split('=').collect::<Vec<_>>();
        if terms.iter().any(|term| term.is_empty()) {
            return Err(ConfigParseError::InvalidValue(
                "parse members error".to_owned(),
            ));
        }
        #[allow(clippy::indexing_slicing)] // that is safe to index slice after checking the length
        if terms.len() == 2 {
            last_node = terms[0];
            let _ignore = map.insert(last_node.to_owned(), vec![terms[1].to_owned()]);
        } else if terms.len() == 1 {
            map.get_mut(last_node)
                .ok_or_else(|| ConfigParseError::InvalidValue("parse members error".to_owned()))?
                .push(terms[0].to_owned());
        } else {
            unreachable!("terms length should be 1 or 2, terms: {terms:?}");
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
    let s = s.to_lowercase();
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
    } else if s.ends_with('m') {
        if let Some(dur) = s.strip_suffix('m') {
            let minutes: u64 = dur.parse()?;
            Ok(Duration::from_secs(minutes.overflow_mul(SECS_PER_MINUTE)))
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of time should not be empty ({s})"
            )))
        }
    } else if s.ends_with('h') {
        if let Some(dur) = s.strip_suffix('h') {
            let hours: u64 = dur.parse()?;
            Ok(Duration::from_secs(hours.overflow_mul(SECS_PER_HOUR)))
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of time should not be empty ({s})"
            )))
        }
    } else if s.ends_with('d') {
        if let Some(dur) = s.strip_suffix('d') {
            let days: u64 = dur.parse()?;
            Ok(Duration::from_secs(days.overflow_mul(SECS_PER_DAY)))
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of time should not be empty ({s})"
            )))
        }
    } else {
        Err(ConfigParseError::InvalidUnit(format!(
            "the unit of time should be one of 'us', 'ms', 's', 'm', 'h' or 'd' ({s})"
        )))
    }
}

/// Parse `InitialClusterState` from string
/// # Errors
/// Return error when parsing the given string to `InitialClusterState` failed
#[inline]
pub fn parse_state(s: &str) -> Result<InitialClusterState, ConfigParseError> {
    match s {
        "new" => Ok(InitialClusterState::New),
        "existing" => Ok(InitialClusterState::Existing),
        _ => Err(ConfigParseError::InvalidValue(format!(
            "the initial cluster state should be one of 'new' or 'existing' ({s})"
        ))),
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

/// Parse bytes from string
/// # Errors
/// Return error when parsing the given string to usize failed
#[inline]
#[allow(clippy::integer_arithmetic)]
pub fn parse_batch_bytes(s: &str) -> Result<u64, ConfigParseError> {
    let s = s.to_lowercase();
    if s.ends_with("kb") {
        if let Some(value) = s.strip_suffix("kb") {
            Ok(value.parse::<u64>()? * 1024)
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of size should not be empty. ({s})"
            )))
        }
    } else if s.ends_with("mb") {
        if let Some(value) = s.strip_suffix("mb") {
            let bytes = value.parse::<u64>()? * 1024 * 1024;
            if bytes >= 4 * 1024 * 1024 {
                Err(ConfigParseError::InvalidValue(format!(
                    "the batch size should be smaller than 4MB. ({s})"
                )))
            } else {
                Ok(bytes)
            }
        } else {
            Err(ConfigParseError::InvalidValue(format!(
                "the value of size should not be empty ({s})"
            )))
        }
    } else {
        Err(ConfigParseError::InvalidUnit(format!(
            "the unit of size should be one of 'kb' or 'mb'({s})"
        )))
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
        assert_eq!(parse_duration("3m").unwrap(), Duration::from_secs(180));
        assert_eq!(
            parse_duration("2h").unwrap(),
            Duration::from_secs(2 * SECS_PER_HOUR)
        );
        assert_eq!(
            parse_duration("30d").unwrap(),
            Duration::from_secs(30 * SECS_PER_DAY)
        );
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
        let m2 = HashMap::from([("a".to_owned(), vec!["1".to_owned()])]);
        assert_eq!(parse_members(s2).unwrap(), m2);

        let s3 = "a=1,b=2,c=3";
        let m3 = HashMap::from([
            ("a".to_owned(), vec!["1".to_owned()]),
            ("b".to_owned(), vec!["2".to_owned()]),
            ("c".to_owned(), vec!["3".to_owned()]),
        ]);
        assert_eq!(parse_members(s3).unwrap(), m3);

        let s4 = "abcde";
        assert!(parse_members(s4).is_err());

        let s5 = "a=1,2,3,b=1,2,c=1";
        let m5 = HashMap::from([
            (
                "a".to_owned(),
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
            ),
            ("b".to_owned(), vec!["1".to_owned(), "2".to_owned()]),
            ("c".to_owned(), vec!["1".to_owned()]),
        ]);
        assert_eq!(parse_members(s5).unwrap(), m5);

        let s6 = "a=1,";
        assert!(parse_members(s6).is_err());

        let s7 = "=1,2,b=3";
        assert!(parse_members(s7).is_err());

        let s8 = "1,2,b=3";
        assert!(parse_members(s8).is_err());
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

    #[allow(clippy::unwrap_used)]
    #[test]
    fn test_parse_batch_size() {
        assert_eq!(parse_batch_bytes("10kb").unwrap(), 10 * 1024);
        assert_eq!(parse_batch_bytes("2MB").unwrap(), 2 * 1024 * 1024);
        assert!(parse_batch_bytes("10MB").is_err());
        assert!(parse_batch_bytes("10Gb").is_err());
        assert!(parse_batch_bytes("kb").is_err());
        assert!(parse_batch_bytes("MB").is_err());
    }
}
