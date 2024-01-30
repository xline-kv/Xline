use std::{collections::HashMap, time::Duration};

use clippy_utilities::OverflowArithmetic;
use thiserror::Error;

use crate::config::{
    ClusterRange, InitialClusterState, LevelConfig, MetricsPushProtocol, RotationConfig,
};

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
#[allow(clippy::arithmetic_side_effects)]
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

/// Get the metrics push protocol
/// # Errors
/// Return error when parsing the given string to `MetricsPushProtocol` failed
#[inline]
pub fn parse_metrics_push_protocol(s: &str) -> Result<MetricsPushProtocol, ConfigParseError> {
    match s {
        "http" => Ok(MetricsPushProtocol::HTTP),
        "grpc" => Ok(MetricsPushProtocol::GRPC),
        _ => Err(ConfigParseError::InvalidValue(format!(
            "the metrics push protocol should be one of 'http' or 'grpc' ({s})"
        ))),
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

    #[test]
    fn test_parse_rotation() {
        assert_eq!(parse_rotation("daily").unwrap(), RotationConfig::Daily);
        assert_eq!(parse_rotation("hourly").unwrap(), RotationConfig::Hourly);
        assert_eq!(parse_rotation("never").unwrap(), RotationConfig::Never);
        let res = parse_rotation("hello world");
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_batch_size() {
        assert_eq!(parse_batch_bytes("10kb").unwrap(), 10 * 1024);
        assert_eq!(parse_batch_bytes("2MB").unwrap(), 2 * 1024 * 1024);
        assert!(parse_batch_bytes("10MB").is_err());
        assert!(parse_batch_bytes("10Gb").is_err());
        assert!(parse_batch_bytes("kb").is_err());
        assert!(parse_batch_bytes("MB").is_err());
    }

    #[test]
    fn test_parse_metrics_push_protocol() {
        assert_eq!(
            parse_metrics_push_protocol("http").unwrap(),
            MetricsPushProtocol::HTTP
        );
        assert_eq!(
            parse_metrics_push_protocol("grpc").unwrap(),
            MetricsPushProtocol::GRPC
        );
        assert!(parse_metrics_push_protocol("thrift").is_err());
    }
}
