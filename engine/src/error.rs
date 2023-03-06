use std::io;

use thiserror::Error;

/// The `EngineError`
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum EngineError {
    /// Met I/O Error during persisting data
    #[error("I/O Error")]
    IoError(String),
    /// Table Not Found
    #[error("Table {0} Not Found")]
    TableNotFound(String),
    /// DB File Corrupted
    #[error("DB File {0} Corrupted")]
    Corruption(String),
    /// Invalid Argument Error
    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),
    /// The Underlying Database Error
    #[error("The Underlying Database Error: {0}")]
    UnderlyingError(String),
}

impl From<io::Error> for EngineError {
    #[inline]
    fn from(err: io::Error) -> Self {
        Self::IoError(err.to_string())
    }
}

impl From<rocksdb::Error> for EngineError {
    #[inline]
    fn from(err: rocksdb::Error) -> Self {
        Self::UnderlyingError(err.to_string())
    }
}
