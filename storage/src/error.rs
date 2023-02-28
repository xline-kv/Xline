use std::io;

use thiserror::Error;

/// The `EngineError`
#[allow(dead_code)]
#[non_exhaustive]
#[derive(Error, Debug)]
pub(crate) enum EngineError {
    /// Met I/O Error during persisting data
    #[error("I/O Error")]
    IoError(#[from] io::Error),
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
