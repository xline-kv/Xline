use thiserror::Error;

/// The `EngineError`
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum EngineError {
    /// Met I/O Error during persisting data
    #[error("I/O Error: {0}")]
    IoError(#[from] std::io::Error),
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
    /// The Snapshot is invalid
    #[error("The Snapshot is invalid")]
    InvalidSnapshot,
}
