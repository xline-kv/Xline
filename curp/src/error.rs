use std::io;
use thiserror::Error;

/// Error met when executing commands
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ExecuteError {
    /// Command is invalid
    #[error("invalid command {0} ")]
    InvalidCommand(String),
    /// Met I/O error while executing
    #[error("meet io related error")]
    IoError(#[from] io::Error),
}

/// Rpc Error
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum RpcError {
    /// Met I/O error during rpc communication
    #[error("meet io related error")]
    IoError(#[from] io::Error),
}
