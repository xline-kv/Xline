use serde::{Deserialize, Serialize};
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

/// Server side error
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ServerError {
    /// Met I/O error during rpc communication
    #[error("meet io related error")]
    IoError(#[from] io::Error),

    /// Rpc Service Error reported by madsim
    #[error("rpc service error")]
    RpcServiceError(String),
}

/// The error met during propose phase
#[derive(Error, Debug, Serialize, Deserialize, Clone)]
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
pub enum ProposeError {
    /// The command conflicts with keys in the speculative commands
    #[error("key conflict error")]
    KeyConflict,
    /// Command execution error
    #[error("command execution error {0}")]
    ExecutionError(String),
    /// Command syncing error
    #[error("syncing error {0}")]
    SyncedError(String),
    /// Rpc error
    #[error("rpc error {0}")]
    RpcError(String),
    /// Protocol error
    #[error("protocol error {0}")]
    ProtocolError(String),
}
