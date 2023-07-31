use std::io;

use serde::{Deserialize, Serialize};
use thiserror::Error;

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

    /// Parsing error
    #[error("parsing error: {0}")]
    ParsingError(String),

    /// Rpc Error
    #[error("rpc error: {0}")]
    RpcError(#[from] tonic::transport::Error),
}

/// The error met during propose phase
#[derive(Error, Debug, Serialize, Deserialize)]
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
pub enum ProposeError {
    /// The command conflicts with keys in the speculative commands
    #[error("key conflict error")]
    KeyConflict,
    /// The command has already been proposed before
    #[error("duplicated, the cmd might have already been proposed")]
    Duplicated,
    /// Command execution error
    #[error("command execution error {0}")]
    ExecutionError(String),
    /// Command syncing error
    #[error("syncing error {0}")]
    SyncedError(String),
    /// Encode error
    #[error("encode error: {0}")]
    EncodeError(String),
}

/// The error met during propose phase
#[derive(Error, Debug, Serialize, Deserialize)]
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[error("rcp error {0}")]
pub struct RpcError(String);

impl From<tonic::transport::Error> for RpcError {
    #[inline]
    fn from(e: tonic::transport::Error) -> Self {
        Self(e.to_string())
    }
}

impl From<tonic::Status> for RpcError {
    #[inline]
    fn from(e: tonic::Status) -> Self {
        Self(e.to_string())
    }
}

impl From<bincode::Error> for ProposeError {
    #[inline]
    fn from(e: bincode::Error) -> Self {
        Self::EncodeError(e.to_string())
    }
}
