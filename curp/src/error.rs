use std::io;

use curp_external_api::cmd::Command;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::rpc::SyncError;

/// Server side error
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum ServerError {
    /// Met I/O error during rpc communication
    #[error("meet io related error")]
    IoError(#[from] io::Error),

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
    /// Command syncing error
    #[error("syncing error {0}")]
    SyncedError(SyncError),
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

/// The union error which includes propose errors and user-defined errors.
#[derive(Error, Debug, Serialize, Deserialize)]
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
pub enum CommandProposeError<C: Command> {
    /// Curp propose error
    #[error("propose error: {0}")]
    Propose(#[from] ProposeError),
    /// User defined execute error
    #[error("execute error: {0}")]
    Execute(C::Error),
    /// User defined after sync error
    #[error("after sync error: {0}")]
    AfterSync(C::Error),
}
