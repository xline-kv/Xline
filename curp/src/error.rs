use std::io;

use curp_external_api::cmd::{Command, PbSerializeError};
use thiserror::Error;

/// Error type of client builder
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientBuildError {
    /// Rpc error
    #[error("Rpc error: {0}")]
    RpcError(String),
    /// Invalid arguments
    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
}

impl ClientBuildError {
    /// Create a new `ClientBuildError::InvalidArguments`
    #[inline]
    #[must_use]
    pub fn invalid_arguments(msg: impl Into<String>) -> Self {
        Self::InvalidArguments(msg.into())
    }
}

impl From<tonic::transport::Error> for ClientBuildError {
    #[inline]
    fn from(e: tonic::transport::Error) -> Self {
        Self::RpcError(e.to_string())
    }
}

impl From<tonic::Status> for ClientBuildError {
    #[inline]
    fn from(e: tonic::Status) -> Self {
        Self::RpcError(e.to_string())
    }
}

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

/// The union error which includes propose errors and user-defined errors.
#[derive(Error, Debug)]
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
pub enum ClientError<C: Command> {
    /// Command error
    #[error("command execute error {0}")]
    CommandError(C::Error),
    /// Io error
    #[error("IO error {0}")]
    IoError(String),
    /// Rpc error
    #[error("RPC error: {0}")]
    OutOfBound(#[from] tonic::Status),
    /// Arguments invalid error，it's for outer client
    #[error("Invalid arguments: {0}")]
    InvalidArgs(String),
    /// Internal Error in client
    #[error("Client Internal error: {0}")]
    InternalError(String),
    /// Request Timeout
    #[error("Request timeout")]
    Timeout,
    /// Server is shutting down
    #[error("Curp Server is shutting down")]
    ShuttingDown,
    /// Serialize and Deserialize Error
    #[error("EncodeDecode error: {0}")]
    EncodeDecode(String),
    /// Wrong cluster version
    #[error("wrong cluster version")]
    WrongClusterVersion,
}

impl<C: Command> From<PbSerializeError> for ClientError<C> {
    #[inline]
    fn from(err: PbSerializeError) -> Self {
        Self::EncodeDecode(err.to_string())
    }
}

impl<C: Command> From<bincode::Error> for ClientError<C> {
    #[inline]
    fn from(err: bincode::Error) -> Self {
        Self::EncodeDecode(err.to_string())
    }
}
