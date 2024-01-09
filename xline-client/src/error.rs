use curp::cmd::Command as CurpCommand;
use thiserror::Error;
use xlineapi::{command::Command, execute_error::ExecuteError};

/// The result type for `xline-client`
pub type Result<T> = std::result::Result<T, XlineClientError<Command>>;

/// Error type of client builder
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum XlineClientBuildError {
    /// Rpc error
    #[error("Rpc error: {0}")]
    RpcError(String),
    /// Invalid arguments
    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
    /// Authentication error
    #[error("Authenticate error: {0}")]
    AuthError(String),
}

impl XlineClientBuildError {
    /// Create a new `XlineClientBuildError::InvalidArguments`
    #[inline]
    #[must_use]
    pub fn invalid_arguments(msg: &str) -> Self {
        Self::InvalidArguments(msg.to_owned())
    }
}

impl From<tonic::transport::Error> for XlineClientBuildError {
    #[inline]
    fn from(e: tonic::transport::Error) -> Self {
        Self::RpcError(e.to_string())
    }
}

impl From<tonic::Status> for XlineClientBuildError {
    #[inline]
    fn from(e: tonic::Status) -> Self {
        Self::RpcError(e.to_string())
    }
}

/// The error type for `xline-client`
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum XlineClientError<C: CurpCommand> {
    /// Command error
    #[error("command execute error {0}")]
    CommandError(C::Error),
    /// Io error
    #[error("IO error {0}")]
    IoError(String),
    /// RPC error
    #[error("rpc error: {0}")]
    RpcError(String),
    /// Command execution error
    #[error("command execution error: {0}")]
    ExecuteError(ExecuteError),
    /// Arguments invalid error
    #[error("Invalid arguments: {0}")]
    InvalidArgs(String),
    /// Internal Error
    #[error("Client Internal error: {0}")]
    InternalError(String),
    /// Error in watch client
    #[error("Watch client error: {0}")]
    WatchError(String),
    /// Error in lease client
    #[error("Lease client error: {0}")]
    LeaseError(String),
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
    #[error("Wrong cluster version")]
    WrongClusterVersion,
}

impl From<tonic::transport::Error> for XlineClientError<Command> {
    #[inline]
    fn from(e: tonic::transport::Error) -> Self {
        Self::RpcError(e.to_string())
    }
}

impl From<tonic::Status> for XlineClientError<Command> {
    #[inline]
    fn from(e: tonic::Status) -> Self {
        Self::RpcError(e.to_string())
    }
}

impl From<ExecuteError> for XlineClientError<Command> {
    #[inline]
    fn from(e: ExecuteError) -> Self {
        Self::ExecuteError(e)
    }
}
