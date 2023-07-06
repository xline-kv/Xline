use thiserror::Error;

/// The result type for `xline-client`
pub type Result<T> = std::result::Result<T, ClientError>;

/// The error type for `xline-client`
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// Propose error
    #[error("propose error {0}")]
    ProposeError(#[from] curp::error::ProposeError),
    /// Io error
    #[error("IO error {0}")]
    IoError(#[from] std::io::Error),
    /// Rpc error
    #[error("rpc error: {0}")]
    RpcError(String),
    /// Arguments invalid error
    #[error("Invalid arguments: {0}")]
    InvalidArgs(String),
    /// Error in watch client
    #[error("Watch client error: {0}")]
    WatchError(String),
    /// Error in lease client
    #[error("Lease client error: {0}")]
    LeaseError(String),
}

impl From<tonic::transport::Error> for ClientError {
    #[inline]
    fn from(e: tonic::transport::Error) -> Self {
        Self::RpcError(e.to_string())
    }
}

impl From<tonic::Status> for ClientError {
    #[inline]
    fn from(e: tonic::Status) -> Self {
        Self::RpcError(e.to_string())
    }
}
