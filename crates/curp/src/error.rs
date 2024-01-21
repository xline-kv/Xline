use std::io;

use thiserror::Error;

use crate::server::StorageError;

/// Server bootstrap error
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

    /// Storage Error
    #[error("storage error: {0}")]
    StorageError(#[from] StorageError),
}
