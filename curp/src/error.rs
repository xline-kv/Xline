use std::io;

use curp_external_api::cmd::{Command, PbCodec, PbSerializeError};
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::rpc::{PbProposeError, PbProposeErrorOuter};

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
    pub fn invalid_arguments(msg: &str) -> Self {
        Self::InvalidArguments(msg.to_owned())
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

/// The error met during propose phase
#[derive(Error, Debug, Clone, Copy, Serialize, Deserialize)]
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
pub enum ProposeError {
    /// The command conflicts with keys in the speculative commands
    #[error("key conflict error")]
    KeyConflict,
    /// The command has already been proposed before
    #[error("duplicated, the cmd might have already been proposed")]
    Duplicated,
}

impl TryFrom<PbProposeError> for ProposeError {
    type Error = PbSerializeError;

    #[inline]
    fn try_from(err: PbProposeError) -> Result<ProposeError, Self::Error> {
        Ok(match err {
            PbProposeError::KeyConflict(_) => ProposeError::KeyConflict,
            PbProposeError::Duplicated(_) => ProposeError::Duplicated,
        })
    }
}

impl From<ProposeError> for PbProposeErrorOuter {
    #[inline]
    fn from(err: ProposeError) -> Self {
        let e = match err {
            ProposeError::KeyConflict => PbProposeError::KeyConflict(()),
            ProposeError::Duplicated => PbProposeError::Duplicated(()),
        };
        PbProposeErrorOuter {
            propose_error: Some(e),
        }
    }
}

impl PbCodec for ProposeError {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        PbProposeErrorOuter::from(*self).encode_to_vec()
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError> {
        PbProposeErrorOuter::decode(buf)?
            .propose_error
            .ok_or(PbSerializeError::EmptyField)?
            .try_into()
    }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn propose_error_serialization_is_ok() {
        let err = ProposeError::Duplicated;
        let _decoded_err =
            <ProposeError as PbCodec>::decode(&err.encode()).expect("decode should success");
        assert!(matches!(err, _decoded_err));
    }
}
