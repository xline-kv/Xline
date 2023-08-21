use std::io;

use curp_external_api::cmd::{Command, PbSerialize, PbSerializeError};
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    members::ServerId,
    rpc::{
        PbCommandSyncError, PbCommandSyncErrorOuter, PbProposeError, PbProposeErrorOuter,
        PbSyncError, PbSyncErrorOuter, RedirectData,
    },
};

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
    pub fn invalid_aurguments(msg: &str) -> Self {
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
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
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

impl TryFrom<PbProposeError> for ProposeError {
    type Error = PbSerializeError;

    #[inline]
    fn try_from(err: PbProposeError) -> Result<ProposeError, Self::Error> {
        Ok(match err {
            PbProposeError::KeyConflict(_) => ProposeError::KeyConflict,
            PbProposeError::Duplicated(_) => ProposeError::Duplicated,
            PbProposeError::SyncError(e) => {
                ProposeError::SyncedError(e.sync_error.ok_or(PbSerializeError::EmptyField)?.into())
            }
            PbProposeError::EncodeError(s) => ProposeError::EncodeError(s),
        })
    }
}

impl From<ProposeError> for PbProposeError {
    #[inline]
    fn from(err: ProposeError) -> Self {
        match err {
            ProposeError::KeyConflict => PbProposeError::KeyConflict(()),
            ProposeError::Duplicated => PbProposeError::Duplicated(()),
            ProposeError::SyncedError(e) => PbProposeError::SyncError(PbSyncErrorOuter {
                sync_error: Some(e.into()),
            }),
            ProposeError::EncodeError(s) => PbProposeError::EncodeError(s),
        }
    }
}

impl From<PbSerializeError> for ProposeError {
    #[inline]
    fn from(err: PbSerializeError) -> Self {
        ProposeError::EncodeError(err.to_string())
    }
}

impl PbSerialize for ProposeError {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        PbProposeErrorOuter {
            propose_error: Some(self.clone().into()),
        }
        .encode_to_vec()
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError> {
        PbProposeErrorOuter::decode(buf)?
            .propose_error
            .ok_or(PbSerializeError::EmptyField)?
            .try_into()
    }
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
    #[error("propose error: {0:?}")]
    Propose(#[from] ProposeError),
    /// User defined execute error
    #[error("execute error: {0}")]
    Execute(C::Error),
    /// User defined after sync error
    #[error("after sync error: {0}")]
    AfterSync(C::Error),
}

/// Wait synced error
#[derive(Clone, Error, Serialize, Deserialize, Debug)]
#[allow(clippy::module_name_repetitions)] // this-error generate code false-positive
#[non_exhaustive]
pub enum SyncError {
    /// If client sent a wait synced request to a non-leader
    #[error("redirect to {0:?}, term {1}")]
    Redirect(Option<ServerId>, u64),
    /// Other error
    #[error("other: {0}")]
    Other(String),
}

impl From<PbSyncError> for SyncError {
    #[inline]
    fn from(err: PbSyncError) -> Self {
        match err {
            PbSyncError::Redirect(data) => SyncError::Redirect(data.server_id, data.term),
            PbSyncError::Other(s) => SyncError::Other(s),
        }
    }
}

impl From<SyncError> for PbSyncError {
    fn from(err: SyncError) -> Self {
        match err {
            SyncError::Redirect(server_id, term) => {
                PbSyncError::Redirect(RedirectData { server_id, term })
            }
            SyncError::Other(s) => PbSyncError::Other(s),
        }
    }
}

/// The union error which includes sync errors and user-defined errors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum CommandSyncError<C: Command> {
    /// If wait sync went wrong
    Sync(SyncError),
    /// If the execution of the cmd went wrong
    Execute(C::Error),
    /// If after sync of the cmd went wrong
    AfterSync(C::Error),
}

impl<C: Command> From<CommandSyncError<C>> for PbCommandSyncError {
    fn from(err: CommandSyncError<C>) -> Self {
        match err {
            CommandSyncError::Sync(e) => PbCommandSyncError::Sync(PbSyncErrorOuter {
                sync_error: Some(e.into()),
            }),
            CommandSyncError::Execute(e) => PbCommandSyncError::Execute(e.encode()),
            CommandSyncError::AfterSync(e) => PbCommandSyncError::AfterSync(e.encode()),
        }
    }
}

impl<C: Command> TryFrom<PbCommandSyncError> for CommandSyncError<C> {
    type Error = PbSerializeError;

    fn try_from(err: PbCommandSyncError) -> Result<Self, Self::Error> {
        Ok(match err {
            PbCommandSyncError::Sync(e) => {
                CommandSyncError::Sync(e.sync_error.ok_or(PbSerializeError::EmptyField)?.into())
            }
            PbCommandSyncError::Execute(e) => {
                CommandSyncError::Execute(<C as Command>::Error::decode(&e)?)
            }
            PbCommandSyncError::AfterSync(e) => {
                CommandSyncError::AfterSync(<C as Command>::Error::decode(&e)?)
            }
        })
    }
}

impl<C: Command> PbSerialize for CommandSyncError<C> {
    fn encode(&self) -> Vec<u8> {
        PbCommandSyncErrorOuter {
            command_sync_error: Some(self.clone().into()),
        }
        .encode_to_vec()
    }

    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError> {
        PbCommandSyncErrorOuter::decode(buf)?
            .command_sync_error
            .ok_or(PbSerializeError::EmptyField)?
            .try_into()
    }
}

impl<C: Command> From<SyncError> for CommandSyncError<C> {
    fn from(err: SyncError) -> Self {
        Self::Sync(err)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn propose_error_serialization_is_ok() {
        let err = ProposeError::Duplicated;
        let _decoded_err =
            <ProposeError as PbSerialize>::decode(&err.encode()).expect("decode should success");
        assert!(matches!(err, _decoded_err));
    }
}
