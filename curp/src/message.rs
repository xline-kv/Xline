use std::fmt::{Display, Formatter, Result};

use madsim::Request;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

use crate::cmd::Command;

/// The propose send by a client to servers
#[derive(Serialize, Deserialize, Request)]
#[serde(bound = "C: Serialize + DeserializeOwned")]
#[rtype("ProposeResponse<C>")]
pub(crate) struct Propose<C: 'static + Command> {
    /// Command
    cmd: C,
}

impl<C: 'static + Command> Propose<C> {
    /// Get command
    pub(crate) fn cmd(&self) -> &C {
        &self.cmd
    }
}

/// Response for propose request
#[derive(Serialize, Deserialize)]
pub(crate) enum ProposeResponse<C: Command> {
    /// Normal return value
    ReturnValue(C::ER),
    /// Any error met
    Error(ProposeError),
}

/// The error met during propose phase
#[derive(Error, Debug, Serialize, Deserialize)]
pub(crate) enum ProposeError {
    /// The command conflicts with keys in the speculative commands
    KeyConflict,
    /// Command execution error
    ExecutionError(String),
}

impl Display for ProposeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            &(ProposeError::KeyConflict | ProposeError::ExecutionError(_)) => {
                write!(f, "{:?}", self)
            }
        }
    }
}
