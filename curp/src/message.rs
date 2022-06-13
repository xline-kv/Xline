use madsim::Request;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{cmd::Command, error::ProposeError};

////////////////////////// Propose ///////////////////////////////

/// The propose send by a client to servers
#[derive(Serialize, Deserialize, Request)]
#[serde(bound = "C: Serialize + DeserializeOwned")]
#[rtype("ProposeResponse<C>")]
pub(crate) struct Propose<C: 'static + Command> {
    /// Command
    cmd: C,
}

impl<C: 'static + Command> Propose<C> {
    /// create a new propose
    pub(crate) fn new(cmd: C) -> Self {
        Self { cmd }
    }
}

impl<C: 'static + Command> Propose<C> {
    /// Get command
    pub(crate) fn cmd(&self) -> &C {
        &self.cmd
    }
}

/// Response for propose request
#[derive(Serialize, Deserialize)]
#[serde(bound = "C: Serialize + DeserializeOwned")]
pub(crate) struct ProposeResponse<C: Command> {
    /// is_leader
    is_leader: bool,
    /// Term number
    term: u64,
    /// Response inner data
    inner: ProposeResponseInner<C>,
}

impl<C: Command> ProposeResponse<C> {
    /// Create an ok propose response
    pub(crate) fn new_ok(is_leader: bool, term: u64, return_value: C::ER) -> Self {
        ProposeResponse {
            is_leader,
            term,
            inner: ProposeResponseInner::ReturnValue(Some(return_value)),
        }
    }

    /// Create an empty propose response
    pub(crate) fn new_empty(is_leader: bool, term: u64) -> Self {
        ProposeResponse {
            is_leader,
            term,
            inner: ProposeResponseInner::ReturnValue(None),
        }
    }

    /// Create an error propose response
    pub(crate) fn new_error(is_leader: bool, term: u64, error: ProposeError) -> Self {
        ProposeResponse {
            is_leader,
            term,
            inner: ProposeResponseInner::Error(error),
        }
    }

    /// Response term
    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    /// Map response to functions `success` and `failure`
    pub(crate) fn map_or_else<SF, FF, R>(&self, success: SF, failure: FF) -> R
    where
        SF: FnOnce(Option<&C::ER>) -> R,
        FF: FnOnce(&ProposeError) -> R,
    {
        match self.inner {
            ProposeResponseInner::ReturnValue(ref rv) => success(rv.as_ref()),
            ProposeResponseInner::Error(ref e) => failure(e),
        }
    }
}

/// Inner data for propose response
#[derive(Serialize, Deserialize)]
pub(crate) enum ProposeResponseInner<C: Command> {
    /// Normal return value
    ReturnValue(Option<C::ER>),
    /// Any error met
    Error(ProposeError),
}
