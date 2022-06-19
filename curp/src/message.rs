use std::marker::PhantomData;

use madsim::Request;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    cmd::{Command, ProposeId},
    error::ProposeError,
};

/// Term Number
pub(crate) type TermNum = u64;

/// Log Index
pub type LogIndex = u64;

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
    term: TermNum,
    /// Response inner data
    inner: ProposeResponseInner<C>,
}

impl<C: Command> ProposeResponse<C> {
    /// Create an ok propose response
    pub(crate) fn new_ok(is_leader: bool, term: TermNum, return_value: C::ER) -> Self {
        ProposeResponse {
            is_leader,
            term,
            inner: ProposeResponseInner::ReturnValue(Some(return_value)),
        }
    }

    /// Create an empty propose response
    pub(crate) fn new_empty(is_leader: bool, term: TermNum) -> Self {
        ProposeResponse {
            is_leader,
            term,
            inner: ProposeResponseInner::ReturnValue(None),
        }
    }

    /// Create an error propose response
    pub(crate) fn new_error(is_leader: bool, term: TermNum, error: ProposeError) -> Self {
        ProposeResponse {
            is_leader,
            term,
            inner: ProposeResponseInner::Error(error),
        }
    }

    /// Response term
    pub(crate) fn term(&self) -> TermNum {
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

////////////////////////// Wait Synced ///////////////////////////////

/// Wait sync request
#[derive(Serialize, Deserialize, Request)]
#[serde(bound = "C: Serialize + DeserializeOwned")]
#[rtype("WaitSyncedResponse<C>")]
pub(crate) struct WaitSynced<C: Command + 'static> {
    /// The propose id to wait
    id: ProposeId,
    /// To keep the `C` type
    phantom: PhantomData<C>,
}

impl<C: Command> WaitSynced<C> {
    /// Create a new `WaitSynced` request
    pub(crate) fn new(id: ProposeId) -> Self {
        Self {
            id,
            phantom: PhantomData,
        }
    }

    /// The propose id
    pub(crate) fn id(&self) -> &ProposeId {
        &self.id
    }
}

/// Wait sync response
#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum WaitSyncedResponse<C: Command> {
    /// Success with log index
    Success((C::ASR, Option<C::ER>)),
    /// Error
    Error(String),
}

impl<C: Command> WaitSyncedResponse<C> {
    /// Create a success response
    pub(crate) fn new_success(asr: C::ASR, er: Option<C::ER>) -> Self {
        Self::Success((asr, er))
    }

    /// Create a error response
    pub(crate) fn new_error(err: String) -> Self {
        Self::Error(err)
    }
}

////////////////////////// Sync ///////////////////////////////

/// The sync message sent by the leader to followers
#[derive(Serialize, Deserialize, Request)]
#[serde(bound = "C: Serialize + DeserializeOwned")]
#[rtype("SyncResponse<C>")]
pub(crate) struct SyncCommand<C: 'static + Command> {
    /// Term number of current leader
    term: TermNum,
    /// Log Index
    index: LogIndex,
    /// Command
    cmd: C,
}

impl<C: 'static + Command> SyncCommand<C> {
    /// Create a new Sync command
    pub(crate) fn new(term: TermNum, index: LogIndex, cmd: C) -> Self {
        Self { term, index, cmd }
    }

    /// Get term
    pub(crate) fn term(&self) -> TermNum {
        self.term
    }

    /// Get index
    pub(crate) fn index(&self) -> LogIndex {
        self.index
    }

    /// Get cmd
    pub(crate) fn cmd(&self) -> &C {
        &self.cmd
    }
}

/// Response for propose request
#[derive(Serialize, Deserialize)]
#[serde(bound = "C: Serialize + DeserializeOwned")]
pub(crate) enum SyncResponse<C: Command> {
    /// The log entry synced
    Synced,
    /// The term is wrong, means you're not the leader now
    WrongTerm(TermNum),
    /// The entry is occupied
    EntryNotEmpty((TermNum, C)),
    /// The previous log is not ready
    PrevNotReady(LogIndex),
}
