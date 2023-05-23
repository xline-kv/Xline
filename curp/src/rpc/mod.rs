use std::sync::Arc;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub use self::proto::protocol_server::ProtocolServer;
pub(crate) use self::proto::{
    fetch_read_state_response::ReadState,
    propose_response::ExeResult,
    protocol_server::Protocol,
    wait_synced_response::{Success, SyncResult as SyncResultRaw},
    AppendEntriesRequest, AppendEntriesResponse, FetchReadStateRequest, FetchReadStateResponse,
    IdSet, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
    WaitSyncedRequest, WaitSyncedResponse,
};
pub use self::proto::{
    propose_response, protocol_client, FetchLeaderRequest, FetchLeaderResponse, ProposeRequest,
    ProposeResponse,
};
use crate::{
    cmd::{Command, ProposeId},
    error::ProposeError,
    log_entry::LogEntry,
    LogIndex, ServerId,
};

/// Rpc connect
pub(crate) mod connect;
pub(crate) use connect::connect;

// Skip for generated code
#[allow(
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    unused_qualifications,
    unreachable_pub,
    variant_size_differences,
    missing_copy_implementations,
    missing_docs,
    trivial_casts
)]
mod proto {
    tonic::include_proto!("messagepb");
}

impl FetchLeaderRequest {
    /// Create a new `FetchLeaderRequest`
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl FetchLeaderResponse {
    /// Create a new `FetchLeaderResponse`
    pub(crate) fn new(leader_id: Option<ServerId>, term: u64) -> Self {
        Self { leader_id, term }
    }
}

impl ProposeRequest {
    /// Create a new `Propose` request
    pub(crate) fn new<C: Command>(cmd: &C) -> bincode::Result<Self> {
        Ok(Self {
            command: bincode::serialize(cmd)?,
        })
    }

    /// Get command
    pub(crate) fn cmd<C: Command>(&self) -> bincode::Result<C> {
        bincode::deserialize(&self.command)
    }
}

impl ProposeResponse {
    /// Create an ok propose response
    pub(crate) fn new_result<C: Command>(
        leader_id: Option<ServerId>,
        term: u64,
        result: &C::ER,
    ) -> bincode::Result<Self> {
        Ok(Self {
            leader_id,
            term,
            exe_result: Some(ExeResult::Result(bincode::serialize(result)?)),
        })
    }

    /// Create an empty propose response
    #[allow(clippy::unnecessary_wraps)] // To keep the new functions return the same type
    pub(crate) fn new_empty(leader_id: Option<ServerId>, term: u64) -> bincode::Result<Self> {
        Ok(Self {
            leader_id,
            term,
            exe_result: None,
        })
    }

    /// Create an error propose response
    pub(crate) fn new_error(
        leader_id: Option<ServerId>,
        term: u64,
        error: &ProposeError,
    ) -> bincode::Result<Self> {
        Ok(Self {
            leader_id,
            term,
            exe_result: Some(ExeResult::Error(bincode::serialize(error)?)),
        })
    }

    /// Response term
    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    /// Map response to functions `success` and `failure`
    pub(crate) fn map_or_else<C: Command, SF, FF, R>(
        &self,
        success: SF,
        failure: FF,
    ) -> bincode::Result<R>
    where
        SF: FnOnce(Option<C::ER>) -> R,
        FF: FnOnce(ProposeError) -> R,
    {
        match self.exe_result {
            Some(ExeResult::Result(ref rv)) => Ok(success(Some(bincode::deserialize(rv)?))),
            Some(ExeResult::Error(ref e)) => Ok(failure(bincode::deserialize(e)?)),
            None => Ok(success(None)),
        }
    }
}

impl WaitSyncedRequest {
    /// Create a `WaitSynced` request
    pub(crate) fn new(id: &ProposeId) -> bincode::Result<Self> {
        Ok(Self {
            id: bincode::serialize(id)?,
        })
    }

    /// Get the propose id
    pub(crate) fn id(&self) -> bincode::Result<ProposeId> {
        bincode::deserialize(&self.id)
    }
}

impl WaitSyncedResponse {
    /// Create a success response
    pub(crate) fn new_success<C: Command>(asr: &C::ASR, er: &C::ER) -> bincode::Result<Self> {
        Ok(Self {
            sync_result: Some(SyncResultRaw::Success(Success {
                after_sync_result: bincode::serialize(&asr)?,
                exe_result: bincode::serialize(&er)?,
            })),
        })
    }

    /// Create an error response
    pub(crate) fn new_error(err: &SyncError) -> bincode::Result<Self> {
        Ok(Self {
            sync_result: Some(SyncResultRaw::Error(bincode::serialize(&err)?)),
        })
    }

    /// Create a new response from execution result and `after_sync` result
    pub(crate) fn new_from_result<C: Command>(
        er: Option<Result<C::ER, String>>,
        asr: Option<Result<C::ASR, String>>,
    ) -> bincode::Result<Self> {
        match (er, asr) {
            (None, Some(_)) => {
                unreachable!("should not call after sync if execution fails")
            }
            (None, None) => WaitSyncedResponse::new_error(&SyncError::AfterSyncError(
                "can't get er result".to_owned(),
            )), // this is highly unlikely to happen,
            (Some(Err(_)), Some(_)) => {
                unreachable!("should not call after_sync when exe failed")
            }
            (Some(Err(err)), None) => WaitSyncedResponse::new_error(&SyncError::ExecuteError(err)),
            (Some(Ok(_er)), Some(Err(err))) => {
                // FIXME: should er be returned?
                WaitSyncedResponse::new_error(&SyncError::AfterSyncError(err))
            }
            (Some(Ok(er)), Some(Ok(asr))) => WaitSyncedResponse::new_success::<C>(&asr, &er),
            (Some(Ok(_er)), None) => {
                // FIXME: should er be returned?
                WaitSyncedResponse::new_error(&SyncError::AfterSyncError(
                    "can't get after sync result".to_owned(),
                )) // this is highly unlikely to happen,
            }
        }
    }

    /// Into deserialized result
    pub(crate) fn into<C: Command>(self) -> bincode::Result<SyncResult<C>> {
        let res = match self.sync_result {
            None => unreachable!("WaitSyncedResponse should contain valid sync_result"),
            Some(SyncResultRaw::Success(success)) => SyncResult::Success {
                asr: bincode::deserialize(&success.after_sync_result)?,
                er: bincode::deserialize(&success.exe_result)?,
            },
            Some(SyncResultRaw::Error(err)) => SyncResult::Error(bincode::deserialize(&err)?),
        };
        Ok(res)
    }
}

/// Sync Result
pub(crate) enum SyncResult<C: Command> {
    /// If sync succeeds, return asr and er
    Success {
        /// After Sync Result
        asr: C::ASR,
        /// Execution Result
        er: C::ER,
    },
    /// If sync fails, return `SyncError`
    Error(SyncError),
}

/// Wait Synced error
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) enum SyncError {
    /// If client sent a wait synced request to a non-leader
    Redirect(Option<ServerId>, u64),
    /// If the execution of the cmd went wrong
    ExecuteError(String),
    /// If after sync of the cmd went wrong
    AfterSyncError(String),
    /// If there is no such cmd to be waited
    NoSuchCmd(ProposeId),
    /// Wait timeout
    Timeout,
}

impl AppendEntriesRequest {
    /// Create a new `append_entries` request
    pub(crate) fn new<C: Command + Serialize>(
        term: u64,
        leader_id: ServerId,
        prev_log_index: LogIndex,
        prev_log_term: u64,
        entries: Vec<LogEntry<C>>,
        leader_commit: LogIndex,
    ) -> bincode::Result<Self> {
        Ok(Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: entries
                .into_iter()
                .map(|e| bincode::serialize(&e))
                .collect::<bincode::Result<Vec<Vec<u8>>>>()?,
            leader_commit,
        })
    }

    /// Get log entries
    pub(crate) fn entries<C: Command>(&self) -> bincode::Result<Vec<LogEntry<C>>> {
        self.entries
            .iter()
            .map(|entry| bincode::deserialize(entry))
            .collect()
    }
}

impl AppendEntriesResponse {
    /// Create a new rejected response
    pub(crate) fn new_reject(term: u64, hint_index: LogIndex) -> Self {
        Self {
            term,
            success: false,
            hint_index,
        }
    }

    /// Create a new accepted response
    pub(crate) fn new_accept(term: u64) -> Self {
        Self {
            term,
            success: true,
            hint_index: 0,
        }
    }
}

impl VoteRequest {
    /// Create a new vote request
    pub fn new(
        term: u64,
        candidate_id: String,
        last_log_index: LogIndex,
        last_log_term: u64,
    ) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

impl VoteResponse {
    /// Create a new accepted vote response
    pub fn new_accept<C: Command + Serialize>(
        term: u64,
        cmds: Vec<Arc<C>>,
    ) -> bincode::Result<Self> {
        Ok(Self {
            term,
            vote_granted: true,
            spec_pool: cmds
                .into_iter()
                .map(|c| bincode::serialize(&c))
                .collect::<bincode::Result<Vec<Vec<u8>>>>()?,
        })
    }

    /// Create a new rejected vote response
    pub fn new_reject(term: u64) -> Self {
        Self {
            term,
            vote_granted: false,
            spec_pool: vec![],
        }
    }

    /// Get spec pool
    pub fn spec_pool<C: Command + DeserializeOwned>(&self) -> bincode::Result<Vec<C>> {
        self.spec_pool
            .iter()
            .map(|cmd| bincode::deserialize(cmd))
            .collect()
    }
}

impl InstallSnapshotResponse {
    /// Create a new snapshot response
    pub(crate) fn new(term: u64) -> Self {
        Self { term }
    }
}

impl IdSet {
    /// Create a new `IdSet`
    pub fn new(ids: Vec<ProposeId>) -> bincode::Result<Self> {
        Ok(Self {
            ids: ids
                .into_iter()
                .map(|id| bincode::serialize(&id))
                .collect::<bincode::Result<Vec<Vec<u8>>>>()?,
        })
    }
}

impl FetchReadStateRequest {
    /// Create a new fetch read state request
    pub(crate) fn new<C: Command>(cmd: &C) -> bincode::Result<Self> {
        Ok(Self {
            command: bincode::serialize(cmd)?,
        })
    }

    /// Get command
    pub(crate) fn cmd<C: Command>(&self) -> bincode::Result<C> {
        bincode::deserialize(&self.command)
    }
}

impl FetchReadStateResponse {
    /// Create a new fetch read state response
    pub(crate) fn new(state: ReadState) -> Self {
        Self {
            read_state: Some(state),
        }
    }
}
