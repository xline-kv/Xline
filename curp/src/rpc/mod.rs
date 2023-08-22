use std::{collections::HashMap, sync::Arc};

use curp_external_api::cmd::{PbCodec, PbSerializeError};
use serde::{de::DeserializeOwned, Serialize};

use self::proto::commandpb::{cmd_result::Result as CmdResultInner, CmdResult};
pub(crate) use self::proto::{
    commandpb::{
        propose_response::ExeResult,
        wait_synced_response::{Success, SyncResult as SyncResultRaw},
        WaitSyncedRequest, WaitSyncedResponse,
    },
    errorpb::{
        command_sync_error::CommandSyncError as PbCommandSyncError,
        propose_error::ProposeError as PbProposeError,
        wait_sync_error::WaitSyncError as PbWaitSyncError,
        CommandSyncError as PbCommandSyncErrorOuter, ProposeError as PbProposeErrorOuter,
        RedirectData, WaitSyncError as PbWaitSyncErrorOuter,
    },
    inner_messagepb::{
        inner_protocol_server::InnerProtocol, AppendEntriesRequest, AppendEntriesResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    messagepb::{
        fetch_read_state_response::ReadState,
        propose_conf_change_request::{ConfChange, ConfChangeType},
        protocol_server::Protocol,
        FetchClusterRequest, FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
        IdSet, ProposeConfChangeRequest, ProposeConfChangeResponse, ShutdownRequest,
        ShutdownResponse,
    },
};
pub use self::proto::{
    commandpb::{ProposeRequest, ProposeResponse},
    inner_messagepb::inner_protocol_server::InnerProtocolServer,
    messagepb::{
        protocol_client, protocol_server::ProtocolServer, FetchLeaderRequest, FetchLeaderResponse,
    },
};
use crate::{
    cmd::{Command, ProposeId},
    error::{CommandSyncError, ProposeError, WaitSyncError},
    log_entry::LogEntry,
    members::ServerId,
    LogIndex,
};

/// Rpc connect
pub(crate) mod connect;
pub(crate) use connect::{connect, inner_connect};

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
    trivial_casts,
    unused_results
)]
mod proto {
    pub(crate) mod messagepb {
        tonic::include_proto!("messagepb");
    }
    pub(crate) mod commandpb {
        tonic::include_proto!("commandpb");
    }
    pub(crate) mod errorpb {
        tonic::include_proto!("errorpb");
    }
    pub(crate) mod inner_messagepb {
        tonic::include_proto!("inner_messagepb");
    }
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

impl FetchClusterRequest {
    /// Create a new `FetchClusterRequest`
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl FetchClusterResponse {
    /// Create a new `FetchClusterResponse`
    pub(crate) fn new(
        leader_id: Option<ServerId>,
        all_members: HashMap<ServerId, Vec<String>>,
        term: u64,
    ) -> Self {
        Self {
            leader_id,
            all_members: all_members
                .into_iter()
                .map(|(id, addrs)| (id, Addrs { addrs }))
                .collect(),
            term,
        }
    }

    /// Get all members
    pub(crate) fn into_members(self) -> HashMap<ServerId, Vec<String>> {
        self.all_members
            .into_iter()
            .map(|(id, addrs)| (id, addrs.addrs))
            .collect()
    }
}

impl ProposeRequest {
    /// Create a new `Propose` request
    pub(crate) fn new<C: Command>(cmd: &C) -> Self {
        Self {
            command: cmd.encode(),
        }
    }

    /// Get command
    pub(crate) fn cmd<C: Command>(&self) -> Result<C, PbSerializeError> {
        C::decode(&self.command)
    }
}

impl ProposeResponse {
    /// Create an ok propose response
    pub(crate) fn new_result<C: Command>(
        leader_id: Option<ServerId>,
        term: u64,
        result: &Result<C::ER, C::Error>,
    ) -> Self {
        let exe_result = match *result {
            Ok(ref er) => Some(ExeResult::Result(CmdResult {
                result: Some(CmdResultInner::Er(er.encode())),
            })),
            Err(ref e) => Some(ExeResult::Result(CmdResult {
                result: Some(CmdResultInner::Error(e.encode())),
            })),
        };
        Self {
            leader_id,
            term,
            exe_result,
        }
    }

    /// Create an empty propose response
    #[allow(clippy::unnecessary_wraps)] // To keep the new functions return the same type
    pub(crate) fn new_empty(leader_id: Option<ServerId>, term: u64) -> Self {
        Self {
            leader_id,
            term,
            exe_result: None,
        }
    }

    /// Create an error propose response
    pub(crate) fn new_error(leader_id: Option<ServerId>, term: u64, error: ProposeError) -> Self {
        Self {
            leader_id,
            term,
            exe_result: Some(ExeResult::Error(error.into())),
        }
    }

    /// Map response to functions `success` and `failure`
    pub(crate) fn map_or_else<C: Command, SF, FF, R>(
        &self,
        success: SF,
        failure: FF,
    ) -> Result<R, PbSerializeError>
    where
        SF: FnOnce(Option<Result<C::ER, C::Error>>) -> R,
        FF: FnOnce(ProposeError) -> R,
    {
        match self.exe_result {
            Some(ExeResult::Result(ref rv)) => {
                let result = rv.result.as_ref().ok_or(PbSerializeError::EmptyField)?;
                let cmd_result = match *result {
                    CmdResultInner::Er(ref buf) => Ok(<C as Command>::ER::decode(buf)?),
                    CmdResultInner::Error(ref buf) => Err(<C as Command>::Error::decode(buf)?),
                };
                Ok(success(Some(cmd_result)))
            }
            Some(ExeResult::Error(ref err)) => {
                let propose_error = err
                    .clone()
                    .propose_error
                    .ok_or(PbSerializeError::EmptyField)?
                    .try_into()?;
                Ok(failure(propose_error))
            }
            None => Ok(success(None)),
        }
    }
}

impl WaitSyncedRequest {
    /// Create a `WaitSynced` request
    pub(crate) fn new(propose_id: ProposeId) -> Self {
        Self { propose_id }
    }

    /// Get the `propose_id` reference
    pub(crate) fn propose_id(&self) -> &ProposeId {
        &self.propose_id
    }
}

impl WaitSyncedResponse {
    /// Create a success response
    pub(crate) fn new_success<C: Command>(asr: &C::ASR, er: &C::ER) -> Self {
        Self {
            sync_result: Some(SyncResultRaw::Success(Success {
                after_sync_result: asr.encode(),
                exe_result: er.encode(),
            })),
        }
    }

    /// Create an error response
    pub(crate) fn new_error<C: Command>(err: CommandSyncError<C>) -> Self {
        Self {
            sync_result: Some(SyncResultRaw::Error(PbCommandSyncErrorOuter {
                command_sync_error: Some(err.into()),
            })),
        }
    }

    /// Create a new response from execution result and `after_sync` result
    pub(crate) fn new_from_result<C: Command>(
        er: Option<Result<C::ER, C::Error>>,
        asr: Option<Result<C::ASR, C::Error>>,
    ) -> Self {
        match (er, asr) {
            (None | Some(Err(_)), Some(_)) => {
                unreachable!("should not call after sync if execution fails")
            }
            (None, None) => WaitSyncedResponse::new_error::<C>(
                WaitSyncError::Other("can't get er result".to_owned()).into(),
            ), // this is highly unlikely to happen,
            (Some(Err(err)), None) => {
                WaitSyncedResponse::new_error(CommandSyncError::<C>::Execute(err))
            }
            // The er is ignored as the propose has failed
            (Some(Ok(_er)), Some(Err(err))) => {
                WaitSyncedResponse::new_error(CommandSyncError::<C>::AfterSync(err))
            }
            (Some(Ok(er)), Some(Ok(asr))) => WaitSyncedResponse::new_success::<C>(&asr, &er),
            // The er is ignored as the propose has failed
            (Some(Ok(_er)), None) => {
                WaitSyncedResponse::new_error::<C>(
                    WaitSyncError::Other("can't get after sync result".to_owned()).into(),
                ) // this is highly unlikely to happen,
            }
        }
    }

    /// Into deserialized result
    pub(crate) fn into<C: Command>(self) -> Result<SyncResult<C>, PbSerializeError> {
        let res = match self.sync_result {
            None => unreachable!("WaitSyncedResponse should contain valid sync_result"),
            Some(SyncResultRaw::Success(success)) => SyncResult::Success {
                asr: <C as Command>::ASR::decode(&success.after_sync_result)?,
                er: <C as Command>::ER::decode(&success.exe_result)?,
            },
            Some(SyncResultRaw::Error(err)) => {
                let cmd_sync_err = err
                    .command_sync_error
                    .ok_or(PbSerializeError::EmptyField)?
                    .try_into()?;
                SyncResult::Error(cmd_sync_err)
            }
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
    Error(CommandSyncError<C>),
}

impl AppendEntriesRequest {
    /// Create a new `append_entries` request
    pub(crate) fn new<C: Command + Serialize>(
        term: u64,
        leader_id: ServerId,
        prev_log_index: LogIndex,
        prev_log_term: u64,
        entries: Vec<Arc<LogEntry<C>>>,
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
    pub(crate) fn new(
        term: u64,
        candidate_id: ServerId,
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
    pub(crate) fn new_accept<C: Command + Serialize>(
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
    pub(crate) fn new_reject(term: u64) -> Self {
        Self {
            term,
            vote_granted: false,
            spec_pool: vec![],
        }
    }

    /// Get spec pool
    pub(crate) fn spec_pool<C: Command + DeserializeOwned>(&self) -> bincode::Result<Vec<C>> {
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

#[allow(dead_code)] // TODO: remove this when we implement conf change
#[allow(clippy::as_conversions)] // ConfChangeType is so small that it won't exceed the range of i32 type.
impl ConfChange {
    /// Create a new `ConfChange` to add a node
    pub(crate) fn add(node_id: ServerId, address: String) -> Self {
        Self {
            change_type: ConfChangeType::Add as i32,
            node_id,
            address: vec![address],
        }
    }

    /// Create a new `ConfChange` to remove a node
    pub(crate) fn remove(node_id: ServerId) -> Self {
        Self {
            change_type: ConfChangeType::Remove as i32,
            node_id,
            address: vec![],
        }
    }

    /// Create a new `ConfChange` to update a node
    pub(crate) fn update(node_id: ServerId, address: String) -> Self {
        Self {
            change_type: ConfChangeType::Update as i32,
            node_id,
            address: vec![address],
        }
    }

    /// Create a new `ConfChange` to add a learner node
    pub(crate) fn add_learner(node_id: ServerId, address: String) -> Self {
        Self {
            change_type: ConfChangeType::AddLearner as i32,
            node_id,
            address: vec![address],
        }
    }
}

impl ShutdownResponse {
    /// Create a new shutdown response
    pub(crate) fn new(leader_id: Option<ServerId>, term: u64, error: Option<ProposeError>) -> Self {
        let error = error.map(Into::into);
        Self {
            leader_id,
            term,
            error,
        }
    }
}
