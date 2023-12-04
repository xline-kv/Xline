use std::{collections::HashMap, sync::Arc};

use curp_external_api::cmd::{PbCodec, PbSerializeError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub use self::proto::{
    commandpb::{
        cmd_result::Result as CmdResultInner,
        propose_conf_change_request::{ConfChange, ConfChangeType},
        propose_conf_change_response::Error as ConfChangeError,
        protocol_client,
        protocol_server::ProtocolServer,
        CmdResult, FetchClusterRequest, FetchClusterResponse, Member, ProposeConfChangeRequest,
        ProposeConfChangeResponse, ProposeRequest, ProposeResponse, PublishRequest,
        PublishResponse,
    },
    inner_messagepb::inner_protocol_server::InnerProtocolServer,
};
pub(crate) use self::proto::{
    commandpb::{
        fetch_read_state_response::{IdSet, ReadState},
        propose_response::ExeResult,
        protocol_server::Protocol,
        FetchReadStateRequest, FetchReadStateResponse, ProposeError, ProposeId as PbProposeId,
        ShutdownRequest, ShutdownResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
    inner_messagepb::{
        inner_protocol_server::InnerProtocol, AppendEntriesRequest, AppendEntriesResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, TriggerShutdownRequest,
        TriggerShutdownResponse, VoteRequest, VoteResponse,
    },
};
use crate::{
    cmd::{Command, ProposeId},
    log_entry::LogEntry,
    members::ServerId,
    server::PoolEntry,
    LogIndex,
};

/// Rpc connect
pub(crate) mod connect;
pub(crate) use connect::{connects, inner_connects};

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
    pub(crate) mod commandpb {
        tonic::include_proto!("commandpb");
    }

    pub(crate) mod inner_messagepb {
        tonic::include_proto!("inner_messagepb");
    }
}

impl From<PbProposeId> for ProposeId {
    #[inline]
    fn from(id: PbProposeId) -> Self {
        Self(id.client_id, id.seq_num)
    }
}

impl From<ProposeId> for PbProposeId {
    #[inline]
    fn from(id: ProposeId) -> Self {
        Self {
            client_id: id.0,
            seq_num: id.1,
        }
    }
}

impl FetchClusterResponse {
    /// Create a new `FetchClusterResponse`
    pub(crate) fn new(
        leader_id: Option<ServerId>,
        term: u64,
        cluster_id: u64,
        members: Vec<Member>,
        cluster_version: u64,
    ) -> Self {
        Self {
            leader_id,
            term,
            cluster_id,
            members,
            cluster_version,
        }
    }

    /// Get all members addresses
    pub(crate) fn into_members_addrs(self) -> HashMap<ServerId, Vec<String>> {
        self.members
            .into_iter()
            .map(|member| (member.id, member.addrs))
            .collect()
    }
}

impl ProposeRequest {
    /// Create a new `Propose` request
    pub(crate) fn new<C: Command>(cmd: &C, cluster_version: u64) -> Self {
        Self {
            command: cmd.encode(),
            cluster_version,
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
                result: Some(CmdResultInner::Ok(er.encode())),
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
                    CmdResultInner::Ok(ref buf) => Ok(<C as Command>::ER::decode(buf)?),
                    CmdResultInner::Error(ref buf) => Err(<C as Command>::Error::decode(buf)?),
                };
                Ok(success(Some(cmd_result)))
            }
            Some(ExeResult::Error(err)) => {
                let propose_error = err.into();
                Ok(failure(propose_error))
            }
            None => Ok(success(None)),
        }
    }
}

impl WaitSyncedRequest {
    /// Create a `WaitSynced` request
    pub(crate) fn new(id: ProposeId, cluster_version: u64) -> Self {
        Self {
            propose_id: Some(id.into()),
            cluster_version,
        }
    }

    /// Get the `propose_id` reference
    pub(crate) fn propose_id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| {
                unreachable!("propose id should be set in propose wait synced request")
            })
            .into()
    }
}

impl WaitSyncedResponse {
    /// Create a success response
    pub(crate) fn new_success<C: Command>(asr: &C::ASR, er: &C::ER) -> Self {
        Self {
            after_sync_result: Some(CmdResult {
                result: Some(CmdResultInner::Ok(asr.encode())),
            }),
            exe_result: Some(CmdResult {
                result: Some(CmdResultInner::Ok(er.encode())),
            }),
        }
    }

    /// Create an error response which includes an execution error
    pub(crate) fn new_er_error<C: Command>(er: &C::Error) -> Self {
        Self {
            after_sync_result: None,
            exe_result: Some(CmdResult {
                result: Some(CmdResultInner::Error(er.encode())),
            }),
        }
    }

    /// Create an error response which includes an `after_sync` error
    pub(crate) fn new_asr_error<C: Command>(er: &C::ER, asr_err: &C::Error) -> Self {
        Self {
            after_sync_result: Some(CmdResult {
                result: Some(CmdResultInner::Error(asr_err.encode())),
            }),
            exe_result: Some(CmdResult {
                result: Some(CmdResultInner::Ok(er.encode())),
            }),
        }
    }

    /// Create a new response from execution result and `after_sync` result
    pub(crate) fn new_from_result<C: Command>(
        er: Result<C::ER, C::Error>,
        asr: Option<Result<C::ASR, C::Error>>,
    ) -> Self {
        match (er, asr) {
            (Ok(ref er), Some(Err(ref asr_err))) => {
                WaitSyncedResponse::new_asr_error::<C>(er, asr_err)
            }
            (Ok(ref er), Some(Ok(ref asr))) => WaitSyncedResponse::new_success::<C>(asr, er),
            (Ok(ref _er), None) => unreachable!("can't get after sync result"),
            (Err(ref err), _) => WaitSyncedResponse::new_er_error::<C>(err),
        }
    }
}

impl<C: Command> TryFrom<WaitSyncedResponse> for SyncResult<C> {
    type Error = PbSerializeError;
    fn try_from(value: WaitSyncedResponse) -> Result<Self, PbSerializeError> {
        let res = match (value.exe_result, value.after_sync_result) {
            (None, _) => unreachable!("WaitSyncedResponse should contain a valid exe_result"),
            (Some(er), None) => {
                if let Some(CmdResultInner::Error(buf)) = er.result {
                    SyncResult::Error(<C as Command>::Error::decode(buf.as_slice())?)
                } else {
                    unreachable!("asr should not be None")
                }
            }
            (Some(er), Some(asr)) => {
                let er = if let Some(CmdResultInner::Ok(er)) = er.result {
                    <C as Command>::ER::decode(er.as_slice())?
                } else {
                    unreachable!("asr should be None when execute failed")
                };
                match asr.result {
                    Some(CmdResultInner::Ok(asr)) => SyncResult::Success {
                        asr: <C as Command>::ASR::decode(asr.as_slice())?,
                        er,
                    },
                    Some(CmdResultInner::Error(err)) => {
                        SyncResult::Error(<C as Command>::Error::decode(err.as_slice())?)
                    }
                    None => {
                        unreachable!("result of asr should not be None")
                    }
                }
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
    Error(C::Error),
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
        is_pre_vote: bool,
    ) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
            is_pre_vote,
        }
    }
}

impl VoteResponse {
    /// Create a new accepted vote response
    pub(crate) fn new_accept<C: Command + Serialize>(
        term: u64,
        cmds: Vec<PoolEntry<C>>,
    ) -> bincode::Result<Self> {
        Ok(Self {
            term,
            vote_granted: true,
            spec_pool: cmds
                .into_iter()
                .map(|c| bincode::serialize(&c))
                .collect::<bincode::Result<Vec<Vec<u8>>>>()?,
            shutdown_candidate: false,
        })
    }

    /// Create a new rejected vote response
    pub(crate) fn new_reject(term: u64) -> Self {
        Self {
            term,
            vote_granted: false,
            spec_pool: vec![],
            shutdown_candidate: false,
        }
    }

    /// Create a new shutdown vote response
    pub(crate) fn new_shutdown() -> Self {
        Self {
            term: 0,
            vote_granted: false,
            spec_pool: vec![],
            shutdown_candidate: true,
        }
    }

    /// Get spec pool
    pub(crate) fn spec_pool<C: Command + DeserializeOwned>(
        &self,
    ) -> bincode::Result<Vec<PoolEntry<C>>> {
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
    pub fn new(ids: Vec<ProposeId>) -> Self {
        Self {
            ids: ids.into_iter().map(Into::into).collect(),
        }
    }
}

impl FetchReadStateRequest {
    /// Create a new fetch read state request
    pub(crate) fn new<C: Command>(cmd: &C, cluster_version: u64) -> bincode::Result<Self> {
        Ok(Self {
            command: bincode::serialize(cmd)?,
            cluster_version,
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

#[allow(clippy::as_conversions)] // ConfChangeType is so small that it won't exceed the range of i32 type.
impl ConfChange {
    /// Create a new `ConfChange` to add a node
    #[must_use]
    #[inline]
    pub fn add(node_id: ServerId, address: Vec<String>) -> Self {
        Self {
            change_type: ConfChangeType::Add as i32,
            node_id,
            address,
        }
    }

    /// Create a new `ConfChange` to remove a node
    #[must_use]
    #[inline]
    pub fn remove(node_id: ServerId) -> Self {
        Self {
            change_type: ConfChangeType::Remove as i32,
            node_id,
            address: vec![],
        }
    }

    /// Create a new `ConfChange` to update a node
    #[must_use]
    #[inline]
    pub fn update(node_id: ServerId, address: Vec<String>) -> Self {
        Self {
            change_type: ConfChangeType::Update as i32,
            node_id,
            address,
        }
    }

    /// Create a new `ConfChange` to add a learner node
    #[must_use]
    #[inline]
    pub fn add_learner(node_id: ServerId, address: Vec<String>) -> Self {
        Self {
            change_type: ConfChangeType::AddLearner as i32,
            node_id,
            address,
        }
    }

    /// Create a new `ConfChange` to promote a learner node
    #[must_use]
    #[inline]
    pub fn promote_learner(node_id: ServerId) -> Self {
        Self {
            change_type: ConfChangeType::Promote as i32,
            node_id,
            address: vec![],
        }
    }

    /// Create a new `ConfChange` to promote a node
    #[must_use]
    #[inline]
    pub fn promote(node_id: ServerId) -> Self {
        Self {
            change_type: ConfChangeType::Promote as i32,
            node_id,
            address: vec![],
        }
    }
}

impl ProposeConfChangeRequest {
    /// Create a new `ProposeConfChangeRequest`
    #[inline]
    #[must_use]
    pub fn new(id: ProposeId, changes: Vec<ConfChange>, cluster_version: u64) -> Self {
        Self {
            propose_id: Some(id.into()),
            changes,
            cluster_version,
        }
    }

    /// Get id of the request
    #[inline]
    #[must_use]
    pub fn id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| {
                unreachable!("propose id should be set in propose conf change request")
            })
            .into()
    }
}

/// Conf change data in log entry
#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct ConfChangeEntry {
    /// Propose id
    id: ProposeId,
    /// Conf changes
    changes: Vec<ConfChange>,
}

impl ConfChangeEntry {
    /// Get id of the entry
    pub(crate) fn id(&self) -> ProposeId {
        self.id
    }

    /// Get changes of the entry
    pub(crate) fn changes(&self) -> &[ConfChange] {
        &self.changes
    }
}

impl From<ProposeConfChangeRequest> for ConfChangeEntry {
    fn from(req: ProposeConfChangeRequest) -> Self {
        Self {
            id: req
                .propose_id
                .unwrap_or_else(|| {
                    unreachable!("propose id should be set in propose conf change request")
                })
                .into(),
            changes: req.changes,
        }
    }
}

impl ShutdownRequest {
    /// Create a new shutdown request
    pub(crate) fn new(id: ProposeId, cluster_version: u64) -> Self {
        Self {
            propose_id: Some(id.into()),
            cluster_version,
        }
    }

    /// Get id of the request
    #[inline]
    #[must_use]
    pub fn id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| {
                unreachable!("propose id should be set in propose conf change request")
            })
            .into()
    }
}

impl ConfChangeError {
    /// Create a new `ConfChangeError` with `ProposeError`
    pub(crate) fn new_propose(error: ProposeError) -> Self {
        Self::Propose(error.into())
    }
}

impl From<ConfChangeError> for tonic::Status {
    #[inline]
    fn from(_err: ConfChangeError) -> Self {
        // we'd better expose some err messages for client
        tonic::Status::invalid_argument("")
    }
}

impl PublishRequest {
    /// Create a new `PublishRequest`
    #[inline]
    #[must_use]
    pub fn new(id: ProposeId, node_id: ServerId, name: String) -> Self {
        Self {
            propose_id: Some(id.into()),
            node_id,
            name,
        }
    }

    /// Get id of the request
    #[inline]
    #[must_use]
    pub fn id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| {
                unreachable!("propose id should be set in propose conf change request")
            })
            .into()
    }
}
