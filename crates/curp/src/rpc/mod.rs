use std::{collections::HashMap, sync::Arc};

use curp_external_api::{
    cmd::{ConflictCheck, PbCodec, PbSerializeError},
    InflightId,
};
use prost::Message;
use serde::{Deserialize, Serialize};

pub(crate) use self::proto::{
    commandpb::CurpError as CurpErrorWrapper,
    inner_messagepb::{
        inner_protocol_server::InnerProtocol, AppendEntriesRequest, AppendEntriesResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, TriggerShutdownRequest,
        TriggerShutdownResponse, TryBecomeLeaderNowRequest, TryBecomeLeaderNowResponse,
        VoteRequest, VoteResponse,
    },
};
pub use self::proto::{
    commandpb::{
        cmd_result::Result as CmdResultInner,
        curp_error::Err as CurpError, // easy for match
        curp_error::Redirect,
        fetch_read_state_response::{IdSet, ReadState},
        propose_conf_change_request::{ConfChange, ConfChangeType},
        protocol_client,
        protocol_server::{Protocol, ProtocolServer},
        CmdResult,
        FetchClusterRequest,
        FetchClusterResponse,
        FetchReadStateRequest,
        FetchReadStateResponse,
        LeaseKeepAliveMsg,
        Member,
        MoveLeaderRequest,
        MoveLeaderResponse,
        ProposeConfChangeRequest,
        ProposeConfChangeResponse,
        ProposeId as PbProposeId,
        ProposeRequest,
        ProposeResponse,
        PublishRequest,
        PublishResponse,
        ShutdownRequest,
        ShutdownResponse,
        WaitSyncedRequest,
        WaitSyncedResponse,
    },
    inner_messagepb::inner_protocol_server::InnerProtocolServer,
};
use crate::{cmd::Command, log_entry::LogEntry, members::ServerId, LogIndex};

/// Metrics
#[cfg(feature = "client-metrics")]
mod metrics;

/// Rpc connect
pub(crate) mod connect;
pub(crate) use connect::{connect, connects, inner_connects};

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

    /// Get all members peer urls
    pub(crate) fn into_peer_urls(self) -> HashMap<ServerId, Vec<String>> {
        self.members
            .into_iter()
            .map(|member| (member.id, member.peer_urls))
            .collect()
    }

    /// Get all members peer urls
    pub(crate) fn into_client_urls(self) -> HashMap<ServerId, Vec<String>> {
        self.members
            .into_iter()
            .map(|member| (member.id, member.client_urls))
            .collect()
    }
}

impl ProposeRequest {
    /// Create a new `Propose` request
    #[inline]
    pub fn new<C: Command>(propose_id: ProposeId, cmd: &C, cluster_version: u64) -> Self {
        Self {
            propose_id: Some(propose_id.into()),
            command: cmd.encode(),
            cluster_version,
        }
    }

    /// Get the propose id
    #[inline]
    #[must_use]
    pub fn propose_id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| unreachable!("propose id must be set in ProposeRequest"))
            .into()
    }

    /// Get command
    /// # Errors
    /// Return error if the command can't be decoded
    #[inline]
    pub fn cmd<C: Command>(&self) -> Result<C, PbSerializeError> {
        C::decode(&self.command)
    }
}

impl ProposeResponse {
    /// Create an ok propose response
    pub(crate) fn new_result<C: Command>(result: &Result<C::ER, C::Error>) -> Self {
        let result = match *result {
            Ok(ref er) => Some(CmdResult {
                result: Some(CmdResultInner::Ok(er.encode())),
            }),
            Err(ref e) => Some(CmdResult {
                result: Some(CmdResultInner::Error(e.encode())),
            }),
        };
        Self { result }
    }

    /// Create an empty propose response
    pub(crate) fn new_empty() -> Self {
        Self { result: None }
    }

    /// Deserialize result in response and take a map function
    pub(crate) fn map_result<C: Command, F, R>(self, f: F) -> Result<R, PbSerializeError>
    where
        F: FnOnce(Result<Option<C::ER>, C::Error>) -> R,
    {
        let Some(res) = self.result.and_then(|res| res.result) else {
            return Ok(f(Ok(None)));
        };
        let res = match res {
            CmdResultInner::Ok(ref buf) => Ok(<C as Command>::ER::decode(buf)?),
            CmdResultInner::Error(ref buf) => Err(<C as Command>::Error::decode(buf)?),
        };
        Ok(f(res.map(Some)))
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
    fn new_success<C: Command>(asr: &C::ASR, er: &C::ER) -> Self {
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
    fn new_er_error<C: Command>(er: &C::Error) -> Self {
        Self {
            after_sync_result: None,
            exe_result: Some(CmdResult {
                result: Some(CmdResultInner::Error(er.encode())),
            }),
        }
    }

    /// Create an error response which includes an `after_sync` error
    fn new_asr_error<C: Command>(er: &C::ER, asr_err: &C::Error) -> Self {
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

    /// Similar to `ProposeResponse::map_result`
    pub(crate) fn map_result<C: Command, F, R>(self, f: F) -> Result<R, PbSerializeError>
    where
        F: FnOnce(Result<(C::ASR, C::ER), C::Error>) -> R,
    {
        // according to the above methods, we can only get the following response union
        // ER: Some(OK), ASR: Some(OK)  <-  WaitSyncedResponse::new_success
        // ER: Some(Err), ASR: None     <-  WaitSyncedResponse::new_er_error
        // ER: Some(OK), ASR: Some(Err) <- WaitSyncedResponse::new_asr_error
        let res = match (self.exe_result, self.after_sync_result) {
            (
                Some(CmdResult {
                    result: Some(CmdResultInner::Ok(ref er)),
                }),
                Some(CmdResult {
                    result: Some(CmdResultInner::Ok(ref asr)),
                }),
            ) => {
                let er = <C as Command>::ER::decode(er)?;
                let asr = <C as Command>::ASR::decode(asr)?;
                Ok((asr, er))
            }
            (
                Some(CmdResult {
                    result: Some(CmdResultInner::Error(ref buf)),
                }),
                None,
            )
            | (
                Some(CmdResult {
                    result: Some(CmdResultInner::Ok(_)),
                }),
                Some(CmdResult {
                    result: Some(CmdResultInner::Error(ref buf)),
                }),
            ) => {
                let er = <C as Command>::Error::decode(buf.as_slice())?;
                Err(er)
            }
            _ => unreachable!("got unexpected WaitSyncedResponse"),
        };

        Ok(f(res))
    }
}

impl AppendEntriesRequest {
    /// Create a new `append_entries` request
    pub(crate) fn new<C: Command>(
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
    pub(crate) fn new_accept<C: Command>(
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
    pub(crate) fn spec_pool<C: Command>(&self) -> bincode::Result<Vec<PoolEntry<C>>> {
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
    pub(crate) fn new(inflight_ids: Vec<InflightId>) -> Self {
        Self { inflight_ids }
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
    pub(crate) fn new(id: ProposeId, changes: Vec<ConfChange>, cluster_version: u64) -> Self {
        Self {
            propose_id: Some(id.into()),
            changes,
            cluster_version,
        }
    }

    /// Get id of the request
    pub(crate) fn propose_id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| {
                unreachable!("propose id should be set in propose conf change request")
            })
            .into()
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
    pub(crate) fn propose_id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| {
                unreachable!("propose id should be set in propose conf change request")
            })
            .into()
    }
}

impl MoveLeaderRequest {
    /// Create a new `MoveLeaderRequest`
    pub(crate) fn new(node_id: ServerId, cluster_version: u64) -> Self {
        Self {
            node_id,
            cluster_version,
        }
    }
}

impl PublishRequest {
    /// Create a new `PublishRequest`
    pub(crate) fn new(
        id: ProposeId,
        node_id: ServerId,
        name: String,
        client_urls: Vec<String>,
    ) -> Self {
        Self {
            propose_id: Some(id.into()),
            node_id,
            name,
            client_urls,
        }
    }

    /// Get id of the request
    pub(crate) fn propose_id(&self) -> ProposeId {
        self.propose_id
            .clone()
            .unwrap_or_else(|| {
                unreachable!("propose id should be set in propose conf change request")
            })
            .into()
    }
}

/// NOTICE:
/// Please check test case `test_unary_fast_round_return_early_err` `test_unary_propose_return_early_err`
/// `test_retry_propose_return_no_retry_error` `test_retry_propose_return_retry_error` if you added some
/// new [`CurpError`]
impl CurpError {
    /// `KeyConflict` error
    pub(crate) fn key_conflict() -> Self {
        Self::KeyConflict(())
    }

    /// `Duplicated` error
    pub(crate) fn duplicated() -> Self {
        Self::Duplicated(())
    }

    /// `ExpiredClientId` error
    #[allow(unused)] // TODO: used in dedup
    pub(crate) fn expired_client_id() -> Self {
        Self::ExpiredClientId(())
    }

    /// `InvalidConfig` error
    pub(crate) fn invalid_config() -> Self {
        Self::InvalidConfig(())
    }

    /// `NodeNotExists` error
    pub(crate) fn node_not_exist() -> Self {
        Self::NodeNotExists(())
    }

    /// `NodeAlreadyExists` error
    pub(crate) fn node_already_exists() -> Self {
        Self::NodeAlreadyExists(())
    }

    /// `LearnerNotCatchUp` error
    pub(crate) fn learner_not_catch_up() -> Self {
        Self::LearnerNotCatchUp(())
    }

    /// `ShuttingDown` error
    pub(crate) fn shutting_down() -> Self {
        Self::ShuttingDown(())
    }

    /// `Duplicated` error
    pub(crate) fn wrong_cluster_version() -> Self {
        Self::WrongClusterVersion(())
    }

    /// `Redirect` error
    pub(crate) fn redirect(leader_id: Option<ServerId>, term: u64) -> Self {
        Self::Redirect(Redirect { leader_id, term })
    }

    /// `Internal` error
    pub(crate) fn internal(reason: impl Into<String>) -> Self {
        Self::Internal(reason.into())
    }

    /// Whether to abort fast round early
    pub(crate) fn should_abort_fast_round(&self) -> bool {
        matches!(
            *self,
            CurpError::Duplicated(_)
                | CurpError::ShuttingDown(_)
                | CurpError::InvalidConfig(_)
                | CurpError::NodeAlreadyExists(_)
                | CurpError::NodeNotExists(_)
                | CurpError::LearnerNotCatchUp(_)
                | CurpError::ExpiredClientId(_)
                | CurpError::Redirect(_)
        )
    }

    /// Whether to abort slow round early
    pub(crate) fn should_abort_slow_round(&self) -> bool {
        matches!(
            *self,
            CurpError::ShuttingDown(_)
                | CurpError::InvalidConfig(_)
                | CurpError::NodeAlreadyExists(_)
                | CurpError::NodeNotExists(_)
                | CurpError::LearnerNotCatchUp(_)
                | CurpError::ExpiredClientId(_)
                | CurpError::Redirect(_)
                | CurpError::WrongClusterVersion(_)
        )
    }

    /// Get the priority of the error
    pub(crate) fn priority(&self) -> CurpErrorPriority {
        match *self {
            CurpError::Duplicated(_)
            | CurpError::ShuttingDown(_)
            | CurpError::InvalidConfig(_)
            | CurpError::NodeAlreadyExists(_)
            | CurpError::NodeNotExists(_)
            | CurpError::LearnerNotCatchUp(_)
            | CurpError::ExpiredClientId(_)
            | CurpError::Redirect(_)
            | CurpError::WrongClusterVersion(_) => CurpErrorPriority::High,
            CurpError::RpcTransport(_)
            | CurpError::Internal(_)
            | CurpError::KeyConflict(_)
            | CurpError::LeaderTransfer(_) => CurpErrorPriority::Low,
        }
    }

    /// `LeaderTransfer` error
    pub(crate) fn leader_transfer(err: impl Into<String>) -> Self {
        Self::LeaderTransfer(err.into())
    }
}

/// The priority of curp error
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CurpErrorPriority {
    /// Low priority, a low-priority error returned may
    /// be overridden by a higher-priority error.
    Low = 1,
    /// High priority, high-priority errors will override
    /// low-priority errors.
    High = 2,
}

impl<E: std::error::Error + 'static> From<E> for CurpError {
    #[inline]
    fn from(value: E) -> Self {
        let err: &dyn std::error::Error = &value;
        if let Some(status) = err.downcast_ref::<tonic::Status>() {
            // Unavailable code often occurs in rpc connection errors,
            // Please DO NOT use this code in CurpError to tonic::Status.
            if status.code() == tonic::Code::Unavailable {
                return Self::RpcTransport(());
            }
            if !status.details().is_empty() {
                return match CurpErrorWrapper::decode(status.details()) {
                    Ok(e) => e
                        .err
                        .unwrap_or_else(|| unreachable!("err must be set in CurpErrorWrapper")),
                    Err(dec_err) => Self::internal(dec_err.to_string()),
                };
            }
        }
        // Errors that are not created manually by `CurpError::xxx()` are trivial,
        // and errors that need to be known to the client are best created manually.
        Self::internal(value.to_string())
    }
}

impl From<CurpError> for tonic::Status {
    #[inline]
    fn from(err: CurpError) -> Self {
        let (code, msg) = match err {
            CurpError::KeyConflict(_) => (
                tonic::Code::AlreadyExists,
                "Key conflict error: A key conflict occurred.",
            ),
            CurpError::Duplicated(_) => (
                tonic::Code::AlreadyExists,
                "Duplicated error: The request already sent.",
            ),
            CurpError::ExpiredClientId(_) => (
                tonic::Code::FailedPrecondition,
                "Expired client ID error: The client ID has expired, we cannot tell if this request is duplicated.",
            ),
            CurpError::InvalidConfig(_) => (
                tonic::Code::InvalidArgument,
                "Invalid config error: The provided configuration is invalid.",
            ),
            CurpError::NodeNotExists(_) => (
                tonic::Code::NotFound,
                "Node not found error: The specified node does not exist.",
            ),
            CurpError::NodeAlreadyExists(_) => (
                tonic::Code::AlreadyExists,
                "Node already exists error: The node already exists.",
            ),
            CurpError::LearnerNotCatchUp(_) => (
                tonic::Code::FailedPrecondition,
                "Learner not caught up error: The learner has not caught up.",
            ),
            CurpError::ShuttingDown(_) => (
                tonic::Code::FailedPrecondition,
                "Shutting down error: The service is currently shutting down.",
            ),
            CurpError::WrongClusterVersion(_) => (
                tonic::Code::FailedPrecondition,
                "Wrong cluster version error: The cluster version is incorrect.",
            ),
            CurpError::Redirect(_) => (
                tonic::Code::ResourceExhausted,
                "Redirect error: The request should be redirected to another node.",
            ),
            CurpError::Internal(_) => (
                tonic::Code::Internal,
                "Internal error: An internal error occurred.",
            ),
            CurpError::RpcTransport(_) => (tonic::Code::Cancelled, "Rpc error: Request cancelled"),
            CurpError::LeaderTransfer(_) => (
                tonic::Code::FailedPrecondition,
                "Leader transfer error: A leader transfer error occurred.",
            ),
        };

        let details = CurpErrorWrapper { err: Some(err) }.encode_to_vec();

        tonic::Status::with_details(code, msg, details.into())
    }
}

// User defined types

/// Entry of speculative pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PoolEntry<C> {
    /// Propose id
    pub(crate) id: ProposeId,
    /// Inner entry
    pub(crate) inner: PoolEntryInner<C>,
}

/// Inner entry of speculative pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PoolEntryInner<C> {
    /// Command entry
    Command(Arc<C>),
    /// ConfChange entry
    ConfChange(Vec<ConfChange>),
}

impl<C> PoolEntry<C>
where
    C: Command,
{
    /// Create a new pool entry
    pub(crate) fn new(id: ProposeId, inner: impl Into<PoolEntryInner<C>>) -> Self {
        Self {
            id,
            inner: inner.into(),
        }
    }

    /// Check if the entry is conflict with the command
    pub(crate) fn is_conflict_with_cmd(&self, c: &C) -> bool {
        match self.inner {
            PoolEntryInner::Command(ref cmd) => cmd.is_conflict(c),
            PoolEntryInner::ConfChange(ref _conf_change) => true,
        }
    }
}

impl<C> ConflictCheck for PoolEntry<C>
where
    C: ConflictCheck,
{
    fn is_conflict(&self, other: &Self) -> bool {
        let PoolEntryInner::Command(ref cmd1) = self.inner else {
            return true;
        };
        let PoolEntryInner::Command(ref cmd2) = other.inner else {
            return true;
        };
        cmd1.is_conflict(cmd2)
    }
}

impl<C> From<Arc<C>> for PoolEntryInner<C> {
    fn from(value: Arc<C>) -> Self {
        Self::Command(value)
    }
}

impl<C> From<Vec<ConfChange>> for PoolEntryInner<C> {
    fn from(value: Vec<ConfChange>) -> Self {
        Self::ConfChange(value)
    }
}

/// Command Id wrapper, which is used to identify a command
/// The underlying data is a tuple of (`client_id`, `seq_num`)
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd, Default,
)]
#[allow(clippy::exhaustive_structs)] // It is exhaustive
pub struct ProposeId(pub(crate) u64, pub(crate) u64);

impl std::fmt::Display for ProposeId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.0, self.1)
    }
}
