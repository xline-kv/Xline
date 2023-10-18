use std::{collections::HashMap, fmt::Debug, io, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use curp_external_api::cmd::PbSerializeError;
use engine::{SnapshotAllocator, SnapshotApi};
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use madsim::rand::{thread_rng, Rng};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tonic::{metadata::MetadataMap, Code, Status};
use tracing::{debug, error, info, trace, warn};
use utils::{
    config::CurpConfig,
    shutdown::{self, Signal},
};

use super::{
    cmd_board::{CmdBoardRef, CommandBoard},
    cmd_worker::{conflict_checked_mpmc, start_cmd_workers},
    gc::run_gc_tasks,
    raw_curp::{AppendEntries, RawCurp, UncommittedPool, Vote},
    spec_pool::{SpecPoolRef, SpeculativePool},
    storage::{StorageApi, StorageError},
};
use crate::{
    cmd::{Command, CommandExecutor},
    error::ERROR_LABEL,
    log_entry::LogEntry,
    members::{ClusterInfo, ServerId},
    role_change::RoleChange,
    rpc::{
        self,
        connect::{InnerConnectApi, InnerConnectApiWrapper},
        AppendEntriesRequest, AppendEntriesResponse, FetchClusterRequest, FetchClusterResponse,
        FetchReadStateRequest, FetchReadStateResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, ProposeConfChangeRequest, ProposeConfChangeResponse,
        ProposeRequest, ProposeResponse, ShutdownRequest, ShutdownResponse, VoteRequest,
        VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
    server::{cmd_worker::CEEventTxApi, raw_curp::SyncAction, storage::db::DB},
    snapshot::{Snapshot, SnapshotMeta},
    ConfChangeType,
};

/// Curp error
#[derive(Debug, Error)]
pub(super) enum CurpError {
    /// Encode or decode error
    #[error("encode or decode error")]
    EncodeDecode(String),
    /// Storage error
    #[error("storage error, {0}")]
    Storage(#[from] StorageError),
    /// Transport error
    #[error("transport error, {0}")]
    Transport(String),
    /// Io error
    #[error("io error, {0}")]
    IO(#[from] io::Error),
    /// Currently, Internal Error includes the following three types of errors:
    /// 1. failed to allocate a new snapshot
    /// 2. failed to reset the command executor by snapshot
    /// 3. failed to get last applied index from command executor.
    #[error("internal error, {0}")]
    Internal(String),
    /// Curp Server is shutting down
    #[error("cluster shutdown")]
    ShuttingDown,
    /// If client sent a wait synced request to a non-leader
    #[error("redirect to {0:?}, term {1}")]
    Redirect(Option<ServerId>, u64),
    /// Wrong cluster version
    #[error("wrong cluster version")]
    WrongClusterVersion,
}

impl From<bincode::Error> for CurpError {
    fn from(err: bincode::Error) -> Self {
        Self::EncodeDecode(err.to_string())
    }
}

impl From<PbSerializeError> for CurpError {
    fn from(err: PbSerializeError) -> Self {
        Self::EncodeDecode(err.to_string())
    }
}

///  Generate metadata for `tonic::Status`
fn gen_metadata(label: &str) -> MetadataMap {
    let mut meta = MetadataMap::new();
    _ = meta.insert(
        ERROR_LABEL,
        label.parse().unwrap_or_else(|e| {
            unreachable!("convert a empty string to MetadataValue should always success: {e}")
        }),
    );
    meta
}

impl From<CurpError> for Status {
    #[inline]
    fn from(err: CurpError) -> Self {
        match err {
            CurpError::EncodeDecode(msg) => {
                let metadata = gen_metadata("encode-decode");
                Status::with_metadata(Code::Cancelled, msg, metadata)
            }
            CurpError::Internal(msg) => {
                let metadata = gen_metadata("internal");
                Status::with_metadata(Code::Internal, msg, metadata)
            }
            CurpError::Storage(err) => {
                let metadata = gen_metadata("storage");
                Status::with_metadata(Code::Internal, err.to_string(), metadata)
            }
            CurpError::IO(err) => {
                let metadata = gen_metadata("io");
                Status::with_metadata(Code::Internal, err.to_string(), metadata)
            }
            CurpError::Transport(msg) => {
                let metadata = gen_metadata("transport");
                Status::with_metadata(Code::Unavailable, msg, metadata)
            }
            CurpError::ShuttingDown => {
                let metadata = gen_metadata("shutting-down");
                Status::with_metadata(Code::Unavailable, "CurpServer is shutting down", metadata)
            }
            CurpError::Redirect(leader_id, term) => {
                let metadata = gen_metadata("redirect");
                let leader_term = (leader_id, term);
                let mut details: Vec<u8> = Vec::new();
                serde_json::to_writer(&mut details, &leader_term).unwrap_or_else(|e| {
                    unreachable!("serialize a tuple (Option<u64>, u64) should always succeed: {e}")
                });
                Status::with_details_and_metadata(
                    Code::FailedPrecondition,
                    "current node is not a leader",
                    details.into(),
                    metadata,
                )
            }
            CurpError::WrongClusterVersion => {
                let metadata = gen_metadata("wrong-cluster-version");
                Status::with_metadata(Code::FailedPrecondition, "wrong cluster version", metadata)
            }
        }
    }
}

/// Internal error encountered when sending `append_entries`
#[derive(Debug, Error)]
enum SendAEError {
    /// When self is no longer leader
    #[error("self is no longer leader")]
    NotLeader,
    /// When the follower rejects
    #[error("follower rejected")]
    Rejected,
    /// Transport
    #[error("transport error, {0}")]
    Transport(String),
    /// RpcError
    #[error("RPC error: {0}")]
    RpcError(String),
    /// Encode/Decode error
    #[error("encode or decode error")]
    EncodeDecode(String),
}

impl From<Status> for SendAEError {
    fn from(status: Status) -> Self {
        #[allow(clippy::wildcard_enum_match_arm)]
        // it's ok to do so since only three status can covert to `SendAEError`
        let metadata = status.metadata();
        if let Some(label) = metadata.get(ERROR_LABEL) {
            match label.to_str().unwrap_or_else(|err| {
                unreachable!("error-label should be always able to convert to str: {err:?}")
            }) {
                "transport" => Self::Transport(status.message().to_owned()),
                "redirect" => Self::NotLeader,
                "encode-decode" => Self::EncodeDecode(status.message().to_owned()),
                _ => Self::RpcError(status.message().to_owned()),
            }
        } else {
            Self::Transport(status.message().to_owned())
        }
    }
}

impl From<bincode::Error> for SendAEError {
    fn from(err: bincode::Error) -> Self {
        Self::EncodeDecode(err.to_string())
    }
}

/// Internal error encountered when sending snapshot
#[derive(Debug, Error)]
enum SendSnapshotError {
    /// When self is no longer leader
    #[error("self is no longer leader")]
    NotLeader,
    /// Transport
    #[error("transport error, {0}")]
    Transport(String),
    /// Rpc error
    #[error("RPC error: {0}")]
    RpcError(String),
}

impl From<Status> for SendSnapshotError {
    fn from(status: Status) -> Self {
        #[allow(clippy::wildcard_enum_match_arm)]
        // it's ok to do so since `SendSnapshotError` only has two variants.
        let metadata = status.metadata();
        if let Some(label) = metadata.get(ERROR_LABEL) {
            match label.to_str().unwrap_or_else(|err| {
                unreachable!("error-label should be always able to convert to str: {err:?}")
            }) {
                "transport" => Self::Transport(status.message().to_owned()),
                "redirect" => Self::NotLeader,
                _ => Self::RpcError(status.message().to_owned()),
            }
        } else {
            Self::Transport(status.message().to_owned())
        }
    }
}

/// `CurpNode` represents a single node of curp cluster
pub(super) struct CurpNode<C: Command, RC: RoleChange> {
    /// `RawCurp` state machine
    curp: Arc<RawCurp<C, RC>>,
    /// The speculative cmd pool, shared with executor
    spec_pool: SpecPoolRef<C>,
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: CmdBoardRef<C>,
    /// CE event tx,
    ce_event_tx: Arc<dyn CEEventTxApi<C>>,
    /// Storage
    storage: Arc<dyn StorageApi<Command = C>>,
    /// Snapshot allocator
    snapshot_allocator: Box<dyn SnapshotAllocator>,
}

// handlers
impl<C: 'static + Command, RC: RoleChange + 'static> CurpNode<C, RC> {
    /// Handle `Propose` requests
    pub(super) async fn propose(&self, req: ProposeRequest) -> Result<ProposeResponse, CurpError> {
        if self.curp.is_shutdown() {
            return Err(CurpError::ShuttingDown);
        }
        self.check_cluster_version(req.cluster_version)?;
        let cmd: Arc<C> = Arc::new(req.cmd()?);

        // handle proposal
        let ((leader_id, term), result) = self.curp.handle_propose(Arc::clone(&cmd))?;
        let resp = match result {
            Ok(true) => {
                let er_res = CommandBoard::wait_for_er(&self.cmd_board, cmd.id()).await;
                ProposeResponse::new_result::<C>(leader_id, term, &er_res)
            }
            Ok(false) => ProposeResponse::new_empty(leader_id, term),
            Err(err) => ProposeResponse::new_error(leader_id, term, err),
        };

        Ok(resp)
    }

    /// Handle `Shutdown` requests
    pub(super) async fn shutdown(
        &self,
        req: ShutdownRequest,
    ) -> Result<ShutdownResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        self.curp.handle_shutdown(req.id())?;
        CommandBoard::wait_for_shutdown_synced(&self.cmd_board).await;
        Ok(ShutdownResponse::default())
    }

    /// Handle `ProposeConfChange` requests
    pub(super) async fn propose_conf_change(
        &self,
        req: ProposeConfChangeRequest,
    ) -> Result<ProposeConfChangeResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        let id = req.id();
        let ((leader_id, term), result) = self.curp.handle_propose_conf_change(req.into())?;
        let error = match result {
            Ok(()) => {
                CommandBoard::wait_for_conf(&self.cmd_board, id).await;
                None
            }
            Err(err) => Some(err),
        };
        let members = self.curp.cluster().all_members_vec();
        Ok(ProposeConfChangeResponse {
            members,
            leader_id,
            term,
            error,
        })
    }

    /// Handle `AppendEntries` requests
    pub(super) fn append_entries(
        &self,
        req: &AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, CurpError> {
        let entries = req.entries()?;

        let result = self.curp.handle_append_entries(
            req.term,
            req.leader_id,
            req.prev_log_index,
            req.prev_log_term,
            entries,
            req.leader_commit,
        );
        let resp = match result {
            Ok(term) => AppendEntriesResponse::new_accept(term),
            Err((term, hint)) => AppendEntriesResponse::new_reject(term, hint),
        };

        Ok(resp)
    }

    /// Handle `Vote` requests
    pub(super) async fn vote(&self, req: VoteRequest) -> Result<VoteResponse, CurpError> {
        let result = self.curp.handle_vote(
            req.term,
            req.candidate_id,
            req.last_log_index,
            req.last_log_term,
        );
        let resp = match result {
            Ok((term, sp)) => {
                self.storage.flush_voted_for(term, req.candidate_id).await?;
                VoteResponse::new_accept(term, sp)?
            }
            Err(term) => VoteResponse::new_reject(term),
        };

        Ok(resp)
    }

    /// handle `WaitSynced` requests
    pub(super) async fn wait_synced(
        &self,
        req: WaitSyncedRequest,
    ) -> Result<WaitSyncedResponse, CurpError> {
        if self.curp.is_shutdown() {
            return Err(CurpError::ShuttingDown);
        }
        self.check_cluster_version(req.cluster_version)?;
        let id = req.propose_id();
        debug!("{} get wait synced request for cmd({id})", self.curp.id());

        let (er, asr) = CommandBoard::wait_for_er_asr(&self.cmd_board, id).await;
        debug!("{} wait synced for cmd({id}) finishes", self.curp.id());
        Ok(WaitSyncedResponse::new_from_result::<C>(er, asr))
    }

    /// Handle `FetchCluster` requests
    #[allow(clippy::unnecessary_wraps, clippy::needless_pass_by_value)] // To keep type consistent with other request handlers
    pub(super) fn fetch_cluster(
        &self,
        req: FetchClusterRequest,
    ) -> Result<FetchClusterResponse, CurpError> {
        let (leader_id, term, is_leader) = self.curp.leader();
        let cluster_id = self.curp.cluster().cluster_id();
        let members = if is_leader || !req.linearizable {
            self.curp.cluster().all_members_vec()
        } else {
            // if it is a follower and enabled linearizable read, return empty members
            // the client will ignore empty members and retry util it gets response from
            // the leader
            Vec::new()
        };
        let cluster_version = self.curp.cluster().cluster_version();
        Ok(FetchClusterResponse::new(
            leader_id,
            term,
            cluster_id,
            members,
            cluster_version,
        ))
    }

    /// Handle `InstallSnapshot` stream
    #[allow(clippy::integer_arithmetic)] // can't overflow
    pub(super) async fn install_snapshot(
        &self,
        req_stream: impl Stream<Item = Result<InstallSnapshotRequest, String>>,
    ) -> Result<InstallSnapshotResponse, CurpError> {
        pin_mut!(req_stream);
        let mut snapshot = self
            .snapshot_allocator
            .allocate_new_snapshot()
            .await
            .map_err(|err| {
                error!("failed to allocate a new snapshot, {err:?}");
                CurpError::Internal("failed to allocate a new snapshot".to_owned())
            })?;
        while let Some(req) = req_stream.next().await {
            let req = req.map_err(|e| {
                warn!("snapshot stream error, {e}");
                CurpError::Transport(e)
            })?;
            if !self.curp.verify_install_snapshot(
                req.term,
                req.leader_id,
                req.last_included_index,
                req.last_included_term,
            ) {
                return Ok(InstallSnapshotResponse::new(self.curp.term()));
            }
            let req_data_len = req.data.len().numeric_cast::<u64>();
            snapshot.write_all(req.data).await.map_err(|err| {
                error!("can't write snapshot data, {err:?}");
                err
            })?;
            if req.done {
                debug_assert_eq!(
                    snapshot.size(),
                    req.offset + req_data_len,
                    "snapshot corrupted"
                );
                let meta = SnapshotMeta {
                    last_included_index: req.last_included_index,
                    last_included_term: req.last_included_term,
                };
                let snapshot = Snapshot::new(meta, snapshot);
                info!(
                    "{} successfully received a snapshot, {snapshot:?}",
                    self.curp.id(),
                );
                self.ce_event_tx
                    .send_reset(Some(snapshot))
                    .await
                    .map_err(|err| {
                        let err = CurpError::Internal(format!(
                            "failed to reset the command executor by snapshot, {err}"
                        ));
                        error!("{err}");
                        err
                    })?;
                return Ok(InstallSnapshotResponse::new(self.curp.term()));
            }
        }
        Err(CurpError::Transport(
            "failed to receive a complete snapshot".to_owned(),
        ))
    }

    /// Handle `FetchReadState` requests
    #[allow(clippy::needless_pass_by_value)] // To keep type consistent with other request handlers
    pub(super) fn fetch_read_state(
        &self,
        req: FetchReadStateRequest,
    ) -> Result<FetchReadStateResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        let cmd = req.cmd()?;
        let state = self.curp.handle_fetch_read_state(&cmd);
        Ok(FetchReadStateResponse::new(state))
    }
}

/// Spawned tasks
impl<C: 'static + Command, RC: RoleChange + 'static> CurpNode<C, RC> {
    /// Tick periodically
    #[allow(clippy::integer_arithmetic)]
    async fn election_task(curp: Arc<RawCurp<C, RC>>) {
        let mut shutdown_listener = curp.shutdown_listener();
        let heartbeat_interval = curp.cfg().heartbeat_interval;
        // wait for some random time before tick starts to minimize vote split possibility
        let rand = thread_rng()
            .gen_range(0..heartbeat_interval.as_millis())
            .numeric_cast();
        tokio::time::sleep(Duration::from_millis(rand)).await;

        let mut ticker = tokio::time::interval(heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _now = ticker.tick() => {}
                _ = shutdown_listener.wait_self_shutdown() => {
                    debug!("election task exits");
                    return;
                }
            }
            if let Some(vote) = curp.tick_election() {
                Self::bcast_vote(curp.as_ref(), vote).await;
            }
        }
    }

    /// Responsible for bringing up `sync_follower_task` when self becomes leader
    #[allow(clippy::integer_arithmetic)]
    async fn sync_followers_daemon(curp: Arc<RawCurp<C, RC>>) {
        let mut shutdown_listener = curp.shutdown_listener();
        let leader_event = curp.leader_event();
        loop {
            if !curp.is_leader() {
                curp.shutdown_trigger().mark_sync_daemon_shutdown();
                curp.shutdown_trigger().check_and_shutdown();
                tokio::select! {
                    _ = shutdown_listener.wait_self_shutdown() => {
                        debug!("sync follower daemon exits");
                        return;
                    }
                    _ = leader_event.listen() => {
                        curp.shutdown_trigger().reset_sync_daemon_shutdown();
                    }
                }
            }
            let (sync_task_tx, mut sync_task_rx) = mpsc::channel(128);
            let mut remove_events = HashMap::new();

            for c in curp.connects().iter() {
                let sync_event = curp.sync_event(c.id());
                let remove_event = Arc::new(Event::new());
                let fut = Self::sync_follower_task(
                    Arc::clone(&curp),
                    c.value().clone(),
                    sync_event,
                    Arc::clone(&remove_event),
                );
                sync_task_tx
                    .send(tokio::spawn(fut))
                    .await
                    .unwrap_or_else(|_e| unreachable!("receiver should not be closed"));
                _ = remove_events.insert(c.id(), remove_event);
            }

            let _handle = tokio::spawn(Self::conf_change_handler(
                Arc::clone(&curp),
                remove_events,
                sync_task_tx,
            ));

            while let Some(h) = sync_task_rx.recv().await {
                let leader_retired = h
                    .await
                    .unwrap_or_else(|e| unreachable!("sync follower task panicked: {e}"));
                if leader_retired {
                    break;
                }
            }

            if shutdown_listener.is_shutdown() {
                curp.shutdown_trigger().mark_sync_daemon_shutdown();
                curp.shutdown_trigger().check_and_shutdown();
                debug!("{} sync follower daemon exits", curp.id());
                return;
            }
        }
    }

    /// Handler of conf
    async fn conf_change_handler(
        curp: Arc<RawCurp<C, RC>>,
        mut remove_events: HashMap<ServerId, Arc<Event>>,
        sender: mpsc::Sender<JoinHandle<bool>>,
    ) {
        let change_rx = curp.change_rx();
        let mut shutdown_listener = curp.shutdown_listener();
        #[allow(clippy::integer_arithmetic)] // introduced by tokio select
        loop {
            tokio::select! {
                _ = shutdown_listener.wait() => {
                    break;
                }
                change_res = change_rx.recv_async() => {
                    let Ok(change) = change_res else {
                        break;
                    };
                    match change.change_type() {
                        ConfChangeType::Add | ConfChangeType::AddLearner => {
                            let connect = match InnerConnectApiWrapper::connect(change.node_id, change.address).await {
                                Ok(connect) => connect,
                                Err(e) => {
                                    error!("connect to {} failed, {}", change.node_id, e);
                                    continue;
                                }
                            };
                            curp.insert_connect(connect.clone());
                            let sync_event = curp.sync_event(change.node_id);
                            let remove_event = Arc::new(Event::new());
                            sender
                                .send(tokio::spawn(Self::sync_follower_task(
                                    Arc::clone(&curp),
                                    connect,
                                    sync_event,
                                    Arc::clone(&remove_event),
                                )))
                                .await
                                .unwrap_or_else(|_e| unreachable!("change receiver should not be closed"));
                            _ = remove_events.insert(change.node_id, remove_event);
                        }
                        ConfChangeType::Remove => {
                            if change.node_id == curp.id() {
                                break;
                            }
                            let Some(event) = remove_events.remove(&change.node_id) else {
                                unreachable!("({:?}) shutdown_event of removed follower ({:x}) should exist", curp.id(), change.node_id);
                            };
                            event.notify(1);
                        }
                        ConfChangeType::Update =>{
                            if let Err(e) = curp.update_connect(change.node_id,change.address).await {
                                error!("update connect {} failed, {}", change.node_id, e);
                                continue;
                            }
                        }
                        ConfChangeType::Promote => {}
                    }
                }
            }
        }
    }

    /// Leader use this task to keep a follower up-to-date, will return if self is no longer leader,
    /// and return true if the leader is retired
    async fn sync_follower_task(
        curp: Arc<RawCurp<C, RC>>,
        connect: InnerConnectApiWrapper,
        sync_event: Arc<Event>,
        remove_event: Arc<Event>,
    ) -> bool {
        debug!("{} to {} sync follower task start", curp.id(), connect.id());
        let mut shutdown_listener = curp.shutdown_listener();
        let mut hb_opt = false;
        let mut ticker = tokio::time::interval(curp.cfg().heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let id = connect.id();
        let batch_timeout = curp.cfg().batch_timeout;
        let mut is_shutdown_state = false;
        let mut is_remove_state = false;

        #[allow(clippy::integer_arithmetic)] // tokio select internal triggered
        let leader_retired = loop {
            // a sync is either triggered by an heartbeat timeout event or when new log entries arrive
            tokio::select! {
                sig = shutdown_listener.wait(), if !is_shutdown_state => {
                    match sig {
                        Some(Signal::Running) => {
                            unreachable!("shutdown trigger should send ClusterShutdown or SelfShutdown")
                        }
                        Some(Signal::ClusterShutdown) => {
                            is_shutdown_state = true;
                        }
                        _ => {
                            break false;
                        },
                    }
                }
                _ = remove_event.listen() => {
                    is_remove_state = true;
                }
                _now = ticker.tick() => {
                    hb_opt = false;
                }
                res = tokio::time::timeout(batch_timeout, sync_event.listen()) => {
                    if let Err(_e) = res {
                        hb_opt = true;
                    }
                }
            }

            let can_remove_node = is_remove_state && curp.can_remove_follower_after_hb(id);

            let Some(sync_action) = curp.sync(id) else {
                break true;
            };

            match sync_action {
                SyncAction::AppendEntries(ae) => {
                    let is_empty = ae.entries.is_empty();
                    // (hb_opt, entries) status combination
                    // (false, empty) => send heartbeat to followers
                    // (true, empty) => indicates that `batch_timeout` expired, and during this period there is not any log generated. Do nothing
                    // (true | false, not empty) => send append entries
                    if !hb_opt || !is_empty {
                        let result = Self::send_ae(connect.as_ref(), curp.as_ref(), ae).await;
                        if let Err(err) = result {
                            warn!("ae to {} failed, {err}", connect.id());
                            if matches!(err, SendAEError::NotLeader) {
                                break true;
                            }
                        } else {
                            hb_opt = true;
                        }
                        if is_shutdown_state && is_empty && curp.is_synced() {
                            break false;
                        }
                        if can_remove_node {
                            curp.remove_node_status(id);
                            break false;
                        }
                    }
                }
                SyncAction::Snapshot(rx) => match rx.await {
                    Ok(snapshot) => {
                        let result =
                            Self::send_snapshot(connect.as_ref(), curp.as_ref(), snapshot).await;
                        if let Err(err) = result {
                            warn!("snapshot to {} failed, {err}", connect.id());
                            if matches!(err, SendSnapshotError::NotLeader) {
                                break true;
                            }
                        }
                    }
                    Err(err) => {
                        warn!("failed to receive snapshot result, {err}");
                    }
                },
            }
        };
        debug!("{} to {} sync follower task exits", curp.id(), connect.id());
        leader_retired
    }

    /// Log persist task
    pub(super) async fn log_persist_task(
        mut log_rx: mpsc::UnboundedReceiver<Arc<LogEntry<C>>>,
        storage: Arc<dyn StorageApi<Command = C>>,
        // This task will safely exit when the log_tx is dropped, but we still
        // need to keep the shutdown_listener here to notify the shutdown trigger
        _shutdown_listener: shutdown::Listener,
    ) {
        while let Some(e) = log_rx.recv().await {
            if let Err(err) = storage.put_log_entry(e.as_ref()).await {
                error!("storage error, {err}");
            }
        }
        debug!("log persist task exits");
    }
}

// utils
impl<C: 'static + Command, RC: RoleChange + 'static> CurpNode<C, RC> {
    /// Create a new server instance
    #[inline]
    pub(super) async fn new<CE: CommandExecutor<C> + 'static>(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        cmd_executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        shutdown_trigger: shutdown::Trigger,
    ) -> Result<Self, CurpError> {
        let shutdown_listener = shutdown_trigger.subscribe();
        let sync_events = cluster_info
            .peers_ids()
            .into_iter()
            .map(|server_id| (server_id, Arc::new(Event::new())))
            .collect();
        let connects = rpc::inner_connect(cluster_info.peers_addrs())
            .await
            .map_err(|e| CurpError::Internal(format!("parse peers addresses failed, err {e:?}")))?
            .collect();
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let last_applied = cmd_executor
            .last_applied()
            .map_err(|e| CurpError::Internal(format!("get applied index error, {e}")))?;
        let (ce_event_tx, task_rx, done_tx) =
            conflict_checked_mpmc::channel(Arc::clone(&cmd_executor), shutdown_trigger.clone());
        let ce_event_tx: Arc<dyn CEEventTxApi<C>> = Arc::new(ce_event_tx);
        let storage = Arc::new(DB::open(&curp_cfg.storage_cfg)?);

        // create curp state machine
        let (voted_for, entries) = storage.recover().await?;
        let curp = if voted_for.is_none() && entries.is_empty() {
            Arc::new(RawCurp::new(
                Arc::clone(&cluster_info),
                is_leader,
                Arc::clone(&cmd_board),
                Arc::clone(&spec_pool),
                uncommitted_pool,
                Arc::clone(&curp_cfg),
                Arc::clone(&ce_event_tx),
                sync_events,
                log_tx,
                role_change,
                shutdown_trigger,
                connects,
            ))
        } else {
            info!(
                "{} recovered voted_for({voted_for:?}), entries from {:?} to {:?}",
                cluster_info.self_id(),
                entries.first(),
                entries.last()
            );
            Arc::new(RawCurp::recover_from(
                Arc::clone(&cluster_info),
                is_leader,
                Arc::clone(&cmd_board),
                Arc::clone(&spec_pool),
                uncommitted_pool,
                &curp_cfg,
                Arc::clone(&ce_event_tx),
                sync_events,
                log_tx,
                voted_for,
                entries,
                last_applied,
                role_change,
                shutdown_trigger,
                connects,
            ))
        };

        start_cmd_workers(
            Arc::clone(&cmd_executor),
            Arc::clone(&curp),
            task_rx,
            done_tx,
            shutdown_listener.clone(),
        );
        run_gc_tasks(
            Arc::clone(&cmd_board),
            Arc::clone(&spec_pool),
            curp_cfg.gc_interval,
            shutdown_listener.clone(),
        );

        Self::run_bg_tasks(Arc::clone(&curp), Arc::clone(&storage), log_rx);

        Ok(Self {
            curp,
            spec_pool,
            cmd_board,
            ce_event_tx,
            storage,
            snapshot_allocator,
        })
    }

    /// Run background tasks for Curp server
    fn run_bg_tasks(
        curp: Arc<RawCurp<C, RC>>,
        storage: Arc<impl StorageApi<Command = C> + 'static>,
        log_rx: mpsc::UnboundedReceiver<Arc<LogEntry<C>>>,
    ) {
        let shutdown_listener = curp.shutdown_listener();
        let _election_task = tokio::spawn(Self::election_task(Arc::clone(&curp)));
        let _sync_followers_daemon = tokio::spawn(Self::sync_followers_daemon(curp));
        let _log_persist_task =
            tokio::spawn(Self::log_persist_task(log_rx, storage, shutdown_listener));
    }

    /// Candidate broadcasts votes
    async fn bcast_vote(curp: &RawCurp<C, RC>, vote: Vote) {
        debug!("{} broadcasts votes to all servers", curp.id());
        let rpc_timeout = curp.cfg().rpc_timeout;
        let voters_connects = curp.voters_connects();
        let resps = voters_connects
            .into_iter()
            .map(|connect| {
                let req = VoteRequest::new(
                    vote.term,
                    vote.candidate_id,
                    vote.last_log_index,
                    vote.last_log_term,
                );
                async move {
                    let resp = connect.vote(req, rpc_timeout).await;
                    (connect.id(), resp)
                }
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|(id, resp)| async move {
                match resp {
                    Err(e) => {
                        warn!("request vote from {id} failed, {e}");
                        None
                    }
                    Ok(resp) => Some((id, resp.into_inner())),
                }
            });
        pin_mut!(resps);
        while let Some((id, resp)) = resps.next().await {
            // collect follower spec pool
            let follower_spec_pool = match resp.spec_pool() {
                Err(e) => {
                    error!("can't deserialize spec_pool from vote response, {e}");
                    continue;
                }
                Ok(spec_pool) => spec_pool.into_iter().collect(),
            };
            let result =
                curp.handle_vote_resp(id, resp.term, resp.vote_granted, follower_spec_pool);
            match result {
                Ok(false) => {}
                Ok(true) | Err(()) => return,
            }
        }
    }

    /// Get a rx for leader changes
    pub(super) fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.curp.leader_rx()
    }

    /// Send `append_entries` request
    #[allow(clippy::integer_arithmetic)] // won't overflow
    async fn send_ae(
        connect: &(impl InnerConnectApi + ?Sized),
        curp: &RawCurp<C, RC>,
        ae: AppendEntries<C>,
    ) -> Result<(), SendAEError> {
        let last_sent_index = (!ae.entries.is_empty())
            .then(|| ae.prev_log_index + ae.entries.len().numeric_cast::<u64>());
        let is_heartbeat = ae.entries.is_empty();
        let req = AppendEntriesRequest::new(
            ae.term,
            ae.leader_id,
            ae.prev_log_index,
            ae.prev_log_term,
            ae.entries,
            ae.leader_commit,
        )?;

        if is_heartbeat {
            trace!("{} send heartbeat to {}", curp.id(), connect.id());
        } else {
            debug!("{} send append_entries to {}", curp.id(), connect.id());
        }

        let resp = connect
            .append_entries(req, curp.cfg().rpc_timeout)
            .await?
            .into_inner();

        let succeeded = curp
            .handle_append_entries_resp(
                connect.id(),
                last_sent_index,
                resp.term,
                resp.success,
                resp.hint_index,
            )
            .map_err(|_e| SendAEError::NotLeader)?;

        if succeeded {
            Ok(())
        } else {
            Err(SendAEError::Rejected)
        }
    }

    /// Send snapshot
    async fn send_snapshot(
        connect: &(impl InnerConnectApi + ?Sized),
        curp: &RawCurp<C, RC>,
        snapshot: Snapshot,
    ) -> Result<(), SendSnapshotError> {
        let meta = snapshot.meta;
        let resp = connect
            .install_snapshot(curp.term(), curp.id(), snapshot)
            .await?
            .into_inner();
        curp.handle_snapshot_resp(connect.id(), meta, resp.term)
            .map_err(|_e| SendSnapshotError::NotLeader)
    }

    /// Get a shutdown listener
    pub(super) fn shutdown_listener(&self) -> shutdown::Listener {
        self.curp.shutdown_listener()
    }

    /// Check cluster version and return new cluster
    fn check_cluster_version(&self, client_cluster_version: u64) -> Result<(), CurpError> {
        let server_cluster_version = self.curp.cluster().cluster_version();
        if client_cluster_version != server_cluster_version {
            debug!(
                "client cluster version({}) and server cluster version({}) not match",
                client_cluster_version, server_cluster_version
            );
            return Err(CurpError::WrongClusterVersion);
        }
        Ok(())
    }
}

impl<C: Command, RC: RoleChange> Debug for CurpNode<C, RC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurpNode")
            .field("raw_curp", &self.curp)
            .field("spec_pool", &self.spec_pool)
            .field("cmd_board", &self.cmd_board)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};

    use curp_test_utils::{mock_role_change, sleep_secs, test_cmd::TestCommand};
    use tokio::sync::oneshot;
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        rpc::connect::MockInnerConnectApi, server::cmd_worker::MockCEEventTxApi, ConfChange,
    };

    fn get_error_label(status: &Status) -> &str {
        let metadata = status.metadata();
        metadata
            .get(ERROR_LABEL)
            .expect("error-label should not be None in CurpError")
            .to_str()
            .expect("error-label must be construct by ascii char")
    }

    #[traced_test]
    #[tokio::test]
    async fn sync_task_will_send_hb() {
        let curp = Arc::new(RawCurp::new_test(
            3,
            MockCEEventTxApi::<TestCommand>::default(),
            mock_role_change(),
        ));
        let mut mock_connect1 = MockInnerConnectApi::default();
        mock_connect1
            .expect_append_entries()
            .times(1..)
            .returning(|_, _| Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))));
        let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
        mock_connect1.expect_id().return_const(s1_id);
        let remove_event = Arc::new(Event::new());
        tokio::spawn(CurpNode::sync_follower_task(
            Arc::clone(&curp),
            InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect1)),
            Arc::new(Event::new()),
            remove_event,
        ));
        sleep_secs(2).await;
        curp.shutdown_trigger().self_shutdown_and_wait().await;
    }

    #[traced_test]
    #[tokio::test]
    async fn tick_task_will_bcast_votes() {
        let curp = {
            let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
            exe_tx
                .expect_send_reset()
                .returning(|_| oneshot::channel().1);
            Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
        };
        let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
        curp.handle_append_entries(1, s2_id, 0, 0, vec![], 0)
            .unwrap();

        let mut mock_connect1 = MockInnerConnectApi::default();
        mock_connect1.expect_vote().returning(|req, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(req.term, vec![]).unwrap(),
            ))
        });
        let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
        mock_connect1.expect_id().return_const(s1_id);
        curp.set_connect(
            s1_id,
            InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect1)),
        );

        let mut mock_connect2 = MockInnerConnectApi::default();
        mock_connect2.expect_vote().returning(|req, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(req.term, vec![]).unwrap(),
            ))
        });
        let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
        mock_connect2.expect_id().return_const(s2_id);
        curp.set_connect(
            s2_id,
            InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect2)),
        );
        tokio::spawn(CurpNode::election_task(Arc::clone(&curp)));
        sleep_secs(3).await;
        assert!(curp.is_leader());
        curp.shutdown_trigger().self_shutdown_and_wait().await;
    }

    #[traced_test]
    #[tokio::test]
    async fn vote_will_not_send_to_learner_during_election() {
        let curp = {
            let exe_tx = MockCEEventTxApi::<TestCommand>::default();
            Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
        };

        let learner_id = 123;
        let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
        let s2_id = curp.cluster().get_id_by_name("S2").unwrap();

        let _ig = curp.apply_conf_change(vec![ConfChange::add_learner(
            learner_id,
            vec!["address".to_owned()],
        )]);

        curp.handle_append_entries(1, s2_id, 0, 0, vec![], 0)
            .unwrap();

        let mut mock_connect1 = MockInnerConnectApi::default();
        mock_connect1.expect_vote().returning(|req, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(req.term, vec![]).unwrap(),
            ))
        });
        mock_connect1.expect_id().return_const(s1_id);
        curp.set_connect(
            s1_id,
            InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect1)),
        );

        let mut mock_connect2 = MockInnerConnectApi::default();
        mock_connect2.expect_vote().returning(|req, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(req.term, vec![]).unwrap(),
            ))
        });
        mock_connect2.expect_id().return_const(s2_id);
        curp.set_connect(
            s2_id,
            InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect2)),
        );

        let mut mock_connect_learner = MockInnerConnectApi::default();
        mock_connect_learner
            .expect_vote()
            .returning(|_, _| panic!("should not send vote to learner"));
        curp.set_connect(
            learner_id,
            InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect_learner)),
        );
        tokio::spawn(CurpNode::election_task(Arc::clone(&curp)));
        sleep_secs(3).await;
        assert!(curp.is_leader());
        curp.shutdown_trigger().self_shutdown_and_wait().await;
    }

    #[test]
    fn curp_error_convert_to_tonic_status_should_success() {
        let encode_decode = CurpError::EncodeDecode("CurpError::EncodeDecode".to_owned());
        let status: Status = encode_decode.into();
        assert_eq!("encode-decode", get_error_label(&status));

        let internal = CurpError::Internal("CurpError::Internal".to_owned());
        let status: Status = internal.into();
        assert_eq!("internal", get_error_label(&status));

        let transport = CurpError::Transport("CurpError::Transport".to_owned());
        let status: Status = transport.into();
        assert_eq!("transport", get_error_label(&status));

        let shutdown = CurpError::ShuttingDown;
        let status: Status = shutdown.into();
        assert_eq!("shutting-down", get_error_label(&status));

        let redirect_1 = CurpError::Redirect(Some(1), 2);
        let status: Status = redirect_1.into();
        assert_eq!("redirect", get_error_label(&status));
        let (leader_id, term): (Option<u64>, u64) = serde_json::from_slice(status.details())
            .expect(" deserialize (leader_id, term) from status' detail should always success");
        assert_eq!(leader_id, Some(1));
        assert_eq!(term, 2);

        let redirect_2 = CurpError::Redirect(None, 2);
        let status: Status = redirect_2.into();
        assert_eq!("redirect", get_error_label(&status));
        let (leader_id, term): (Option<u64>, u64) = serde_json::from_slice(status.details())
            .expect(" deserialize (leader_id, term) from status' detail should always success");
        assert_eq!(leader_id, None);
        assert_eq!(term, 2);

        let io = CurpError::IO(Error::new(ErrorKind::Other, "oh no!"));
        let status: Status = io.into();
        assert_eq!("io", get_error_label(&status));

        let bincode_err = Box::new(bincode::ErrorKind::Custom("StorageError".to_owned()));
        let storage = CurpError::Storage(bincode_err.into());
        let status: Status = storage.into();
        assert_eq!("storage", get_error_label(&status));
    }
}
