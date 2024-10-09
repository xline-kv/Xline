use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use engine::{SnapshotAllocator, SnapshotApi};
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use madsim::rand::{thread_rng, Rng};
use opentelemetry::KeyValue;
use parking_lot::{Mutex, RwLock};
use tokio::{
    sync::{broadcast, oneshot},
    time::MissedTickBehavior,
};
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, trace, warn};
#[cfg(madsim)]
use utils::ClientTlsConfig;
use utils::{
    barrier::IdBarrier,
    config::CurpConfig,
    task_manager::{tasks::TaskName, Listener, State, TaskManager},
};

use super::{
    cmd_board::{CmdBoardRef, CommandBoard},
    cmd_worker::execute,
    conflict::spec_pool_new::{SpObject, SpeculativePool},
    conflict::uncommitted_pool::{UcpObject, UncommittedPool},
    gc::gc_client_lease,
    lease_manager::LeaseManager,
    raw_curp::{AppendEntries, RawCurp, Vote},
    storage::StorageApi,
};
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    members::{ClusterInfo, ServerId},
    response::ResponseSender,
    role_change::RoleChange,
    rpc::{
        self,
        connect::{InnerConnectApi, InnerConnectApiWrapper},
        AppendEntriesRequest, AppendEntriesResponse, ConfChange, ConfChangeType, CurpError,
        FetchClusterRequest, FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, LeaseKeepAliveMsg, MoveLeaderRequest,
        MoveLeaderResponse, PoolEntry, ProposeConfChangeRequest, ProposeConfChangeResponse,
        ProposeId, ProposeRequest, ProposeResponse, PublishRequest, PublishResponse,
        ReadIndexResponse, RecordRequest, RecordResponse, ShutdownRequest, ShutdownResponse,
        SyncedResponse, TriggerShutdownRequest, TriggerShutdownResponse, TryBecomeLeaderNowRequest,
        TryBecomeLeaderNowResponse, VoteRequest, VoteResponse,
    },
    server::{
        cmd_worker::{after_sync, worker_reset, worker_snapshot},
        metrics,
        raw_curp::SyncAction,
        storage::db::DB,
    },
    snapshot::{Snapshot, SnapshotMeta},
};

/// After sync entry, composed of a log entry and response sender
pub(crate) type AfterSyncEntry<C> = (Arc<LogEntry<C>>, Option<Arc<ResponseSender>>);

/// The after sync task type
#[derive(Debug)]
pub(super) enum TaskType<C: Command> {
    /// After sync an entry
    Entries(Vec<AfterSyncEntry<C>>),
    /// Reset the CE
    Reset(Option<Snapshot>, oneshot::Sender<()>),
    /// Snapshot
    Snapshot(SnapshotMeta, oneshot::Sender<Snapshot>),
}

/// A propose type
pub(super) struct Propose<C> {
    /// The command of the propose
    pub(super) cmd: Arc<C>,
    /// Propose id
    pub(super) id: ProposeId,
    /// Term the client proposed
    /// NOTE: this term should be equal to the cluster's latest term
    /// for the propose to be accepted.
    pub(super) term: u64,
    /// Tx used for sending the streaming response back to client
    pub(super) resp_tx: Arc<ResponseSender>,
}

impl<C> Propose<C>
where
    C: Command,
{
    /// Attempts to create a new `Propose` from request
    fn try_new(req: &ProposeRequest, resp_tx: Arc<ResponseSender>) -> Result<Self, CurpError> {
        let cmd: Arc<C> = Arc::new(req.cmd()?);
        Ok(Self {
            cmd,
            id: req.propose_id(),
            term: req.term,
            resp_tx,
        })
    }

    /// Returns `true` if the proposed command is read-only
    fn is_read_only(&self) -> bool {
        self.cmd.is_read_only()
    }

    /// Gets response sender
    fn response_tx(&self) -> Arc<ResponseSender> {
        Arc::clone(&self.resp_tx)
    }

    /// Convert self into parts
    fn into_parts(self) -> (Arc<C>, ProposeId, u64, Arc<ResponseSender>) {
        let Self {
            cmd,
            id,
            term,
            resp_tx,
        } = self;
        (cmd, id, term, resp_tx)
    }
}

/// Entry to execute
type ExecutorEntry<C> = (Arc<LogEntry<C>>, Arc<ResponseSender>);

/// `CurpNode` represents a single node of curp cluster
pub(super) struct CurpNode<C: Command, CE: CommandExecutor<C>, RC: RoleChange> {
    /// `RawCurp` state machine
    curp: Arc<RawCurp<C, RC>>,
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: CmdBoardRef<C>,
    /// Storage
    storage: Arc<dyn StorageApi<Command = C>>,
    /// Snapshot allocator
    snapshot_allocator: Box<dyn SnapshotAllocator>,
    /// Command Executor
    #[allow(unused)]
    cmd_executor: Arc<CE>,
    /// Tx to send entries to after_sync
    as_tx: flume::Sender<TaskType<C>>,
    /// Tx to send to propose task
    propose_tx: flume::Sender<Propose<C>>,
}

/// Handlers for clients
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Handle `ProposeStream` requests
    pub(super) async fn propose_stream(
        &self,
        req: &ProposeRequest,
        resp_tx: Arc<ResponseSender>,
        bypassed: bool,
    ) -> Result<(), CurpError> {
        if self.curp.is_cluster_shutdown() {
            return Err(CurpError::shutting_down());
        }
        self.curp.check_leader_transfer()?;
        self.check_cluster_version(req.cluster_version)?;
        self.curp.check_term(req.term)?;

        if req.slow_path {
            resp_tx.set_conflict(true);
        } else {
            info!("not using slow path for: {req:?}");
        }

        if bypassed {
            self.curp.mark_client_id_bypassed(req.propose_id().0);
        }

        match self
            .curp
            .deduplicate(req.propose_id(), Some(req.first_incomplete))
        {
            // If the propose is duplicated, return the result directly
            Err(CurpError::Duplicated(())) => {
                let (er, asr) =
                    CommandBoard::wait_for_er_asr(&self.cmd_board, req.propose_id()).await;
                resp_tx.send_propose(ProposeResponse::new_result::<C>(&er, true));
                resp_tx.send_synced(SyncedResponse::new_result::<C>(&asr));
            }
            Err(CurpError::ExpiredClientId(())) => {
                metrics::get()
                    .proposals_failed
                    .add(1, &[KeyValue::new("reason", "duplicated proposal")]);
                return Err(CurpError::expired_client_id());
            }
            Err(_) => unreachable!("deduplicate won't return other type of errors"),
            Ok(()) => {}
        }

        let propose = Propose::try_new(req, resp_tx)?;
        let _ignore = self.propose_tx.send(propose);

        Ok(())
    }

    /// Handle `Record` requests
    pub(super) fn record(&self, req: &RecordRequest) -> Result<RecordResponse, CurpError> {
        if self.curp.is_cluster_shutdown() {
            return Err(CurpError::shutting_down());
        }
        let id = req.propose_id();
        let cmd: Arc<C> = Arc::new(req.cmd()?);
        let conflict = self.curp.follower_record(id, &cmd);

        Ok(RecordResponse { conflict })
    }

    /// Handle `Record` requests
    pub(super) fn read_index(&self) -> Result<ReadIndexResponse, CurpError> {
        if self.curp.is_cluster_shutdown() {
            return Err(CurpError::shutting_down());
        }
        Ok(ReadIndexResponse {
            term: self.curp.term(),
        })
    }

    /// Handle propose task
    async fn handle_propose_task(
        ce: Arc<CE>,
        curp: Arc<RawCurp<C, RC>>,
        rx: flume::Receiver<Propose<C>>,
    ) {
        /// Max number of propose in a batch
        const MAX_BATCH_SIZE: usize = 1024;

        let cmd_executor = Self::build_executor(ce, Arc::clone(&curp));
        loop {
            let Ok(first) = rx.recv_async().await else {
                info!("handle propose task exit");
                break;
            };
            let mut addition: Vec<_> = std::iter::repeat_with(|| rx.try_recv())
                .take(MAX_BATCH_SIZE)
                .flatten()
                .collect();
            addition.push(first);
            let (read_onlys, mutatives): (Vec<_>, Vec<_>) =
                addition.into_iter().partition(Propose::is_read_only);

            Self::handle_read_onlys(cmd_executor.clone(), &curp, read_onlys);
            Self::handle_mutatives(cmd_executor.clone(), &curp, mutatives);
        }
    }

    /// Handle read-only proposes
    fn handle_read_onlys<Executor>(
        cmd_executor: Executor,
        curp: &RawCurp<C, RC>,
        proposes: Vec<Propose<C>>,
    ) where
        Executor: Fn(ExecutorEntry<C>) + Clone + Send + 'static,
    {
        for propose in proposes {
            info!("handle read only cmd: {:?}", propose.cmd);
            // TODO: Disable dedup if the command is read only or commute
            let Propose {
                cmd, resp_tx, id, ..
            } = propose;
            // Use default value for the entry as we don't need to put it into curp log
            let entry = Arc::new(LogEntry::new(0, 0, id, Arc::clone(&cmd)));
            let wait_conflict = curp.wait_conflicts_synced(cmd);
            let wait_no_op = curp.wait_no_op_applied();
            let cmd_executor_c = cmd_executor.clone();
            let _ignore = tokio::spawn(async move {
                tokio::join!(wait_conflict, wait_no_op);
                cmd_executor_c((entry, resp_tx));
            });
        }
    }

    /// Handle read-only proposes
    fn handle_mutatives<Executor>(
        cmd_executor: Executor,
        curp: &RawCurp<C, RC>,
        proposes: Vec<Propose<C>>,
    ) where
        Executor: Fn(ExecutorEntry<C>),
    {
        if proposes.is_empty() {
            return;
        }
        let pool_entries = proposes
            .iter()
            .map(|p| PoolEntry::new(p.id, Arc::clone(&p.cmd)));
        let conflicts = curp.leader_record(pool_entries);
        for (p, conflict) in proposes.iter().zip(conflicts) {
            info!("handle mutative cmd: {:?}, conflict: {conflict}", p.cmd);
            p.resp_tx.set_conflict(conflict);
        }
        let resp_txs: Vec<_> = proposes.iter().map(Propose::response_tx).collect();
        let logs: Vec<_> = proposes.into_iter().map(Propose::into_parts).collect();
        let entries = curp.push_logs(logs);
        #[allow(clippy::pattern_type_mismatch)] // Can't be fixed
        entries
            .into_iter()
            .zip(resp_txs)
            .filter(|(_, tx)| !tx.is_conflict())
            .for_each(cmd_executor);
    }

    /// Speculatively execute a command
    fn build_executor(ce: Arc<CE>, curp: Arc<RawCurp<C, RC>>) -> impl Fn(ExecutorEntry<C>) + Clone {
        move |(entry, resp_tx): (_, Arc<ResponseSender>)| {
            info!("spec execute entry: {entry:?}");
            let result = execute(&entry, ce.as_ref(), curp.as_ref());
            match result {
                Ok((er, Some(asr))) => {
                    resp_tx.send_propose(ProposeResponse::new_result::<C>(&Ok(er), false));
                    resp_tx.send_synced(SyncedResponse::new_result::<C>(&Ok(asr)));
                }
                Ok((er, None)) => {
                    resp_tx.send_propose(ProposeResponse::new_result::<C>(&Ok(er), false));
                }
                Err(e) => resp_tx.send_err::<C>(e),
            }
        }
    }

    /// Handle `Shutdown` requests
    pub(super) async fn shutdown(
        &self,
        req: ShutdownRequest,
        bypassed: bool,
    ) -> Result<ShutdownResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        if bypassed {
            self.curp.mark_client_id_bypassed(req.propose_id().0);
        }
        self.curp.handle_shutdown(req.propose_id())?;
        CommandBoard::wait_for_shutdown_synced(&self.cmd_board).await;
        Ok(ShutdownResponse::default())
    }

    /// Handle `ProposeConfChange` requests
    pub(super) async fn propose_conf_change(
        &self,
        req: ProposeConfChangeRequest,
        bypassed: bool,
    ) -> Result<ProposeConfChangeResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        let id = req.propose_id();
        if bypassed {
            self.curp.mark_client_id_bypassed(id.0);
        }
        self.curp.handle_propose_conf_change(id, req.changes)?;
        CommandBoard::wait_for_conf(&self.cmd_board, id).await;
        let members = self.curp.cluster().all_members_vec();
        Ok(ProposeConfChangeResponse { members })
    }

    /// Handle `Publish` requests
    pub(super) fn publish(
        &self,
        req: PublishRequest,
        bypassed: bool,
    ) -> Result<PublishResponse, CurpError> {
        if bypassed {
            self.curp.mark_client_id_bypassed(req.propose_id().0);
        }
        self.curp.handle_publish(req)?;
        Ok(PublishResponse::default())
    }

    /// Handle lease keep alive requests
    pub(super) async fn lease_keep_alive<E: std::error::Error + 'static>(
        &self,
        req_stream: impl Stream<Item = Result<LeaseKeepAliveMsg, E>>,
    ) -> Result<LeaseKeepAliveMsg, CurpError> {
        pin_mut!(req_stream);
        while let Some(req) = req_stream.next().await {
            // NOTE: The leader may shutdown itself in configuration change.
            // We must first check this situation.
            self.curp.check_leader_transfer()?;
            if self.curp.is_cluster_shutdown() {
                return Err(CurpError::shutting_down());
            }
            if self.curp.is_node_shutdown() {
                return Err(CurpError::node_not_exist());
            }
            if !self.curp.is_leader() {
                let (leader_id, term, _) = self.curp.leader();
                return Err(CurpError::redirect(leader_id, term));
            }
            let req = req.map_err(|err| {
                error!("{err}");
                CurpError::RpcTransport(())
            })?;
            if let Some(client_id) = self.curp.handle_lease_keep_alive(req.client_id) {
                return Ok(LeaseKeepAliveMsg { client_id });
            }
        }
        Err(CurpError::RpcTransport(()))
    }
}

/// Handlers for peers
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
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
            Ok((term, to_persist)) => {
                self.storage
                    .put_log_entries(&to_persist.iter().map(Arc::as_ref).collect::<Vec<_>>())?;
                AppendEntriesResponse::new_accept(term)
            }
            Err((term, hint)) => AppendEntriesResponse::new_reject(term, hint),
        };

        Ok(resp)
    }

    /// Handle `Vote` requests
    pub(super) fn vote(&self, req: &VoteRequest) -> Result<VoteResponse, CurpError> {
        let result = if req.is_pre_vote {
            self.curp.handle_pre_vote(
                req.term,
                req.candidate_id,
                req.last_log_index,
                req.last_log_term,
            )
        } else {
            self.curp.handle_vote(
                req.term,
                req.candidate_id,
                req.last_log_index,
                req.last_log_term,
            )
        };

        let resp = match result {
            Ok((term, sp)) => {
                if !req.is_pre_vote {
                    self.storage.flush_voted_for(term, req.candidate_id)?;
                }
                VoteResponse::new_accept(term, sp)?
            }
            Err(Some(term)) => VoteResponse::new_reject(term),
            Err(None) => VoteResponse::new_shutdown(),
        };

        Ok(resp)
    }

    /// Handle `TriggerShutdown` requests
    pub(super) fn trigger_shutdown(&self, _req: TriggerShutdownRequest) -> TriggerShutdownResponse {
        self.curp.task_manager().mark_leader_notified();
        TriggerShutdownResponse::default()
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
    #[allow(clippy::arithmetic_side_effects)] // can't overflow
    pub(super) async fn install_snapshot<E: std::error::Error + 'static>(
        &self,
        req_stream: impl Stream<Item = Result<InstallSnapshotRequest, E>>,
    ) -> Result<InstallSnapshotResponse, CurpError> {
        metrics::get().apply_snapshot_in_progress.add(1, &[]);
        let start = Instant::now();
        pin_mut!(req_stream);
        let mut snapshot = self
            .snapshot_allocator
            .allocate_new_snapshot()
            .await
            .map_err(|err| {
                error!("failed to allocate a new snapshot, error: {err}");
                CurpError::internal(format!("failed to allocate a new snapshot, error: {err}"))
            })?;
        while let Some(req) = req_stream.next().await {
            let req = req?;
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
                let (tx, rx) = oneshot::channel();
                self.as_tx.send(TaskType::Reset(Some(snapshot), tx))?;
                rx.await.map_err(|err| {
                    error!("failed to reset the command executor by snapshot, {err}");
                    CurpError::internal(format!(
                        "failed to reset the command executor by snapshot, {err}"
                    ))
                })?;
                metrics::get().apply_snapshot_in_progress.add(-1, &[]);
                metrics::get()
                    .snapshot_install_total_duration_seconds
                    .record(start.elapsed().as_secs(), &[]);
                return Ok(InstallSnapshotResponse::new(self.curp.term()));
            }
        }
        Err(CurpError::internal(
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
        let state = self.curp.handle_fetch_read_state(Arc::new(cmd));
        Ok(FetchReadStateResponse::new(state))
    }

    /// Handle `MoveLeader` requests
    pub(super) async fn move_leader(
        &self,
        req: MoveLeaderRequest,
    ) -> Result<MoveLeaderResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        let should_send_try_become_leader_now = self.curp.handle_move_leader(req.node_id)?;
        if should_send_try_become_leader_now {
            if let Err(e) = self
                .curp
                .connects()
                .get(&req.node_id)
                .unwrap_or_else(|| unreachable!("connect to {} should exist", req.node_id))
                .try_become_leader_now(self.curp.cfg().rpc_timeout)
                .await
            {
                warn!(
                    "{} send try become leader now to {} failed: {:?}",
                    self.curp.id(),
                    req.node_id,
                    e
                );
            };
        }

        let mut ticker = tokio::time::interval(self.curp.cfg().heartbeat_interval);
        let mut current_leader = self.curp.leader().0;
        while !current_leader.is_some_and(|id| id == req.node_id) {
            if self.curp.get_transferee().is_none()
                && current_leader.is_some_and(|id| id != req.node_id)
            {
                return Err(CurpError::LeaderTransfer(
                    "leader transferee aborted".to_owned(),
                ));
            };
            _ = ticker.tick().await;
            current_leader = self.curp.leader().0;
        }
        Ok(MoveLeaderResponse::default())
    }

    /// Handle `TryBecomeLeaderNow` request
    pub(super) async fn try_become_leader_now(
        &self,
        _req: &TryBecomeLeaderNowRequest,
    ) -> Result<TryBecomeLeaderNowResponse, CurpError> {
        if let Some(vote) = self.curp.handle_try_become_leader_now() {
            _ = Self::bcast_vote(self.curp.as_ref(), vote).await;
        }
        Ok(TryBecomeLeaderNowResponse::default())
    }
}

/// Spawned tasks
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Tick periodically
    #[allow(clippy::arithmetic_side_effects, clippy::ignored_unit_patterns)]
    async fn election_task(curp: Arc<RawCurp<C, RC>>, shutdown_listener: Listener) {
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
                _ = shutdown_listener.wait() => {
                    debug!("election task exits");
                    return;
                }
            }
            if let Some(pre_vote_or_vote) = curp.tick_election() {
                // bcast pre vote or vote, if it is a pre vote and success, it will return Some(vote)
                // then we need to bcast normal vote, and bcast normal vote always return None
                if let Some(vote) = Self::bcast_vote(curp.as_ref(), pre_vote_or_vote.clone()).await
                {
                    debug_assert!(
                        !vote.is_pre_vote,
                        "bcast pre vote should return Some(normal_vote)"
                    );
                    let opt = Self::bcast_vote(curp.as_ref(), vote).await;
                    debug_assert!(opt.is_none(), "bcast normal vote should always return None");
                }
            }
        }
    }

    /// Handler of conf change
    async fn conf_change_handler(
        curp: Arc<RawCurp<C, RC>>,
        mut remove_events: HashMap<ServerId, Arc<Event>>,
        shutdown_listener: Listener,
    ) {
        let task_manager = curp.task_manager();
        let change_rx = curp.change_rx();
        #[allow(clippy::arithmetic_side_effects, clippy::ignored_unit_patterns)]
        // introduced by tokio select
        loop {
            let change: ConfChange = tokio::select! {
                _ = shutdown_listener.wait() => break,
                change_res = change_rx.recv_async() => {
                    let Ok(change) = change_res else {
                        break;
                    };
                    change
                },
            };
            match change.change_type() {
                ConfChangeType::Add | ConfChangeType::AddLearner => {
                    let connect = InnerConnectApiWrapper::connect(
                        change.node_id,
                        change.address,
                        curp.client_tls_config().cloned(),
                    );
                    curp.insert_connect(connect.clone());
                    let sync_event = curp.sync_event(change.node_id);
                    let remove_event = Arc::new(Event::new());

                    task_manager.spawn(TaskName::SyncFollower, |n| {
                        Self::sync_follower_task(
                            Arc::clone(&curp),
                            connect,
                            sync_event,
                            Arc::clone(&remove_event),
                            n,
                        )
                    });
                    _ = remove_events.insert(change.node_id, remove_event);
                }
                ConfChangeType::Remove => {
                    if change.node_id == curp.id() {
                        break;
                    }
                    let Some(event) = remove_events.remove(&change.node_id) else {
                        unreachable!(
                            "({:?}) shutdown_event of removed follower ({:x}) should exist",
                            curp.id(),
                            change.node_id
                        );
                    };
                    let _ignore = event.notify(1);
                }
                ConfChangeType::Update => {
                    if let Err(e) = curp.update_connect(change.node_id, change.address).await {
                        error!("update connect {} failed, err {:?}", change.node_id, e);
                        continue;
                    }
                }
                ConfChangeType::Promote => {}
            }
        }
    }

    /// This task will keep a follower up-to-data when current node is leader,
    /// and it will wait for `leader_event` if current node is not leader
    #[allow(clippy::arithmetic_side_effects, clippy::ignored_unit_patterns)] // tokio select internal triggered
    async fn sync_follower_task(
        curp: Arc<RawCurp<C, RC>>,
        connect: InnerConnectApiWrapper,
        sync_event: Arc<Event>,
        remove_event: Arc<Event>,
        shutdown_listener: Listener,
    ) {
        debug!("{} to {} sync follower task start", curp.id(), connect.id());
        let _guard = shutdown_listener.sync_follower_guard();
        let mut ticker = tokio::time::interval(curp.cfg().heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let connect_id = connect.id();
        let batch_timeout = curp.cfg().batch_timeout;
        let leader_event = curp.leader_event();

        if !curp.is_leader() {
            tokio::select! {
                _ = shutdown_listener.wait_state() => return,
                _ = remove_event.listen() => return,
                _ = leader_event.listen() => {}
            }
        }
        let mut hb_opt = false;
        let mut is_shutdown_state = false;
        let mut ae_fail_count = 0;
        loop {
            // a sync is either triggered by an heartbeat timeout event or when new log entries arrive
            tokio::select! {
                state = shutdown_listener.wait_state(), if !is_shutdown_state => {
                    match state {
                        State::Running => unreachable!("wait state should not return Run"),
                        State::Shutdown => return,
                        State::ClusterShutdown => is_shutdown_state = true,
                    }
                },
                _ = remove_event.listen() => return,
                _now = ticker.tick() => hb_opt = false,
                res = tokio::time::timeout(batch_timeout, sync_event.listen()) => {
                    if let Err(_e) = res {
                        hb_opt = true;
                    }
                }
            }

            let Some(sync_action) = curp.sync(connect_id) else {
                break;
            };
            if Self::handle_sync_action(
                sync_action,
                &mut hb_opt,
                is_shutdown_state,
                &mut ae_fail_count,
                connect.as_ref(),
                curp.as_ref(),
            )
            .await
            {
                break;
            };
        }
        debug!("{} to {} sync follower task exits", curp.id(), connect.id());
    }

    /// After sync task
    async fn after_sync_task(
        curp: Arc<RawCurp<C, RC>>,
        cmd_executor: Arc<CE>,
        as_rx: flume::Receiver<TaskType<C>>,
    ) {
        while let Ok(task) = as_rx.recv_async().await {
            Self::handle_as_task(&curp, &cmd_executor, task).await;
        }
        debug!("after sync task exits");
    }

    /// Handles a after sync task
    async fn handle_as_task(curp: &RawCurp<C, RC>, cmd_executor: &CE, task: TaskType<C>) {
        debug!("after sync: {task:?}");
        match task {
            TaskType::Entries(entries) => {
                after_sync(entries, cmd_executor, curp).await;
            }
            TaskType::Reset(snap, tx) => {
                let _ignore = worker_reset(snap, tx, cmd_executor, curp).await;
            }
            TaskType::Snapshot(meta, tx) => {
                let _ignore = worker_snapshot(meta, tx, cmd_executor, curp).await;
            }
        }
    }
}

// utils
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Create a new server instance
    #[inline]
    #[allow(clippy::too_many_arguments)] // TODO: refactor this use builder pattern
    #[allow(clippy::needless_pass_by_value)] // The value should be consumed
    pub(super) fn new(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        cmd_executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        storage: Arc<DB<C>>,
        task_manager: Arc<TaskManager>,
        client_tls_config: Option<ClientTlsConfig>,
        sps: Vec<SpObject<C>>,
        ucps: Vec<UcpObject<C>>,
    ) -> Result<Self, CurpError> {
        let sync_events = cluster_info
            .peers_ids()
            .into_iter()
            .map(|server_id| (server_id, Arc::new(Event::new())))
            .collect();
        let connects =
            rpc::inner_connects(cluster_info.peers_addrs(), client_tls_config.as_ref()).collect();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let lease_manager = Arc::new(RwLock::new(LeaseManager::new()));
        let last_applied = cmd_executor
            .last_applied()
            .map_err(|e| CurpError::internal(format!("get applied index error, {e}")))?;
        let (as_tx, as_rx) = flume::unbounded();
        let (propose_tx, propose_rx) = flume::bounded(4096);
        let sp = Arc::new(Mutex::new(SpeculativePool::new(sps)));
        let ucp = Arc::new(Mutex::new(UncommittedPool::new(ucps)));
        // create curp state machine
        let (voted_for, entries) = storage.recover()?;
        let curp = Arc::new(
            RawCurp::builder()
                .cluster_info(Arc::clone(&cluster_info))
                .is_leader(is_leader)
                .cmd_board(Arc::clone(&cmd_board))
                .lease_manager(Arc::clone(&lease_manager))
                .cfg(Arc::clone(&curp_cfg))
                .sync_events(sync_events)
                .role_change(role_change)
                .task_manager(Arc::clone(&task_manager))
                .connects(connects)
                .last_applied(last_applied)
                .voted_for(voted_for)
                .entries(entries)
                .curp_storage(Arc::clone(&storage))
                .client_tls_config(client_tls_config)
                .spec_pool(Arc::clone(&sp))
                .uncommitted_pool(ucp)
                .as_tx(as_tx.clone())
                .resp_txs(Arc::new(Mutex::default()))
                .id_barrier(Arc::new(IdBarrier::new()))
                .build_raw_curp()
                .map_err(|e| CurpError::internal(format!("build raw curp failed, {e}")))?,
        );

        metrics::Metrics::register_callback(Arc::clone(&curp))?;

        task_manager.spawn(TaskName::GcClientLease, |n| {
            gc_client_lease(
                lease_manager,
                Arc::clone(&cmd_board),
                sp,
                curp_cfg.gc_interval,
                n,
            )
        });

        Self::run_bg_tasks(
            Arc::clone(&curp),
            Arc::clone(&cmd_executor),
            propose_rx,
            as_rx,
        );

        Ok(Self {
            curp,
            cmd_board,
            storage,
            snapshot_allocator,
            cmd_executor,
            as_tx,
            propose_tx,
        })
    }

    /// Run background tasks for Curp server
    fn run_bg_tasks(
        curp: Arc<RawCurp<C, RC>>,
        cmd_executor: Arc<CE>,
        propose_rx: flume::Receiver<Propose<C>>,
        as_rx: flume::Receiver<TaskType<C>>,
    ) {
        let task_manager = curp.task_manager();

        task_manager.spawn(TaskName::Election, |n| {
            Self::election_task(Arc::clone(&curp), n)
        });

        let mut remove_events = HashMap::new();
        for c in curp.connects() {
            let sync_event = curp.sync_event(c.id());
            let remove_event = Arc::new(Event::new());

            task_manager.spawn(TaskName::SyncFollower, |n| {
                Self::sync_follower_task(
                    Arc::clone(&curp),
                    c.value().clone(),
                    sync_event,
                    Arc::clone(&remove_event),
                    n,
                )
            });
            _ = remove_events.insert(c.id(), remove_event);
        }

        task_manager.spawn(TaskName::ConfChange, |n| {
            Self::conf_change_handler(Arc::clone(&curp), remove_events, n)
        });
        task_manager.spawn(TaskName::HandlePropose, |_n| {
            Self::handle_propose_task(Arc::clone(&cmd_executor), Arc::clone(&curp), propose_rx)
        });
        task_manager.spawn(TaskName::AfterSync, |_n| {
            Self::after_sync_task(curp, cmd_executor, as_rx)
        });
    }

    /// Candidate or pre candidate broadcasts votes
    ///
    /// # Returns
    ///
    /// - `Some(vote)` if bcast pre vote and success
    /// - `None` if bcast pre vote and fail or bcast vote
    async fn bcast_vote(curp: &RawCurp<C, RC>, vote: Vote) -> Option<Vote> {
        if vote.is_pre_vote {
            debug!("{} broadcasts pre votes to all servers", curp.id());
        } else {
            debug!("{} broadcasts votes to all servers", curp.id());
        }
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
                    vote.is_pre_vote,
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
            if vote.is_pre_vote {
                if resp.shutdown_candidate {
                    curp.task_manager().shutdown(false).await;
                    return None;
                }
                let result = curp.handle_pre_vote_resp(id, resp.term, resp.vote_granted);
                match result {
                    Ok(None) | Err(()) => {}
                    Ok(Some(v)) => return Some(v),
                }
            } else {
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
                    Ok(true) | Err(()) => return None,
                }
            };
        }
        None
    }

    /// Get a rx for leader changes
    pub(super) fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.curp.leader_rx()
    }

    /// Send `append_entries` request
    /// Return `tonic::Error` if meet network issue
    /// Return (`leader_retires`, `ae_succeed`)
    #[allow(clippy::arithmetic_side_effects)] // won't overflow
    async fn send_ae(
        connect: &(impl InnerConnectApi + ?Sized),
        curp: &RawCurp<C, RC>,
        ae: AppendEntries<C>,
    ) -> Result<(bool, bool), CurpError> {
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

        let Ok(ae_succeed) = curp.handle_append_entries_resp(
            connect.id(),
            last_sent_index,
            resp.term,
            resp.success,
            resp.hint_index,
        ) else {
            return Ok((true, false));
        };

        Ok((false, ae_succeed))
    }

    /// Send snapshot
    /// Return `tonic::Error` if meet network issue
    /// Return `leader_retires`
    async fn send_snapshot(
        connect: &(impl InnerConnectApi + ?Sized),
        curp: &RawCurp<C, RC>,
        snapshot: Snapshot,
    ) -> Result<bool, CurpError> {
        let meta = snapshot.meta;
        let resp = connect
            .install_snapshot(curp.term(), curp.id(), snapshot)
            .await?
            .into_inner();
        Ok(curp
            .handle_snapshot_resp(connect.id(), meta, resp.term)
            .is_err())
    }

    /// Check cluster version and return new cluster
    fn check_cluster_version(&self, client_cluster_version: u64) -> Result<(), CurpError> {
        let server_cluster_version = self.curp.cluster().cluster_version();
        if client_cluster_version != server_cluster_version {
            debug!(
                "client cluster version({}) and server cluster version({}) not match",
                client_cluster_version, server_cluster_version
            );
            return Err(CurpError::wrong_cluster_version());
        }
        Ok(())
    }

    /// Get `RawCurp`
    pub(super) fn raw_curp(&self) -> Arc<RawCurp<C, RC>> {
        Arc::clone(&self.curp)
    }

    /// Handle `SyncAction`
    /// If no longer need to sync to this node, return true
    async fn handle_sync_action(
        sync_action: SyncAction<C>,
        hb_opt: &mut bool,
        is_shutdown_state: bool,
        ae_fail_count: &mut u32,
        connect: &(impl InnerConnectApi + ?Sized),
        curp: &RawCurp<C, RC>,
    ) -> bool {
        let connect_id = connect.id();
        match sync_action {
            SyncAction::AppendEntries(ae) => {
                let is_empty = ae.entries.is_empty();
                let is_commit_shutdown = ae.entries.last().is_some_and(|e| {
                    matches!(e.entry_data, EntryData::Shutdown) && e.index == ae.leader_commit
                });
                // (hb_opt, entries) status combination
                // (false, empty) => send heartbeat to followers
                // (true, empty) => indicates that `batch_timeout` expired, and during this period there is not any log generated. Do nothing
                // (true | false, not empty) => send append entries
                if !*hb_opt || !is_empty {
                    match Self::send_ae(connect, curp, ae).await {
                        Ok((true, _)) => return true,
                        Ok((false, ae_succeed)) => {
                            if ae_succeed {
                                *hb_opt = true;
                                if curp
                                    .get_transferee()
                                    .is_some_and(|transferee| transferee == connect_id)
                                    && curp
                                        .get_match_index(connect_id)
                                        .is_some_and(|idx| idx == curp.last_log_index())
                                {
                                    if let Err(e) = connect
                                        .try_become_leader_now(curp.cfg().wait_synced_timeout)
                                        .await
                                    {
                                        warn!(
                                            "{} send try become leader now to {} failed: {:?}",
                                            curp.id(),
                                            connect_id,
                                            e
                                        );
                                    };
                                }
                            } else {
                                debug!("ae rejected by {}", connect.id());
                            }
                            // Check Follower shutdown
                            // When the leader is in the shutdown state, its last log must be shutdown, and if the follower is
                            // already synced with leader and current AE is a heartbeat, then the follower will commit the shutdown
                            // log after AE, or when the follower is not synced with the leader, the current AE will send and directly commit
                            // shutdown log.
                            if is_shutdown_state
                                && ((curp.is_synced(connect_id) && is_empty)
                                    || (!curp.is_synced(connect_id) && is_commit_shutdown))
                            {
                                if let Err(e) = connect.trigger_shutdown().await {
                                    warn!("trigger shutdown to {} failed, {e}", connect_id);
                                } else {
                                    debug!("trigger shutdown to {} success", connect_id);
                                }
                                return true;
                            }
                        }
                        Err(err) => {
                            if is_empty {
                                metrics::get().heartbeat_send_failures.add(1, &[]);
                            }
                            warn!("ae to {} failed, {err:?}", connect.id());
                            if is_shutdown_state {
                                *ae_fail_count = ae_fail_count.overflow_add(1);
                                if *ae_fail_count >= 5 {
                                    warn!("the follower {} may have been shutdown", connect_id);
                                    return true;
                                }
                            }
                        }
                    };
                }
            }
            SyncAction::Snapshot(rx) => match rx.await {
                Ok(snapshot) => match Self::send_snapshot(connect, curp, snapshot).await {
                    Ok(true) => return true,
                    Err(err) => warn!("snapshot to {} failed, {err:?}", connect.id()),
                    Ok(false) => {}
                },
                Err(err) => {
                    warn!("failed to receive snapshot result, {err}");
                }
            },
        }
        false
    }
}

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> Debug for CurpNode<C, CE, RC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurpNode")
            .field("raw_curp", &self.curp)
            .field("cmd_board", &self.cmd_board)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use curp_test_utils::{
        mock_role_change, sleep_secs,
        test_cmd::{TestCE, TestCommand},
    };
    use tracing_test::traced_test;

    use super::*;
    use crate::rpc::{connect::MockInnerConnectApi, ConfChange};

    #[traced_test]
    #[tokio::test]
    async fn sync_task_will_send_hb() {
        let task_manager = Arc::new(TaskManager::new());
        let curp = Arc::new(RawCurp::new_test(
            3,
            mock_role_change(),
            Arc::clone(&task_manager),
        ));
        let mut mock_connect1 = MockInnerConnectApi::default();
        mock_connect1
            .expect_append_entries()
            .times(1..)
            .returning(|_, _| Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))));
        let s1_id = curp.cluster().get_id_by_name("S1").unwrap();
        mock_connect1.expect_id().return_const(s1_id);
        let remove_event = Arc::new(Event::new());
        task_manager.spawn(TaskName::SyncFollower, |n| {
            CurpNode::<_, TestCE, _>::sync_follower_task(
                Arc::clone(&curp),
                InnerConnectApiWrapper::new_from_arc(Arc::new(mock_connect1)),
                Arc::new(Event::new()),
                remove_event,
                n,
            )
        });
        sleep_secs(2).await;
        task_manager.shutdown(true).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn tick_task_will_bcast_votes() {
        let task_manager = Arc::new(TaskManager::new());
        let curp = {
            Arc::new(RawCurp::new_test(
                3,
                mock_role_change(),
                Arc::clone(&task_manager),
            ))
        };
        let s2_id = curp.cluster().get_id_by_name("S2").unwrap();
        curp.handle_append_entries(2, s2_id, 0, 0, vec![], 0)
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
        task_manager.spawn(TaskName::Election, |n| {
            CurpNode::<_, TestCE, _>::election_task(Arc::clone(&curp), n)
        });
        sleep_secs(3).await;
        assert!(curp.is_leader());
        task_manager.shutdown(true).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn vote_will_not_send_to_learner_during_election() {
        let task_manager = Arc::new(TaskManager::new());
        let curp = {
            Arc::new(RawCurp::new_test(
                3,
                mock_role_change(),
                Arc::clone(&task_manager),
            ))
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
        task_manager.spawn(TaskName::Election, |n| {
            CurpNode::<_, TestCE, _>::election_task(Arc::clone(&curp), n)
        });
        sleep_secs(3).await;
        assert!(curp.is_leader());
        task_manager.shutdown(true).await;
    }
}
