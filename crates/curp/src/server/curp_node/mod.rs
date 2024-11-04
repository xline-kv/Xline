use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};

use clippy_utilities::NumericCast;
use engine::{SnapshotAllocator, SnapshotApi};
use futures::{future::join_all, pin_mut, stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use madsim::rand::{thread_rng, Rng};
use parking_lot::{Mutex, RwLock};
use tokio::{sync::oneshot, time::MissedTickBehavior};
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::{debug, error, info, warn};
#[cfg(madsim)]
use utils::ClientTlsConfig;
#[cfg(madsim)]
use utils::{
    barrier::IdBarrier,
    config::CurpConfig,
    task_manager::{tasks::TaskName, Listener, TaskManager},
};
#[cfg(not(madsim))]
use utils::{
    barrier::IdBarrier,
    config::CurpConfig,
    task_manager::{tasks::TaskName, Listener, TaskManager},
};

use super::{
    cmd_board::{CmdBoardRef, CommandBoard},
    cmd_worker::execute,
    conflict::spec_pool_new::{SpObject, SpeculativePool},
    conflict::uncommitted_pool::{UcpObject, UncommittedPool},
    gc::gc_client_lease,
    lease_manager::LeaseManager,
    raw_curp::{RawCurp, Vote},
    storage::StorageApi,
};
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::LogEntry,
    member::{MembershipConfig, MembershipInfo},
    response::ResponseSender,
    role_change::RoleChange,
    rpc::{
        self, AppendEntriesRequest, AppendEntriesResponse, CurpError, FetchMembershipRequest,
        InstallSnapshotRequest, InstallSnapshotResponse, LeaseKeepAliveMsg, MembershipResponse,
        MoveLeaderRequest, MoveLeaderResponse, PoolEntry, ProposeId, ProposeRequest,
        ProposeResponse, ReadIndexResponse, RecordRequest, RecordResponse, ShutdownRequest,
        ShutdownResponse, SyncedResponse, TriggerShutdownRequest, TriggerShutdownResponse,
        TryBecomeLeaderNowRequest, TryBecomeLeaderNowResponse, VoteRequest, VoteResponse,
    },
    server::{
        cmd_worker::{after_sync, worker_reset, worker_snapshot},
        metrics,
        storage::db::DB,
    },
    snapshot::{Snapshot, SnapshotMeta},
};

/// `CurpNode` member implementation
mod member_impl;

/// Log replication implementation
mod replication;

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
            resp_tx,
        })
    }

    /// Returns `true` if the proposed command is read-only
    fn is_read_only(&self) -> bool {
        self.cmd.is_read_only()
    }

    /// Convert self into parts
    fn into_parts(self) -> ((ProposeId, Arc<C>), Arc<ResponseSender>) {
        let Self { cmd, id, resp_tx } = self;
        ((id, cmd), resp_tx)
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
    pub(super) fn propose_stream(
        &self,
        req: &ProposeRequest,
        resp_tx: Arc<ResponseSender>,
        bypassed: bool,
    ) -> Result<(), CurpError> {
        if self.curp.is_cluster_shutdown() {
            return Err(CurpError::shutting_down());
        }
        self.curp.check_leader_transfer()?;
        self.curp.check_term(req.term)?;
        self.curp.check_cluster_version(req.cluster_version)?;

        if req.slow_path {
            resp_tx.set_conflict(true);
        } else {
            info!("not using slow path for: {req:?}");
        }

        if bypassed {
            self.curp.mark_client_id_bypassed(req.propose_id().0);
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
            let Propose { cmd, id, resp_tx } = propose;
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
        let (cmds, resp_txs): (Vec<_>, Vec<_>) =
            proposes.into_iter().map(Propose::into_parts).unzip();
        let entries = curp.push_log_entries(cmds);
        curp.insert_resp_txs(entries.iter().map(|e| e.index).zip(resp_txs.clone()));
        //let entries = curp.push_logs(logs);
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
        if bypassed {
            self.curp.mark_client_id_bypassed(req.propose_id().0);
        }
        self.curp.handle_shutdown(req.propose_id())?;
        CommandBoard::wait_for_shutdown_synced(&self.cmd_board).await;
        self.trigger_nodes_shutdown().await;
        Self::abort_replication();
        Ok(ShutdownResponse::default())
    }

    #[allow(clippy::arithmetic_side_effects, clippy::pattern_type_mismatch)] // won't overflow
    /// Trigger other nodes to shutdown
    async fn trigger_nodes_shutdown(&self) {
        /// Wait interval for trigger shutdown
        const TRIGGER_INTERVAL: Duration = Duration::from_millis(100);
        let mut notified = HashSet::<u64>::new();
        let commit_index = self.curp.commit_index();
        loop {
            let states = self.curp.all_node_states();
            if notified.len() + 1 == states.len() {
                break;
            }
            let futs: FuturesUnordered<_> = states
                .iter()
                .filter(|(id, _)| !notified.contains(id))
                .filter(|(_, state)| state.match_index() == commit_index)
                .map(|(id, state)| state.connect().trigger_shutdown().map(move |res| (id, res)))
                .collect();
            for (id, result) in join_all(futs).await {
                match result {
                    Ok(()) => {
                        info!("node {id} shutdown triggered");
                        let _ignore = notified.insert(*id);
                    }
                    Err(err) => warn!("send trigger shutdown rpc to {id} failed, err: {err}"),
                }
            }

            tokio::time::sleep(TRIGGER_INTERVAL).await;
        }
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

    /// Handles fetch membership requests
    pub(super) fn fetch_membership(
        &self,
        _req: FetchMembershipRequest,
    ) -> Result<MembershipResponse, CurpError> {
        if self.curp.is_learner() {
            return Err(CurpError::learner_not_catch_up());
        }
        let (leader_id, term, _) = self.curp.leader();
        let leader_id =
            leader_id.ok_or(CurpError::LeaderTransfer("no current leader".to_owned()))?;
        Ok(self.build_membership_response(leader_id, term))
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
        let leader_id = req.leader_id;
        let term = req.term;
        let prev_log_index = req.prev_log_index;
        let prev_log_term = req.prev_log_term;
        let leader_commit = req.leader_commit;

        if entries.is_empty() {
            return Ok(self.heartbeat(leader_id, term, leader_commit));
        }

        self.append_entries_inner(
            entries,
            leader_id,
            term,
            prev_log_index,
            prev_log_term,
            leader_commit,
        )
    }

    /// Handles heartbeat
    fn heartbeat(
        &self,
        leader_id: u64,
        req_term: u64,
        leader_commit: u64,
    ) -> AppendEntriesResponse {
        match self
            .curp
            .handle_heartbeat(req_term, leader_id, leader_commit)
        {
            Ok(()) => AppendEntriesResponse::new_accept(req_term),
            Err((term, hint)) => AppendEntriesResponse::new_reject(term, hint),
        }
    }

    /// Handle `AppendEntries` requests
    pub(super) fn append_entries_inner(
        &self,
        entries: Vec<LogEntry<C>>,
        leader_id: u64,
        req_term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
    ) -> Result<AppendEntriesResponse, CurpError> {
        let membership_entries: Vec<_> = Self::filter_membership_entries(&entries).collect();
        let result = self.curp.handle_append_entries(
            req_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        );
        #[allow(clippy::pattern_type_mismatch)] // can't fix
        let resp = match result {
            Ok((term, truncate_at, to_persist)) => {
                self.storage
                    .put_log_entries(&to_persist.iter().map(Arc::as_ref).collect::<Vec<_>>())?;
                self.update_membership(truncate_at, membership_entries, Some(leader_commit))?;
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

    /// Handle `MoveLeader` requests
    pub(super) async fn move_leader(
        &self,
        req: MoveLeaderRequest,
    ) -> Result<MoveLeaderResponse, CurpError> {
        let should_send_try_become_leader_now = self.curp.handle_move_leader(req.node_id)?;
        if should_send_try_become_leader_now {
            if let Err(e) = self
                .curp
                .connects(Some(&req.node_id))
                .next()
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
            let result = Self::bcast_vote(self.curp.as_ref(), vote).await;
            if matches!(result, BCastVoteResult::VoteSuccess) {
                Self::respawn_replication(Arc::clone(&self.curp));
            }
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
        // wait for some random time before tick starts to minimize vote split
        // possibility
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
                // bcast pre vote or vote, if it is a pre vote and success, it will return
                // Some(vote) then we need to bcast normal vote, and bcast
                // normal vote always return None
                if let BCastVoteResult::PreVoteSuccess(vote) =
                    Self::bcast_vote(curp.as_ref(), pre_vote_or_vote.clone()).await
                {
                    debug_assert!(
                        !vote.is_pre_vote,
                        "bcast pre vote should return Some(normal_vote)"
                    );
                    let result = Self::bcast_vote(curp.as_ref(), vote).await;
                    debug_assert!(
                        matches!(result, BCastVoteResult::VoteSuccess | BCastVoteResult::Fail),
                        "bcast normal vote should always return Vote variants, result: {result:?}"
                    );
                    if matches!(result, BCastVoteResult::VoteSuccess) {
                        Self::respawn_replication(Arc::clone(&curp));
                    }
                }
            }
        }
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
        membership_info: MembershipInfo,
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
        let ms = storage.recover_membership()?;
        let membership_config = ms.map_or(
            MembershipConfig::Init(membership_info),
            MembershipConfig::Recovered,
        );
        let peer_addrs: HashMap<_, _> = membership_config
            .members()
            .into_iter()
            .map(|(id, meta)| (id, meta.into_peer_urls()))
            .collect();
        let member_connects = rpc::inner_connects(peer_addrs, client_tls_config.as_ref()).collect();
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
                .is_leader(is_leader)
                .cmd_board(Arc::clone(&cmd_board))
                .lease_manager(Arc::clone(&lease_manager))
                .cfg(Arc::clone(&curp_cfg))
                .role_change(role_change)
                .task_manager(Arc::clone(&task_manager))
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
                .membership_config(membership_config)
                .member_connects(member_connects)
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

        if is_leader {
            Self::respawn_replication(Arc::clone(&curp));
        }

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

        task_manager.spawn(TaskName::HandlePropose, |_n| {
            Self::handle_propose_task(Arc::clone(&cmd_executor), Arc::clone(&curp), propose_rx)
        });
        task_manager.spawn(TaskName::AfterSync, |_n| {
            Self::after_sync_task(curp, cmd_executor, as_rx)
        });
    }

    /// Candidate or pre candidate broadcasts votes
    async fn bcast_vote(curp: &RawCurp<C, RC>, vote: Vote) -> BCastVoteResult {
        let self_id = curp.id();
        if vote.is_pre_vote {
            debug!("{self_id} broadcasts pre votes to all servers");
        } else {
            debug!("{self_id} broadcasts votes to all servers");
        }
        let rpc_timeout = curp.cfg().rpc_timeout;
        let voters_connects = curp.voters_connects();
        let req = VoteRequest::new(
            vote.term,
            vote.candidate_id,
            vote.last_log_index,
            vote.last_log_term,
            vote.is_pre_vote,
        );
        let resps = voters_connects
            .into_iter()
            .filter_map(|(id, connect)| {
                (id != self_id).then_some(async move {
                    connect.vote(req, rpc_timeout).map(|res| (id, res)).await
                })
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
                    return BCastVoteResult::Fail;
                }
                let result = curp.handle_pre_vote_resp(id, resp.term, resp.vote_granted);
                match result {
                    Ok(None) | Err(()) => {}
                    Ok(Some(v)) => return BCastVoteResult::PreVoteSuccess(v),
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
                    Ok(true) => return BCastVoteResult::VoteSuccess,
                    Err(()) => return BCastVoteResult::Fail,
                }
            };
        }

        BCastVoteResult::Fail
    }

    /// Get `RawCurp`
    pub(super) fn raw_curp(&self) -> Arc<RawCurp<C, RC>> {
        Arc::clone(&self.curp)
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

/// Represents the result of broadcasting a vote in the consensus process.
#[derive(Debug)]
enum BCastVoteResult {
    /// Indicates that the pre-vote phase was successful.
    PreVoteSuccess(Vote),
    /// Indicates that the vote phase was successful.
    VoteSuccess,
    /// Indicates that the vote or pre-vote phase failed.
    Fail,
}

#[cfg(test)]
mod tests {
    #[cfg(ignore)] // TODO : rewrite this
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
        let s1_id = curp.get_id_by_name("S1").unwrap();
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

    #[cfg(ignore)] // TODO : rewrite `set_connect`
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
        let s2_id = curp.get_id_by_name("S2").unwrap();
        curp.handle_append_entries(2, s2_id, 0, 0, vec![], 0, |_, _, _| {})
            .unwrap();

        let mut mock_connect1 = MockInnerConnectApi::default();
        mock_connect1.expect_vote().returning(|req, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(req.term, vec![]).unwrap(),
            ))
        });
        let s1_id = curp.get_id_by_name("S1").unwrap();
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
        let s2_id = curp.get_id_by_name("S2").unwrap();
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

    #[cfg(ignore)]
    #[traced_test]
    #[tokio::test]
    async fn vote_will_not_send_to_learner_during_election() {}
}
