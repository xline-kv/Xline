use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use engine::{SnapshotAllocator, SnapshotApi};
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use madsim::rand::{thread_rng, Rng};
use parking_lot::{Mutex, RwLock};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::MissedTickBehavior,
};
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
    storage::StorageApi,
};
use crate::{
    cmd::{Command, CommandExecutor},
    log_entry::{EntryData, LogEntry},
    members::{ClusterInfo, ServerId},
    role_change::RoleChange,
    rpc::{
        self,
        connect::{InnerConnectApi, InnerConnectApiWrapper},
        AppendEntriesRequest, AppendEntriesResponse, ConfChangeType, CurpError,
        FetchClusterRequest, FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, ProposeConfChangeRequest,
        ProposeConfChangeResponse, ProposeRequest, ProposeResponse, PublishRequest,
        PublishResponse, ShutdownRequest, ShutdownResponse, TriggerShutdownRequest,
        TriggerShutdownResponse, VoteRequest, VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
    server::{cmd_worker::CEEventTxApi, raw_curp::SyncAction, storage::db::DB},
    snapshot::{Snapshot, SnapshotMeta},
};

/// `CurpNode` represents a single node of curp cluster
pub(super) struct CurpNode<C: Command, RC: RoleChange> {
    /// `RawCurp` state machine
    curp: Arc<RawCurp<C, RC>>,
    /// The speculative cmd pool, shared with executor
    spec_pool: SpecPoolRef<C>,
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: CmdBoardRef<C>,
    /// Command executor
    ce: Arc<dyn CommandExecutor<C>>,
    /// CE event tx,
    ce_event_tx: Arc<dyn CEEventTxApi<C>>,
    /// Storage
    storage: Arc<dyn StorageApi<Command = C>>,
    /// Snapshot allocator
    snapshot_allocator: Box<dyn SnapshotAllocator>,
}

/// Handlers for clients
impl<C: Command, RC: RoleChange> CurpNode<C, RC> {
    /// Handle `Propose` requests
    pub(super) async fn propose(&self, req: ProposeRequest) -> Result<ProposeResponse, CurpError> {
        if self.curp.is_shutdown() {
            return Err(CurpError::shutting_down());
        }
        let id = req.propose_id();
        self.check_cluster_version(req.cluster_version)?;
        let cmd: Arc<C> = Arc::new(req.cmd()?);
        if !self.ce.check_quota(cmd.as_ref()) {
            warn!(
                "{} has no enough quota to execute cmd({:?})",
                self.curp.id(),
                cmd
            );
            return Err(CurpError::Internal("Quota exceeded".to_owned()));
        }
        // handle proposal
        let sp_exec = self.curp.handle_propose(id, Arc::clone(&cmd))?;

        // if speculatively executed, wait for the result and return
        if sp_exec {
            let er_res = CommandBoard::wait_for_er(&self.cmd_board, id).await;
            return Ok(ProposeResponse::new_result::<C>(&er_res));
        }

        Ok(ProposeResponse::new_empty())
    }

    /// Handle `Shutdown` requests
    pub(super) async fn shutdown(
        &self,
        req: ShutdownRequest,
    ) -> Result<ShutdownResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        self.curp.handle_shutdown(req.propose_id())?;
        CommandBoard::wait_for_shutdown_synced(&self.cmd_board).await;
        Ok(ShutdownResponse::default())
    }

    /// Handle `ProposeConfChange` requests
    pub(super) async fn propose_conf_change(
        &self,
        req: ProposeConfChangeRequest,
    ) -> Result<ProposeConfChangeResponse, CurpError> {
        self.check_cluster_version(req.cluster_version)?;
        let id = req.propose_id();
        self.curp.handle_propose_conf_change(id, req.changes)?;
        CommandBoard::wait_for_conf(&self.cmd_board, id).await;
        let members = self.curp.cluster().all_members_vec();
        Ok(ProposeConfChangeResponse { members })
    }

    /// handle `WaitSynced` requests
    pub(super) async fn wait_synced(
        &self,
        req: WaitSyncedRequest,
    ) -> Result<WaitSyncedResponse, CurpError> {
        if self.curp.is_shutdown() {
            return Err(CurpError::shutting_down());
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

    /// Handle `Publish` requests
    pub(super) fn publish(&self, req: PublishRequest) -> Result<PublishResponse, CurpError> {
        self.curp.handle_publish(req)?;
        Ok(PublishResponse::default())
    }
}

/// Handlers for peers
impl<C: Command, RC: RoleChange> CurpNode<C, RC> {
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
                    self.storage.flush_voted_for(term, req.candidate_id).await?;
                }
                VoteResponse::new_accept(term, sp)?
            }
            Err(Some(term)) => VoteResponse::new_reject(term),
            Err(None) => VoteResponse::new_shutdown(),
        };

        Ok(resp)
    }

    /// Handle `TriggerShutdown` requests
    pub(super) fn trigger_shutdown(
        &self,
        _req: &TriggerShutdownRequest,
    ) -> TriggerShutdownResponse {
        let shutdown_trigger = self.curp.shutdown_trigger();
        shutdown_trigger.mark_leader_notified();
        shutdown_trigger.check_and_shutdown();
        TriggerShutdownResponse::default()
    }

    /// Handle `InstallSnapshot` stream
    #[allow(clippy::integer_arithmetic)] // can't overflow
    pub(super) async fn install_snapshot<E: std::error::Error + 'static>(
        &self,
        req_stream: impl Stream<Item = Result<InstallSnapshotRequest, E>>,
    ) -> Result<InstallSnapshotResponse, CurpError> {
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
                self.ce_event_tx
                    .send_reset(Some(snapshot))
                    .await
                    .map_err(|err| {
                        error!("failed to reset the command executor by snapshot, {err}");
                        CurpError::internal(format!(
                            "failed to reset the command executor by snapshot, {err}"
                        ))
                    })?;
                return Ok(InstallSnapshotResponse::new(self.curp.term()));
            }
        }
        Err(CurpError::internal(
            "failed to receive a complete snapshot".to_owned(),
        ))
    }
}

/// Spawned tasks
impl<C: Command, RC: RoleChange> CurpNode<C, RC> {
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
                                error!("update connect {} failed, err {:?}", change.node_id, e);
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
        let connect_id = connect.id();
        let batch_timeout = curp.cfg().batch_timeout;
        let mut is_shutdown_state = false;
        let mut ae_fail_count = 0;

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
                    break false;
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

            let Some(sync_action) = curp.sync(connect_id) else {
                break true;
            };

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
                    if !hb_opt || !is_empty {
                        match Self::send_ae(connect.as_ref(), curp.as_ref(), ae).await {
                            Ok((true, _)) => break true,
                            Ok((false, ae_succeed)) => {
                                if ae_succeed {
                                    hb_opt = true;
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
                                    break false;
                                }
                            }
                            Err(err) => {
                                warn!("ae to {} failed, {err:?}", connect.id());
                                if is_shutdown_state {
                                    ae_fail_count += 1;
                                    if ae_fail_count >= 5 {
                                        warn!("the follower {} may have been shutdown", connect_id);
                                        break false;
                                    }
                                }
                            }
                        };
                    }
                }
                SyncAction::Snapshot(rx) => match rx.await {
                    Ok(snapshot) => {
                        match Self::send_snapshot(connect.as_ref(), curp.as_ref(), snapshot).await {
                            Ok(true) => break true,
                            Err(err) => warn!("snapshot to {} failed, {err:?}", connect.id()),
                            Ok(false) => {}
                        }
                    }
                    Err(err) => {
                        warn!("failed to receive snapshot result, {err}");
                    }
                },
            }
        };
        debug!("{} to {} sync follower task exits", curp.id(), connect_id);
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
impl<C: Command, RC: RoleChange> CurpNode<C, RC> {
    /// Create a new server instance
    #[inline]
    pub(super) async fn new<CE: CommandExecutor<C>>(
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
        let connects = rpc::inner_connects(cluster_info.peers_addrs())
            .await
            .map_err(|e| CurpError::internal(format!("parse peers addresses failed, err {e:?}")))?
            .collect();
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let last_applied = cmd_executor
            .last_applied()
            .map_err(|e| CurpError::internal(format!("get applied index error, {e}")))?;
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
            ce: cmd_executor,
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

    /// Candidate or pre candidate broadcasts votes
    /// Return `Some(vote)` if bcast pre vote and success
    /// Return `None` if bcast pre vote and fail or bcast vote
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
                    curp.shutdown_trigger().self_shutdown();
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
    #[allow(clippy::integer_arithmetic)] // won't overflow
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
            return Err(CurpError::wrong_cluster_version());
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
    use curp_test_utils::{mock_role_change, sleep_secs, test_cmd::TestCommand};
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        rpc::{connect::MockInnerConnectApi, ConfChange},
        server::cmd_worker::MockCEEventTxApi,
    };

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
            let exe_tx = MockCEEventTxApi::<TestCommand>::default();
            Arc::new(RawCurp::new_test(3, exe_tx, mock_role_change()))
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
}
