use std::{collections::HashMap, fmt::Debug, io, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use engine::snapshot_api::SnapshotApi;
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use itertools::Itertools;
use madsim::rand::{thread_rng, Rng};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc},
    time::MissedTickBehavior,
};
use tracing::{debug, error, info, warn};
use utils::config::CurpConfig;

use super::{
    cmd_board::{CmdBoardRef, CommandBoard},
    cmd_worker::{conflict_checked_mpmc, start_bg_workers, CEEventTx},
    gc::run_gc_tasks,
    raw_curp::{AppendEntries, RawCurp, UncommittedPool, Vote},
    spec_pool::{SpecPoolRef, SpeculativePool},
    storage::{StorageApi, StorageError},
};
use crate::{
    cmd::{Command, CommandExecutor},
    error::ProposeError,
    log_entry::LogEntry,
    rpc::{
        self, connect::ConnectApi, AppendEntriesRequest, AppendEntriesResponse, FetchLeaderRequest,
        FetchLeaderResponse, FetchReadStateRequest, FetchReadStateResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, ProposeRequest, ProposeResponse, VoteRequest, VoteResponse,
        WaitSyncedRequest, WaitSyncedResponse,
    },
    server::{cmd_worker::CEEventTxApi, raw_curp::SyncAction, storage::rocksdb::RocksDBStorage},
    snapshot::{Snapshot, SnapshotMeta},
    ServerId, TxFilter,
};

/// Curp error
#[derive(Debug, Error)]
pub(super) enum CurpError {
    /// Encode or decode error
    #[error("encode or decode error")]
    EncodeDecode(#[from] bincode::Error),
    /// Storage error
    #[error("storage error, {0}")]
    Storage(#[from] StorageError),
    /// Transport error
    #[error("transport error, {0}")]
    Transport(String),
    /// Io error
    #[error("io error, {0}")]
    IO(#[from] io::Error),
    /// Internal error
    #[error("internal error, {0}")]
    Internal(String),
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
    Transport(#[from] ProposeError),
    /// Encode/Decode error
    #[error("encode or decode error")]
    EncodeDecode(#[from] bincode::Error),
}

/// Internal error encountered when sending snapshot
#[derive(Debug, Error)]
enum SendSnapshotError {
    /// When self is no longer leader
    #[error("self is no longer leader")]
    NotLeader,
    /// Transport
    #[error("transport error, {0}")]
    Transport(#[from] ProposeError),
}

/// `CurpNode` represents a single node of curp cluster
pub(super) struct CurpNode<C: Command> {
    /// `RawCurp` state machine
    curp: Arc<RawCurp<C>>,
    /// The speculative cmd pool, shared with executor
    spec_pool: SpecPoolRef<C>,
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: CmdBoardRef<C>,
    /// Shutdown trigger
    shutdown_trigger: Arc<Event>,
    /// CE event tx,
    ce_event_tx: CEEventTx<C>,
    /// Storage
    storage: Arc<dyn StorageApi<Command = C>>,
}

// handlers
impl<C: 'static + Command> CurpNode<C> {
    /// Handle "propose" requests
    pub(super) async fn propose(&self, req: ProposeRequest) -> Result<ProposeResponse, CurpError> {
        let cmd: Arc<C> = Arc::new(req.cmd()?);

        // handle proposal
        let ((leader_id, term), result) = self.curp.handle_propose(Arc::clone(&cmd));
        let resp = match result {
            Ok(true) => match CommandBoard::wait_for_er(&self.cmd_board, cmd.id()).await {
                Ok(er) => ProposeResponse::new_result::<C>(leader_id, term, &er)?,
                Err(err) => {
                    ProposeResponse::new_error(leader_id, term, &ProposeError::ExecutionError(err))?
                }
            },
            Ok(false) => ProposeResponse::new_empty(leader_id, term)?,
            Err(err) => ProposeResponse::new_error(leader_id, term, &err)?,
        };

        Ok(resp)
    }

    /// Handle `AppendEntries` requests
    pub(super) fn append_entries(
        &self,
        req: AppendEntriesRequest,
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
    #[allow(clippy::pedantic)] // need not return result, but to keep it consistent with rpc handler functions, we keep it this way
    pub(super) async fn vote(&self, req: VoteRequest) -> Result<VoteResponse, CurpError> {
        let result = self.curp.handle_vote(
            req.term,
            req.candidate_id.clone(),
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

    /// handle "wait synced" request
    pub(super) async fn wait_synced(
        &self,
        req: WaitSyncedRequest,
    ) -> Result<WaitSyncedResponse, CurpError> {
        let id = req.id()?;
        debug!("{} get wait synced request for cmd({id})", self.curp.id());

        let (er, asr) = CommandBoard::wait_for_er_asr(&self.cmd_board, &id).await;
        let resp = WaitSyncedResponse::new_from_result::<C>(Some(er), asr)?;

        debug!("{} wait synced for cmd({id}) finishes", self.curp.id());
        Ok(resp)
    }

    /// Handle fetch leader requests
    #[allow(clippy::unnecessary_wraps, clippy::needless_pass_by_value)] // To keep type consistent with other request handlers
    pub(super) fn fetch_leader(
        &self,
        _req: FetchLeaderRequest,
    ) -> Result<FetchLeaderResponse, CurpError> {
        let (leader_id, term) = self.curp.leader();
        Ok(FetchLeaderResponse::new(leader_id, term))
    }

    /// Install snapshot
    #[allow(clippy::integer_arithmetic)] // can't overflow
    pub(super) async fn install_snapshot(
        &self,
        req_stream: impl Stream<Item = Result<InstallSnapshotRequest, String>>,
    ) -> Result<InstallSnapshotResponse, CurpError> {
        pin_mut!(req_stream);
        let mut snapshot = self.storage.new_snapshot().await?;
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
            snapshot.write_all(req.data.as_slice()).await?;
            if req.done {
                debug_assert_eq!(
                    snapshot.size(),
                    req.offset + req.data.len().numeric_cast::<u64>(),
                    "snapshot corrupted"
                );
                info!(
                    "{} successfully received a snapshot, size: {}",
                    self.curp.id(),
                    snapshot.size()
                );
                let meta = SnapshotMeta {
                    last_included_index: req.last_included_index,
                    last_included_term: req.last_included_term,
                };
                let snapshot = Snapshot::new(meta, snapshot);
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

    /// Handle fetch read state requests
    #[allow(clippy::needless_pass_by_value)] // To keep type consistent with other request handlers
    pub(super) fn fetch_read_state(
        &self,
        req: FetchReadStateRequest,
    ) -> Result<FetchReadStateResponse, CurpError> {
        let cmd = req.cmd()?;
        let state = self.curp.handle_fetch_read_state(&cmd)?;
        Ok(FetchReadStateResponse::new(state))
    }
}

/// Spawned tasks
impl<C: 'static + Command> CurpNode<C> {
    /// Tick periodically
    async fn election_task(
        curp: Arc<RawCurp<C>>,
        connects: HashMap<ServerId, Arc<impl ConnectApi>>,
    ) {
        let heartbeat_interval = curp.cfg().heartbeat_interval;
        // wait for some random time before tick starts to minimize vote split possibility
        let rand = thread_rng()
            .gen_range(0..heartbeat_interval.as_millis())
            .numeric_cast();
        tokio::time::sleep(Duration::from_millis(rand)).await;

        let mut ticker = tokio::time::interval(heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let _now = ticker.tick().await;
            if let Some(vote) = curp.tick_election() {
                Self::bcast_vote(curp.as_ref(), &connects, vote).await;
            }
        }
    }

    /// Responsible for bringing up `sync_follower_task` when self becomes leader
    async fn sync_follower_daemon(
        curp: Arc<RawCurp<C>>,
        connect: Arc<impl ConnectApi>,
        sync_event: Arc<Event>,
    ) {
        let leader_event = curp.leader_event();
        loop {
            if !curp.is_leader() {
                leader_event.listen().await;
            }
            Self::sync_follower_task(
                Arc::clone(&curp),
                Arc::clone(&connect),
                Arc::clone(&sync_event),
            )
            .await;
        }
    }

    /// Leader use this task to keep a follower up-to-date, will return if self is no longer leader
    async fn sync_follower_task(
        curp: Arc<RawCurp<C>>,
        connect: Arc<impl ConnectApi>,
        sync_event: Arc<Event>,
    ) {
        let mut hb_opt = false;
        let mut ticker = tokio::time::interval(curp.cfg().heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let id = connect.id();
        let batch_timeout = curp.cfg().batch_timeout;

        #[allow(clippy::integer_arithmetic, clippy::unwrap_used)]
        // tokio select internal triggered,
        loop {
            // grab the listener in the beginning to prevent missing sync events
            let listener: event_listener::EventListener = sync_event.listen();

            let Ok(sync_action) = curp.sync(id) else {
                return;
            };

            match sync_action {
                SyncAction::AppendEntries(ae) => {
                    // (hb_opt, entries) status combination
                    // (false, empty) => send heartbeat to followers
                    // (true, empty) => indicates that `batch_timeout` expired, and during this period there is not any log generated. Do nothing
                    // (true | false, not empty) => send append entries
                    if !hb_opt || !ae.entries.is_empty() {
                        let result = Self::send_ae(connect.as_ref(), curp.as_ref(), ae).await;
                        if let Err(err) = result {
                            warn!("ae to {} failed, {err}", connect.id());
                            if matches!(err, SendAEError::NotLeader) {
                                return;
                            }
                        } else {
                            hb_opt = true;
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
                                return;
                            }
                        }
                    }
                    Err(err) => {
                        warn!("failed to receive snapshot result, {err}");
                    }
                },
            }

            // a sync is either triggered by an heartbeat timeout event or when new log entries arrive
            tokio::select! {
                _now = ticker.tick() => {
                    hb_opt = false;
                }
                res = tokio::time::timeout(batch_timeout, listener) => {
                    if let Err(_e) = res {
                        hb_opt = true;
                    }
                }
            }
        }
    }
}

// utils
impl<C: 'static + Command> CurpNode<C> {
    /// Create a new server instance
    #[inline]
    pub(super) async fn new<CE: CommandExecutor<C> + 'static>(
        id: ServerId,
        is_leader: bool,
        others: HashMap<ServerId, String>,
        cmd_executor: CE,
        curp_cfg: Arc<CurpConfig>,
        tx_filter: Option<Box<dyn TxFilter>>,
    ) -> Result<Self, CurpError> {
        let sync_events = others
            .keys()
            .map(|server_id| (server_id.clone(), Arc::new(Event::new())))
            .collect();
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        let shutdown_trigger = Arc::new(Event::new());
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let last_applied = cmd_executor
            .last_applied()
            .map_err(|e| CurpError::Internal(format!("get applied index error, {e}")))?;
        let (ce_event_tx, task_rx, as_rx, done_tx) = conflict_checked_mpmc::channel();

        let storage = Arc::new(RocksDBStorage::new(&curp_cfg.data_dir)?);

        // create curp state machine
        let (voted_for, entries) = storage.recover().await?;
        let curp = if voted_for.is_none() && entries.is_empty() {
            Arc::new(RawCurp::new(
                id,
                others.keys().cloned().collect(),
                is_leader,
                Arc::clone(&cmd_board),
                Arc::clone(&spec_pool),
                uncommitted_pool,
                Arc::clone(&curp_cfg),
                Box::new(ce_event_tx.clone()),
                sync_events,
                log_tx,
            ))
        } else {
            info!(
                "{} recovered voted_for({voted_for:?}), entries from {:?} to {:?}",
                id,
                entries.first(),
                entries.last()
            );
            Arc::new(RawCurp::recover_from(
                id,
                others.keys().cloned().collect(),
                is_leader,
                Arc::clone(&cmd_board),
                Arc::clone(&spec_pool),
                uncommitted_pool,
                &curp_cfg,
                Box::new(ce_event_tx.clone()),
                sync_events,
                log_tx,
                voted_for,
                entries,
                last_applied,
            ))
        };

        start_bg_workers(
            cmd_executor,
            Arc::clone(&curp),
            task_rx,
            as_rx,
            done_tx,
            Arc::clone(&shutdown_trigger),
        );
        run_gc_tasks(
            Arc::clone(&cmd_board),
            Arc::clone(&spec_pool),
            curp_cfg.gc_interval,
        );

        let curp_c = Arc::clone(&curp);
        let shutdown_trigger_c = Arc::clone(&shutdown_trigger);
        let storage_c = Arc::clone(&storage);
        let _ig = tokio::spawn(async move {
            // establish connection with other servers
            let connects = rpc::connect(others.clone(), tx_filter).await;
            let election_task =
                tokio::spawn(Self::election_task(Arc::clone(&curp_c), connects.clone()));
            let sync_task_daemons = connects
                .into_iter()
                .map(|(server_id, connect)| {
                    tokio::spawn(Self::sync_follower_daemon(
                        Arc::clone(&curp_c),
                        connect,
                        curp_c.sync_event(&server_id),
                    ))
                })
                .collect_vec();

            let log_persist_task = tokio::spawn(Self::log_persist_task(log_rx, storage_c));
            shutdown_trigger_c.listen().await;
            election_task.abort();
            for sync_task in sync_task_daemons {
                sync_task.abort();
            }
            log_persist_task.abort();
        });

        Ok(Self {
            curp,
            spec_pool,
            cmd_board,
            shutdown_trigger,
            ce_event_tx,
            storage,
        })
    }

    /// Candidate broadcasts votes
    async fn bcast_vote(
        curp: &RawCurp<C>,
        connects: &HashMap<ServerId, Arc<impl ConnectApi>>,
        vote: Vote,
    ) {
        debug!("{} broadcasts votes to all servers", curp.id());
        let rpc_timeout = curp.cfg().rpc_timeout;
        let resps = connects
            .iter()
            .map(|(id, connect)| {
                let req = VoteRequest::new(
                    vote.term,
                    vote.candidate_id.clone(),
                    vote.last_log_index,
                    vote.last_log_term,
                );
                async move {
                    let resp = connect.vote(req, rpc_timeout).await;
                    (id.clone(), resp)
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
                Ok(spec_pool) => spec_pool.into_iter().map(|cmd| Arc::new(cmd)).collect(),
            };
            let result =
                curp.handle_vote_resp(&id, resp.term, resp.vote_granted, follower_spec_pool);
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

    /// Log persist task
    pub(super) async fn log_persist_task(
        mut log_rx: mpsc::UnboundedReceiver<LogEntry<C>>,
        storage: Arc<dyn StorageApi<Command = C>>,
    ) {
        while let Some(e) = log_rx.recv().await {
            if let Err(err) = storage.put_log_entry(e).await {
                error!("storage error, {err}");
            }
        }
        error!("log persist task exits unexpectedly");
    }

    /// Send `append_entries` request
    #[allow(clippy::integer_arithmetic)] // won't overflow
    async fn send_ae(
        connect: &impl ConnectApi,
        curp: &RawCurp<C>,
        ae: AppendEntries<C>,
    ) -> Result<(), SendAEError> {
        let last_sent_index = (!ae.entries.is_empty())
            .then(|| ae.prev_log_index + ae.entries.len().numeric_cast::<u64>());
        let req = AppendEntriesRequest::new(
            ae.term,
            ae.leader_id,
            ae.prev_log_index,
            ae.prev_log_term,
            ae.entries,
            ae.leader_commit,
        )?;

        debug!("{} send ae to {}", curp.id(), connect.id());
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
        connect: &impl ConnectApi,
        curp: &RawCurp<C>,
        snapshot: Snapshot,
    ) -> Result<(), SendSnapshotError> {
        let meta = snapshot.meta;
        let resp = connect
            .install_snapshot(curp.term(), curp.id().clone(), snapshot)
            .await?
            .into_inner();
        curp.handle_snapshot_resp(connect.id(), meta, resp.term)
            .map_err(|_e| SendSnapshotError::NotLeader)
    }
}

impl<C: Command> Drop for CurpNode<C> {
    #[inline]
    fn drop(&mut self) {
        self.shutdown_trigger.notify(usize::MAX);
    }
}

impl<C: Command> Debug for CurpNode<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurpNode")
            .field("raw_curp", &self.curp)
            .field("spec_pool", &self.spec_pool)
            .field("cmd_board", &self.cmd_board)
            .field("shutdown_trigger", &self.shutdown_trigger)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        rpc::connect::MockConnectApi,
        server::cmd_worker::MockCEEventTxApi,
        test_utils::{sleep_secs, test_cmd::TestCommand},
    };

    #[traced_test]
    #[tokio::test]
    async fn sync_task_will_send_hb() {
        let curp = Arc::new(RawCurp::new_test(
            3,
            MockCEEventTxApi::<TestCommand>::default(),
        ));
        let mut mock_connect1 = MockConnectApi::default();
        mock_connect1
            .expect_append_entries()
            .times(1..)
            .returning(|_, _| Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))));
        mock_connect1.expect_id().return_const("S1".to_owned());
        tokio::spawn(CurpNode::sync_follower_task(
            curp,
            Arc::new(mock_connect1),
            Arc::new(Event::new()),
        ));
        sleep_secs(2).await;
    }

    #[traced_test]
    #[tokio::test]
    async fn tick_task_will_bcast_votes() {
        let curp = {
            let mut exe_tx = MockCEEventTxApi::<TestCommand>::default();
            exe_tx
                .expect_send_reset()
                .returning(|_| oneshot::channel().1);
            Arc::new(RawCurp::new_test(3, exe_tx))
        };
        curp.handle_append_entries(1, "S2".to_owned(), 0, 0, vec![], 0)
            .unwrap();

        let mut mock_connect1 = MockConnectApi::default();
        mock_connect1.expect_vote().returning(|req, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(req.term, vec![]).unwrap(),
            ))
        });
        mock_connect1.expect_id().return_const("S1".to_owned());

        let mut mock_connect2 = MockConnectApi::default();
        mock_connect2.expect_vote().returning(|req, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(req.term, vec![]).unwrap(),
            ))
        });
        mock_connect2.expect_id().return_const("S2".to_owned());

        tokio::spawn(CurpNode::election_task(
            Arc::clone(&curp),
            HashMap::from([
                ("S1".to_owned(), Arc::new(mock_connect1)),
                ("S2".to_owned(), Arc::new(mock_connect2)),
            ]),
        ));
        sleep_secs(3).await;
        assert!(curp.is_leader());
    }
}
