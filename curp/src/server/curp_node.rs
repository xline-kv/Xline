use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
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
    cmd_worker::start_cmd_workers,
    gc::run_gc_tasks,
    raw_curp::{RawCurp, Vote},
    spec_pool::{SpecPoolRef, SpeculativePool},
    storage::{StorageApi, StorageError},
};
use crate::{
    cmd::{Command, CommandExecutor, ProposeId},
    error::ProposeError,
    log_entry::LogEntry,
    rpc::{
        self, connect::ConnectApi, AppendEntriesRequest, AppendEntriesResponse, FetchLeaderRequest,
        FetchLeaderResponse, ProposeRequest, ProposeResponse, VoteRequest, VoteResponse,
        WaitSyncedRequest, WaitSyncedResponse,
    },
    server::storage::rocksdb::RocksDBStorage,
    ServerId, TxFilter,
};

/// Uncommitted pool type
pub(super) type UncommittedPool<C> = HashMap<ProposeId, Arc<C>>;

/// Reference to uncommitted pool
pub(super) type UncommittedPoolRef<C> = Arc<Mutex<UncommittedPool<C>>>;

/// Curp error
#[derive(Debug, Error)]
pub(super) enum CurpError {
    /// Encode or decode error
    #[error("encode or decode error")]
    EncodeDecode(#[from] bincode::Error),
    /// Storage error
    #[error("storage error, {0}")]
    Storage(#[from] StorageError),
    /// Get applied index error
    #[error("internal error {0}")]
    Internal(String),
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
}

/// Spawned tasks
impl<C: 'static + Command> CurpNode<C> {
    /// Tick periodically
    async fn tick_task(curp: Arc<RawCurp<C>>, connects: HashMap<ServerId, Arc<impl ConnectApi>>) {
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

    /// Sync task is responsible for syncing a follower
    async fn sync_task(
        curp: Arc<RawCurp<C>>,
        connect: Arc<impl ConnectApi>,
        sync_event: Arc<Event>,
    ) {
        let leader_event = curp.leader_event();
        loop {
            if !curp.is_leader() {
                leader_event.listen().await;
            }
            Self::_sync_task(
                Arc::clone(&curp),
                Arc::clone(&connect),
                Arc::clone(&sync_event),
            )
            .await;
        }
    }

    /// real sync task, will stop if self is no long the leader
    async fn _sync_task(
        curp: Arc<RawCurp<C>>,
        connect: Arc<impl ConnectApi>,
        sync_event: Arc<Event>,
    ) {
        let mut hb_opt = false;
        let mut ticker = tokio::time::interval(curp.cfg().heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let id = connect.id();
        let rpc_timeout = curp.cfg().rpc_timeout;

        #[allow(clippy::integer_arithmetic)] // tokio select internal triggered
        loop {
            // a sync is either triggered by an heartbeat timeout event or when new log entries arrive
            tokio::select! {
                _now = ticker.tick() => {
                    if hb_opt {
                        hb_opt = false;
                        continue;
                    }
                }
                // TODO: use a batch timer
                _ = sync_event.listen() => {}
            }

            let Ok(ae) = curp.sync(id) else {
                return;
            };

            let last_sent_index = (!ae.entries.is_empty())
                .then(|| ae.prev_log_index + ae.entries.len().numeric_cast::<u64>());
            let Ok(req) = AppendEntriesRequest::new(
                ae.term,
                ae.leader_id,
                ae.prev_log_index,
                ae.prev_log_term,
                ae.entries,
                ae.leader_commit,
            ) else {
                error!("failed to serialize ae");
                continue;
            };

            debug!("{} send ae to {}", curp.id(), connect.id());
            let resp = match connect.append_entries(req, rpc_timeout).await {
                Err(e) => {
                    warn!("ae to {} failed, {e}", connect.id());
                    continue;
                }
                Ok(resp) => resp.into_inner(),
            };

            let Ok(succeeded) = curp.handle_append_entries_resp(
                connect.id(),
                last_sent_index,
                resp.term,
                resp.success,
                resp.hint_index,
            ) else {
                return;
            };

            // is not a heartbeat and succeeded, then we opt out heartbeat
            if succeeded && last_sent_index.is_some() {
                hb_opt = true;
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
        let sync_event = Arc::new(Event::new());
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        let shutdown_trigger = Arc::new(Event::new());
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));
        let last_applied = cmd_executor
            .last_applied()
            .map_err(|e| CurpError::Internal(format!("get applied index error, {e}")))?;

        let storage = Arc::new(RocksDBStorage::new(&curp_cfg.data_dir)?);

        // start cmd workers
        let exe_tx = start_cmd_workers(
            cmd_executor,
            Arc::clone(&spec_pool),
            Arc::clone(&uncommitted_pool),
            Arc::clone(&cmd_board),
            Arc::clone(&shutdown_trigger),
        );

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
                curp_cfg,
                Box::new(exe_tx),
                Arc::clone(&sync_event),
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
                curp_cfg,
                Box::new(exe_tx),
                Arc::clone(&sync_event),
                log_tx,
                voted_for,
                entries,
                last_applied,
            ))
        };

        run_gc_tasks(Arc::clone(&cmd_board), Arc::clone(&spec_pool));

        let curp_c = Arc::clone(&curp);
        let shutdown_trigger_c = Arc::clone(&shutdown_trigger);
        let storage_c = Arc::clone(&storage);
        let _ig = tokio::spawn(async move {
            // establish connection with other servers
            let connects = rpc::connect(others.clone(), tx_filter).await;
            let tick_task = tokio::spawn(Self::tick_task(Arc::clone(&curp_c), connects.clone()));
            let sync_tasks = connects
                .into_values()
                .map(|connect| {
                    tokio::spawn(Self::sync_task(
                        Arc::clone(&curp_c),
                        connect,
                        Arc::clone(&sync_event),
                    ))
                })
                .collect_vec();

            let log_persist_task = tokio::spawn(Self::log_persist_task(log_rx, storage_c));
            shutdown_trigger_c.listen().await;
            tick_task.abort();
            for sync_task in sync_tasks {
                sync_task.abort();
            }
            log_persist_task.abort();
        });

        Ok(Self {
            curp,
            spec_pool,
            cmd_board,
            shutdown_trigger,
            storage,
        })
    }

    /// Candidate broadcasts votes
    async fn bcast_vote(
        curp: &RawCurp<C>,
        connects: &HashMap<ServerId, Arc<impl ConnectApi>>,
        vote: Vote,
    ) {
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
