use std::{collections::HashMap, fmt::Debug, path::Path, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
use madsim::rand::{thread_rng, Rng};
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tracing::{debug, error, info, warn};
use utils::config::ServerTimeout;

use super::{
    cmd_board::{CmdBoardRef, CommandBoard},
    cmd_worker::start_cmd_workers,
    gc::run_gc_tasks,
    raw_curp::{AppendEntries, RawCurp, TickAction, Vote},
    spec_pool::{SpecPoolRef, SpeculativePool},
    storage::StorageApi,
};
use crate::{
    cmd::{Command, CommandExecutor, ProposeId},
    error::ProposeError,
    log_entry::LogEntry,
    message::ServerId,
    rpc::{
        self, connect::ConnectApi, AppendEntriesRequest, AppendEntriesResponse, FetchLeaderRequest,
        FetchLeaderResponse, ProposeRequest, ProposeResponse, SyncError, VoteRequest, VoteResponse,
        WaitSyncedRequest, WaitSyncedResponse,
    },
    server::storage::rocksdb::RocksDBStorage,
    TxFilter,
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
    Storage(#[from] Box<dyn std::error::Error>),
}

/// Connects
type Connects = HashMap<ServerId, Arc<dyn ConnectApi>>;

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
            Ok(Some(er_rx)) => match er_rx.await {
                Ok(Ok(er)) => ProposeResponse::new_result::<C>(leader_id, term, &er)?,
                Ok(Err(err)) => {
                    ProposeResponse::new_error(leader_id, term, &ProposeError::ExecutionError(err))?
                }
                Err(err) => ProposeResponse::new_error(
                    leader_id,
                    term,
                    &ProposeError::ProtocolError(err.to_string()),
                )?,
            },
            Ok(None) => ProposeResponse::new_empty(leader_id, term)?,
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
            req.prev_log_index.numeric_cast(),
            req.prev_log_term,
            entries,
            req.leader_commit.numeric_cast(),
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
            req.last_log_index.numeric_cast(),
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

        debug!("{} get wait synced request for {id:?}", self.curp.id());
        let resp = loop {
            let listener = {
                // check if the server is still leader
                let (leader, term) = self.curp.leader();
                let is_leader = leader.as_ref() == Some(self.curp.id());
                if !is_leader {
                    warn!("non-leader server {} receives wait_synced", self.curp.id());
                    break WaitSyncedResponse::new_error(&SyncError::Redirect(leader, term))?;
                }

                // check if the cmd board already has response
                let board_r = self.cmd_board.upgradable_read();
                #[allow(clippy::pattern_type_mismatch)] // can't get away with this
                match (board_r.er_buffer.get(&id), board_r.asr_buffer.get(&id)) {
                    (Some(Err(err)), _) => {
                        break WaitSyncedResponse::new_error(&SyncError::ExecuteError(
                            err.to_string(),
                        ))?;
                    }
                    (Some(er), Some(asr)) => {
                        break WaitSyncedResponse::new_from_result::<C>(
                            Some(er.clone()),
                            Some(asr.clone()),
                        )?;
                    }
                    _ => {}
                }

                let mut board_w = RwLockUpgradableReadGuard::upgrade(board_r);
                // generate wait_synced event listener
                board_w
                    .notifiers
                    .entry(id.clone())
                    .or_insert_with(Event::new)
                    .listen()
            };
            let wait_synced_timeout = *self.curp.timeout().wait_synced_timeout();
            if tokio::time::timeout(wait_synced_timeout, listener)
                .await
                .is_err()
            {
                let _ignored = self.cmd_board.write().notifiers.remove(&id);
                warn!("wait synced timeout for {id:?}");
                break WaitSyncedResponse::new_error(&SyncError::Timeout)?;
            }
        };

        debug!("{} wait synced for {id:?} finishes", self.curp.id());
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
    async fn tick_task(curp: Arc<RawCurp<C>>, connects: Connects) {
        let heartbeat_interval = *curp.timeout().heartbeat_interval();
        // wait for some random time before tick starts to minimize vote split possibility
        let rand = thread_rng()
            .gen_range(0..heartbeat_interval.as_millis())
            .numeric_cast();
        tokio::time::sleep(Duration::from_millis(rand)).await;

        let mut ticker = tokio::time::interval(heartbeat_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        #[allow(clippy::integer_arithmetic, unused_attributes)] // tokio internal triggered
        loop {
            let _now = ticker.tick().await;
            let action = curp.tick();
            match action {
                TickAction::Heartbeat(hbs) => {
                    Self::bcast_heartbeats(Arc::clone(&curp), &connects, hbs).await;
                }
                TickAction::Votes(votes) => {
                    Self::bcast_votes(Arc::clone(&curp), &connects, votes).await;
                }
                TickAction::Nothing => {}
            }
        }
    }

    /// Background leader calibrate followers
    async fn calibrate_task(
        curp: Arc<RawCurp<C>>,
        connects: Connects,
        mut calibrate_rx: mpsc::UnboundedReceiver<ServerId>,
    ) {
        let mut handlers: HashMap<ServerId, JoinHandle<()>> = HashMap::new();
        while let Some(follower_id) = calibrate_rx.recv().await {
            if handlers
                .get(&follower_id)
                .map_or(false, |hd| !hd.is_finished())
            {
                continue;
            }
            let connect = connects
                .get(&follower_id)
                .cloned()
                .unwrap_or_else(|| unreachable!("no server {follower_id}'s connect"));
            let hd = tokio::spawn(Self::leader_calibrates_follower(Arc::clone(&curp), connect));
            let _prev_hd = handlers.insert(follower_id, hd);
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
        timeout: Arc<ServerTimeout>,
        tx_filter: Option<Box<dyn TxFilter>>,
        storage_path: impl AsRef<Path>,
    ) -> Result<Self, CurpError> {
        let (sync_tx, sync_rx) = mpsc::unbounded_channel();
        let (calibrate_tx, calibrate_rx) = mpsc::unbounded_channel();
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        let shutdown_trigger = Arc::new(Event::new());
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));
        let uncommitted_pool = Arc::new(Mutex::new(UncommittedPool::new()));

        let storage = Arc::new(
            RocksDBStorage::new(storage_path).map_err(|err| CurpError::Storage(Box::new(err)))?,
        );

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
                timeout,
                Box::new(exe_tx),
                sync_tx,
                calibrate_tx,
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
                timeout,
                Box::new(exe_tx),
                sync_tx,
                calibrate_tx,
                log_tx,
                voted_for,
                entries,
            ))
        };

        run_gc_tasks(Arc::clone(&cmd_board), Arc::clone(&spec_pool));

        let curp_c = Arc::clone(&curp);
        let shutdown_trigger_c = Arc::clone(&shutdown_trigger);
        let storage_c = Arc::clone(&storage);
        let _ig = tokio::spawn(async move {
            // establish connection with other servers
            let connects = rpc::connect(others, tx_filter).await;
            let tick_task = tokio::spawn(Self::tick_task(Arc::clone(&curp_c), connects.clone()));
            let sync_task = tokio::spawn(Self::sync_task(
                Arc::clone(&curp_c),
                connects.clone(),
                sync_rx,
            ));
            let calibrate_task = tokio::spawn(Self::calibrate_task(curp_c, connects, calibrate_rx));
            let log_persist_task = tokio::spawn(Self::log_persist_task(log_rx, storage_c));
            shutdown_trigger_c.listen().await;
            tick_task.abort();
            sync_task.abort();
            calibrate_task.abort();
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

    /// Leader broadcasts heartbeats
    async fn bcast_heartbeats(
        curp: Arc<RawCurp<C>>,
        connects: &Connects,
        hbs: HashMap<ServerId, AppendEntries<C>>,
    ) {
        let rpc_timeout = *curp.timeout().rpc_timeout();
        let resps = hbs
            .into_iter()
            .map(|(id, hb)| {
                let connect = connects
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| unreachable!("no server {id}'s connect"));
                let req = AppendEntriesRequest::new_heartbeat(
                    hb.term,
                    hb.leader_id,
                    hb.prev_log_index,
                    hb.prev_log_term,
                    hb.leader_commit,
                );
                async move {
                    let resp = connect.append_entries(req, rpc_timeout).await;
                    (id, resp)
                }
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|(id, resp)| async move {
                match resp {
                    Err(e) => {
                        warn!("heartbeat to {} failed, {e}", id);
                        None
                    }
                    Ok(resp) => Some((id, resp.into_inner())),
                }
            });
        pin_mut!(resps);
        while let Some((id, resp)) = resps.next().await {
            let result = curp.handle_append_entries_resp(
                &id,
                None,
                resp.term,
                resp.success,
                resp.hint_index.numeric_cast(),
            );
            if result.is_err() {
                return;
            }
        }
    }

    /// Candidate broadcasts votes
    async fn bcast_votes(
        curp: Arc<RawCurp<C>>,
        connects: &Connects,
        votes: HashMap<ServerId, Vote>,
    ) {
        let rpc_timeout = *curp.timeout().rpc_timeout();
        let resps = votes
            .into_iter()
            .map(|(id, vote)| {
                let connect = connects
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| unreachable!("no server {id}'s connect"));
                let req = VoteRequest::new(
                    vote.term,
                    vote.candidate_id,
                    vote.last_log_index,
                    vote.last_log_term,
                );
                async move {
                    let resp = connect.vote(req, rpc_timeout).await;
                    (id, resp)
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

    /// Leader calibrates a follower
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn leader_calibrates_follower(curp: Arc<RawCurp<C>>, connect: Arc<dyn ConnectApi>) {
        debug!("{} starts calibrating follower {}", curp.id(), connect.id());
        let (rpc_timeout, retry_timeout) = (
            *curp.timeout().rpc_timeout(),
            *curp.timeout().retry_timeout(),
        );
        loop {
            // send append entry
            let Ok(ae) = curp.append_entries(connect.id()) else {
                return;
            };
            let last_sent_index = ae.prev_log_index + ae.entries.len();
            let req = match AppendEntriesRequest::new(
                ae.term,
                ae.leader_id,
                ae.prev_log_index,
                ae.prev_log_term,
                ae.entries,
                ae.leader_commit,
            ) {
                Err(e) => {
                    error!("unable to serialize append entries request: {}", e);
                    return;
                }
                Ok(req) => req,
            };

            let resp = connect.append_entries(req, rpc_timeout).await;

            #[allow(clippy::unwrap_used)]
            // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
            match resp {
                Err(e) => {
                    warn!("append_entries error: {e}");
                }
                Ok(resp) => {
                    let resp = resp.into_inner();

                    let result = curp.handle_append_entries_resp(
                        connect.id(),
                        Some(last_sent_index),
                        resp.term,
                        resp.success,
                        resp.hint_index.numeric_cast(),
                    );

                    match result {
                        Ok(true) => {
                            debug!(
                                "{} successfully calibrates follower {}",
                                curp.id(),
                                connect.id()
                            );
                            return;
                        }
                        Ok(false) => {}
                        Err(()) => {
                            return;
                        }
                    }
                }
            };
            tokio::time::sleep(retry_timeout).await;
        }
    }

    /// Sync task is responsible for replicating log entries
    async fn sync_task(
        curp: Arc<RawCurp<C>>,
        connects: Connects,
        mut sync_rx: mpsc::UnboundedReceiver<usize>,
    ) {
        while let Some(i) = sync_rx.recv().await {
            let req = {
                let Ok(ae) = curp.append_entries_single(i) else {
                    continue;
                };
                let req = AppendEntriesRequest::new(
                    ae.term,
                    ae.leader_id,
                    ae.prev_log_index,
                    ae.prev_log_term,
                    ae.entries,
                    ae.leader_commit,
                );
                match req {
                    Err(e) => {
                        error!("can't serialize log entries, {e}");
                        continue;
                    }
                    Ok(req) => req,
                }
            };
            // send append_entries to each server in parallel
            for connect in connects.values() {
                let _handle = tokio::spawn(Self::send_log_until_succeed(
                    Arc::clone(&curp),
                    Arc::clone(connect),
                    i,
                    req.clone(),
                ));
            }
            curp.opt_out_hb();
        }
    }

    /// Send `append_entries` containing a single log to a server
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn send_log_until_succeed(
        curp: Arc<RawCurp<C>>,
        connect: Arc<dyn ConnectApi>,
        i: usize,
        req: AppendEntriesRequest,
    ) {
        let (rpc_timeout, retry_timeout) = (
            *curp.timeout().rpc_timeout(),
            *curp.timeout().retry_timeout(),
        );
        // send log[i] until succeed
        loop {
            let resp = connect.append_entries(req.clone(), rpc_timeout).await;

            #[allow(clippy::unwrap_used)]
            // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
            match resp {
                Err(e) => {
                    warn!("append_entries error: {e}");
                }
                Ok(resp) => {
                    let resp = resp.into_inner();

                    let result = curp.handle_append_entries_resp(
                        connect.id(),
                        Some(i),
                        resp.term,
                        resp.success,
                        resp.hint_index.numeric_cast(),
                    );

                    match result {
                        Ok(true) | Err(()) => return,
                        Ok(false) => {
                            warn!("{} failed to send log {i}, retrying", curp.id());
                        }
                    }
                }
            }
            // wait for some time until next retry
            tokio::time::sleep(retry_timeout).await;
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

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;
    use utils::config::default_retry_timeout;

    use super::*;
    use crate::{
        log_entry::LogEntry,
        rpc::connect::MockConnectApi,
        server::cmd_worker::MockCmdExeSenderApi,
        test_utils::{sleep_millis, test_cmd::TestCommand},
    };

    /*************** tests for send_log_until_succeed **************/

    #[traced_test]
    #[tokio::test]
    async fn logs_will_be_resent() {
        let curp = Arc::new(RawCurp::new_test(
            3,
            MockCmdExeSenderApi::<TestCommand>::default(),
        ));

        let mut mock_connect = MockConnectApi::default();
        mock_connect
            .expect_append_entries()
            .times(3..)
            .returning(|_, _| Err(ProposeError::RpcStatus("timeout".to_owned())));
        mock_connect.expect_id().return_const("S1".to_owned());

        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                "S0".to_owned(),
                0,
                0,
                vec![LogEntry::new(1, 1, Arc::new(TestCommand::default()))],
                0,
            )
            .unwrap();
            CurpNode::send_log_until_succeed(curp, Arc::new(mock_connect), 1, req).await;
        });
        sleep_millis(default_retry_timeout().as_millis() as u64 * 4).await;
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[traced_test]
    #[tokio::test]
    async fn send_log_will_stop_after_new_election() {
        let curp = {
            let mut exe_tx = MockCmdExeSenderApi::<TestCommand>::default();
            exe_tx.expect_send_reset().returning(|| ());
            Arc::new(RawCurp::new_test(3, exe_tx))
        };

        let mut mock_connect = MockConnectApi::default();
        mock_connect.expect_append_entries().returning(|_, _| {
            Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                2, 0,
            )))
        });
        mock_connect.expect_id().return_const("S1".to_owned());

        let curp_c = Arc::clone(&curp);
        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                "S0".to_owned(),
                0,
                0,
                vec![LogEntry::new(1, 1, Arc::new(TestCommand::default()))],
                0,
            )
            .unwrap();
            CurpNode::send_log_until_succeed(curp, Arc::new(mock_connect), 1, req).await;
        });

        assert!(handle.await.is_ok());
        assert_eq!(curp_c.term(), 2);
    }

    #[traced_test]
    #[tokio::test]
    async fn send_log_will_succeed_and_commit() {
        let curp = {
            let mut exe_tx = MockCmdExeSenderApi::<TestCommand>::default();
            exe_tx.expect_send_reset().returning(|| ());
            exe_tx.expect_send_after_sync().returning(|_, _| ());
            Arc::new(RawCurp::new_test(3, exe_tx))
        };
        let cmd = Arc::new(TestCommand::default());
        curp.push_cmd(Arc::clone(&cmd));

        let mut mock_connect = MockConnectApi::default();
        mock_connect
            .expect_append_entries()
            .returning(|_, _| Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))));
        mock_connect.expect_id().return_const("S1".to_owned());
        let curp_c = Arc::clone(&curp);
        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                "S0".to_owned(),
                0,
                0,
                vec![LogEntry::new(1, 1, cmd)],
                0,
            )
            .unwrap();
            CurpNode::send_log_until_succeed(curp, Arc::new(mock_connect), 1, req).await;
        });

        assert!(handle.await.is_ok());
        assert_eq!(curp_c.term(), 0);
        assert_eq!(curp_c.commit_index(), 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn leader_will_calibrate_follower_until_succeed() {
        let curp = {
            let mut exe_tx = MockCmdExeSenderApi::<TestCommand>::default();
            exe_tx.expect_send_after_sync().returning(|_, _| ());
            Arc::new(RawCurp::new_test(3, exe_tx))
        };
        let cmd1 = Arc::new(TestCommand::default());
        curp.push_cmd(Arc::clone(&cmd1));
        let cmd2 = Arc::new(TestCommand::default());
        curp.push_cmd(Arc::clone(&cmd2));

        let mut mock_connect = MockConnectApi::default();
        let mut call_cnt = 0;
        mock_connect
            .expect_append_entries()
            .times(2)
            .returning(move |_, _| {
                call_cnt += 1;
                match call_cnt {
                    1 => Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                        0, 1,
                    ))),
                    2 => Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))),
                    _ => panic!("should not be called more than 2 times"),
                }
            });
        mock_connect.expect_id().return_const("S1".to_owned());

        CurpNode::leader_calibrates_follower(curp, Arc::new(mock_connect)).await;
    }
}
