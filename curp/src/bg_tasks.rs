use std::{iter, ops::Range, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use futures::future::Either;
use lock_utils::parking_lot_lock::RwLockMap;
use madsim::rand::{thread_rng, Rng};
use parking_lot::{lock_api::RwLockUpgradableReadGuard, Mutex, RwLock};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tracing::{debug, error, info, warn};

use crate::{
    cmd::{Command, CommandExecutor},
    cmd_board::{CmdState, CommandBoard},
    cmd_execute_worker::{execute_worker, CmdExeReceiver, CmdExeSender, N_EXECUTE_WORKERS},
    log::LogEntry,
    message::TermNum,
    rpc::{self, AppendEntriesRequest, Connect, VoteRequest, WaitSyncedResponse},
    server::{ServerRole, SpeculativePool, State},
    shutdown::Shutdown,
    LogIndex,
};

/// Run background tasks
#[allow(clippy::too_many_arguments)] // we call this function once, it's ok
pub(crate) async fn run_bg_tasks<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    state: Arc<RwLock<State<C>>>,
    last_rpc_time: Arc<RwLock<Instant>>,
    sync_chan: flume::Receiver<SyncMessage<C>>,
    cmd_executor: CE,
    spec: Arc<Mutex<SpeculativePool<C>>>,
    cmd_exe_tx: CmdExeSender<C>,
    cmd_exe_rx: CmdExeReceiver<C>,
    cmd_board: Arc<Mutex<CommandBoard>>,
    mut shutdown: Shutdown,
) {
    let others = state.read().others.clone();
    // establish connection with other servers
    let connects = rpc::try_connect(others.into_iter().collect()).await;

    // notify when a broadcast of append_entries is needed immediately
    let (ae_trigger, ae_trigger_rx) = mpsc::unbounded_channel::<usize>();

    let bg_ae_handle = tokio::spawn(bg_append_entries(
        connects.clone(),
        Arc::clone(&state),
        ae_trigger_rx,
    ));
    let bg_election_handle = tokio::spawn(bg_election(
        connects.clone(),
        Arc::clone(&state),
        Arc::clone(&last_rpc_time),
    ));
    let bg_apply_handle = tokio::spawn(bg_apply(Arc::clone(&state), cmd_exe_tx, spec, cmd_board));
    let bg_heartbeat_handle = tokio::spawn(bg_heartbeat(connects.clone(), Arc::clone(&state)));
    let bg_get_sync_cmds_handle =
        tokio::spawn(bg_get_sync_cmds(Arc::clone(&state), sync_chan, ae_trigger));
    let calibrate_handle = tokio::spawn(leader_calibrates_followers(connects, state));

    // spawn cmd execute worker
    let bg_exe_worker_handles: Vec<JoinHandle<_>> =
        iter::repeat((cmd_exe_rx, Arc::new(cmd_executor)))
            .take(N_EXECUTE_WORKERS)
            .map(|(rx, ce)| tokio::spawn(execute_worker(rx, ce)))
            .collect();

    shutdown.recv().await;
    bg_ae_handle.abort();
    bg_election_handle.abort();
    bg_apply_handle.abort();
    bg_heartbeat_handle.abort();
    bg_get_sync_cmds_handle.abort();
    calibrate_handle.abort();
    for handle in bg_exe_worker_handles {
        handle.abort();
    }
    info!("all background task stopped");
}

/// The message sent to the background sync task
pub(crate) struct SyncMessage<C>
where
    C: Command,
{
    /// Term number
    term: TermNum,
    /// Command
    cmd: Arc<C>,
}

impl<C> SyncMessage<C>
where
    C: Command,
{
    /// Create a new `SyncMessage`
    pub(crate) fn new(term: TermNum, cmd: Arc<C>) -> Self {
        Self { term, cmd }
    }

    /// Get all values from the message
    fn inner(self) -> (TermNum, Arc<C>) {
        (self.term, self.cmd)
    }
}

/// Fetch commands need to be synced and add them to the log
async fn bg_get_sync_cmds<C: Command + 'static>(
    state: Arc<RwLock<State<C>>>,
    sync_chan: flume::Receiver<SyncMessage<C>>,
    ae_trigger: mpsc::UnboundedSender<usize>,
) {
    loop {
        let (term, cmd) = match sync_chan.recv_async().await {
            Ok(msg) => msg.inner(),
            Err(_) => {
                return;
            }
        };

        #[allow(clippy::shadow_unrelated)] // clippy false positive
        state.map_write(|mut state| {
            state.log.push(LogEntry::new(term, &[cmd]));
            if let Err(e) = ae_trigger.send(state.last_log_index()) {
                error!("ae_trigger failed: {}", e);
            }

            debug!(
                "received new log, index {:?}",
                state.log.len().checked_sub(1),
            );
        });
    }
}

/// Interval between sending heartbeats
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
/// Rpc request timeout
const RPC_TIMEOUT: Duration = Duration::from_millis(50);

/// Background `append_entries`, only works for the leader
async fn bg_append_entries<C: Command + 'static>(
    connects: Vec<Arc<Connect>>,
    state: Arc<RwLock<State<C>>>,
    mut ae_trigger_rx: mpsc::UnboundedReceiver<usize>,
) {
    while let Some(i) = ae_trigger_rx.recv().await {
        let req = {
            let state = state.read();
            if !state.is_leader() {
                warn!("Non leader receives sync log[{i}] request");
                continue;
            }

            // log.len() >= 1 because we have a fake log[0]
            #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
            match AppendEntriesRequest::new(
                state.term,
                i - 1,
                state.log[i - 1].term(),
                vec![state.log[i].clone()],
                state.commit_index,
            ) {
                Err(e) => {
                    error!("unable to serialize append entries request: {}", e);
                    continue;
                }
                Ok(req) => (req),
            }
        };

        // send append_entries to each server in parallel
        for connect in &connects {
            let _handle = tokio::spawn(send_log_until_succeed(
                i,
                req.clone(),
                Arc::clone(connect),
                Arc::clone(&state),
            ));
        }
    }
}

/// Send `append_entries` containing a single log to a server
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn send_log_until_succeed<C: Command + 'static>(
    i: usize,
    req: AppendEntriesRequest,
    connect: Arc<Connect>,
    state: Arc<RwLock<State<C>>>,
) {
    // send log[i] until succeed
    loop {
        debug!("append_entries sent to {}", connect.addr);
        let resp = connect.append_entries(req.clone(), RPC_TIMEOUT).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => warn!("append_entries error: {}", e),
            Ok(resp) => {
                let resp = resp.into_inner();
                let state = state.upgradable_read();

                // calibrate term
                if resp.term > state.term {
                    let mut state = RwLockUpgradableReadGuard::upgrade(state);
                    state.update_to_term(resp.term);
                    return;
                }

                if resp.success {
                    let mut state = RwLockUpgradableReadGuard::upgrade(state);
                    // update match_index and next_index
                    let match_index = state.match_index.get_mut(&connect.addr).unwrap();
                    if *match_index < i {
                        *match_index = i;
                    }
                    *state.next_index.get_mut(&connect.addr).unwrap() = *match_index + 1;

                    let min_replicated = (state.others.len() + 1) / 2;
                    // If the majority of servers has replicated the log, commit
                    if state.commit_index < i
                        && state.log[i].term() == state.term
                        && state
                            .others
                            .iter()
                            .filter(|addr| state.match_index[*addr] >= i)
                            .count()
                            >= min_replicated
                    {
                        state.commit_index = i;
                        debug!("commit_index updated to {i}");
                        state.commit_trigger.notify(1);
                    }
                    break;
                }
            }
        }
    }
}

/// Background `append_entries`, only works for the leader
async fn bg_heartbeat<C: Command + 'static>(
    connects: Vec<Arc<Connect>>,
    state: Arc<RwLock<State<C>>>,
) {
    let role_trigger = state.read().role_trigger();
    #[allow(clippy::integer_arithmetic)] // tokio internal triggered
    loop {
        // only leader should run this task
        while !state.read().is_leader() {
            role_trigger.listen().await;
        }

        tokio::time::sleep(HEARTBEAT_INTERVAL).await;

        // send append_entries to each server in parallel
        for connect in &connects {
            let _handle = tokio::spawn(send_heartbeat(Arc::clone(connect), Arc::clone(&state)));
        }
    }
}

/// Send `append_entries` to a server
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn send_heartbeat<C: Command + 'static>(connect: Arc<Connect>, state: Arc<RwLock<State<C>>>) {
    // prepare append_entries request args
    #[allow(clippy::shadow_unrelated)] // clippy false positive
    let (term, prev_log_index, prev_log_term, leader_commit) = state.map_read(|state| {
        let next_index = state.next_index[&connect.addr];
        (
            state.term,
            next_index - 1,
            state.log[next_index - 1].term(),
            state.commit_index,
        )
    });
    let req =
        AppendEntriesRequest::new_heartbeat(term, prev_log_index, prev_log_term, leader_commit);

    // send append_entries request and receive response
    debug!("heartbeat sent to {}", connect.addr);
    let resp = connect.append_entries(req, RPC_TIMEOUT).await;

    #[allow(clippy::unwrap_used)]
    // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    match resp {
        Err(e) => warn!("append_entries error: {}", e),
        Ok(resp) => {
            let resp = resp.into_inner();
            // calibrate term
            let state = state.upgradable_read();
            if resp.term > state.term {
                let mut state = RwLockUpgradableReadGuard::upgrade(state);
                state.update_to_term(resp.term);
                return;
            }
            if !resp.success {
                let mut state = RwLockUpgradableReadGuard::upgrade(state);
                *state.next_index.get_mut(&connect.addr).unwrap() -= 1;
            }
        }
    };
}

/// Background apply
async fn bg_apply<C: Command + 'static>(
    state: Arc<RwLock<State<C>>>,
    exe_tx: CmdExeSender<C>,
    spec: Arc<Mutex<SpeculativePool<C>>>,
    cmd_board: Arc<Mutex<CommandBoard>>,
) {
    let commit_trigger = state.read().commit_trigger();
    loop {
        // wait until there is something to commit
        let state = loop {
            {
                let state = state.upgradable_read();
                if state.need_commit() {
                    break state;
                }
            }
            commit_trigger.listen().await;
        };

        let mut state = RwLockUpgradableReadGuard::upgrade(state);
        #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
        // TODO: overflow of log index should be prevented
        for i in (state.last_applied + 1)..=state.commit_index {
            for cmd in state.log[i].cmds().iter() {
                let cmd_id = cmd.id();
                if state.is_leader() {
                    handle_after_sync_leader(
                        Arc::clone(&cmd_board),
                        Arc::clone(cmd),
                        i.numeric_cast(),
                        &exe_tx,
                    );
                } else {
                    handle_after_sync_follower(&exe_tx, Arc::clone(cmd), i.numeric_cast());
                }
                spec.lock().mark_ready(cmd_id);
            }
            state.last_applied = i;
            debug!("log[{i}] committed, last_applied updated to {}", i);
        }
    }
}

/// The leader handles after sync
fn handle_after_sync_leader<C: Command + 'static>(
    cmd_board: Arc<Mutex<CommandBoard>>,
    cmd: Arc<C>,
    index: LogIndex,
    exe_tx: &CmdExeSender<C>,
) {
    let cmd_id = cmd.id().clone();

    let needs_execute = {
        // the leader will see if the command needs execution from cmd board
        let cmd_board = cmd_board.lock();
        let cmd_state = if let Some(cmd_state) = cmd_board.cmd_states.get(cmd.id()) {
            cmd_state
        } else {
            error!("No cmd {:?} in command board", cmd.id());
            return;
        };
        match *cmd_state {
            CmdState::Execute => true,
            CmdState::AfterSync => false,
            CmdState::EarlyArrive | CmdState::FinalResponse(_) => {
                error!("should not get state {:?} before after sync", cmd_state);
                return;
            }
        }
    };

    let resp = if needs_execute {
        let result = exe_tx.send_exe_and_after_sync(cmd, index);
        Either::Left(async move {
            result.await.map_or_else(
                |e| {
                    WaitSyncedResponse::new_error(&format!(
                        "can't get execution and after sync result, {e}"
                    ))
                },
                |(er, asr)| WaitSyncedResponse::new_from_result::<C>(Some(er), asr),
            )
        })
    } else {
        let result = exe_tx.send_after_sync(cmd, index);
        Either::Right(async move {
            result.await.map_or_else(
                |e| WaitSyncedResponse::new_error(&format!("can't get after sync result, {e}")),
                |asr| WaitSyncedResponse::new_from_result::<C>(None, Some(asr)),
            )
        })
    };

    // update the cmd_board after execution and after_sync is completed
    let _ignored = tokio::spawn(async move {
        let resp = resp.await;

        let mut cmd_board = cmd_board.lock();
        let cmd_state = if let Some(cmd_state) = cmd_board.cmd_states.get_mut(&cmd_id) {
            cmd_state
        } else {
            error!("No cmd {:?} in command board", cmd_id);
            return;
        };
        *cmd_state = CmdState::FinalResponse(resp);

        // now we can notify the waiting request
        if let Some(notify) = cmd_board.notifiers.get(&cmd_id) {
            notify.notify(usize::MAX);
        }
    });
}

/// The follower handles after sync
fn handle_after_sync_follower<C: Command + 'static>(
    exe_tx: &CmdExeSender<C>,
    cmd: Arc<C>,
    index: LogIndex,
) {
    // FIXME: should follower store er and asr in case it becomes leader later?
    let _ignore = exe_tx.send_exe_and_after_sync(cmd, index);
}

/// How long a candidate should wait before it starts another round of election
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a follower should wait before it starts a round of election (in millis)
const FOLLOWER_TIMEOUT: Range<u64> = 1000..2000;

/// Background election
async fn bg_election<C: Command + 'static>(
    connects: Vec<Arc<Connect>>,
    state: Arc<RwLock<State<C>>>,
    last_rpc_time: Arc<RwLock<Instant>>,
) {
    let role_trigger = state.read().role_trigger();
    loop {
        // only follower or candidate should run this task
        while state.read().is_leader() {
            role_trigger.listen().await;
        }

        let current_role = state.read().role();
        let start_vote = match current_role {
            ServerRole::Follower => {
                let timeout = Duration::from_millis(thread_rng().gen_range(FOLLOWER_TIMEOUT));
                // wait until it needs to vote
                loop {
                    let next_check = last_rpc_time.read().to_owned() + timeout;
                    tokio::time::sleep_until(next_check).await;
                    if Instant::now() - *last_rpc_time.read() > timeout {
                        break;
                    }
                }
                true
            }
            ServerRole::Candidate => loop {
                let next_check = last_rpc_time.read().to_owned() + CANDIDATE_TIMEOUT;
                tokio::time::sleep_until(next_check).await;
                // check election status
                match state.read().role() {
                    // election failed, becomes a follower || election succeeded, becomes a leader
                    ServerRole::Follower | ServerRole::Leader => {
                        break false;
                    }
                    ServerRole::Candidate => {}
                }
                if Instant::now() - *last_rpc_time.read() > CANDIDATE_TIMEOUT {
                    break true;
                }
            },
            ServerRole::Leader => false, // leader should not vote
        };
        if !start_vote {
            continue;
        }

        // start election
        #[allow(clippy::integer_arithmetic)] // TODO: handle possible overflow
        let req = {
            let mut state = state.write();
            let new_term = state.term + 1;
            state.update_to_term(new_term);
            state.set_role(ServerRole::Candidate);
            state.voted_for = Some(state.id.clone());
            state.votes_received = 1;
            VoteRequest::new(
                state.term,
                state.id.clone(),
                state.last_log_index(),
                state.last_log_term(),
            )
        };
        // reset
        *last_rpc_time.write() = Instant::now();
        debug!("server {} starts election", req.candidate_id);

        for connect in &connects {
            let _ignore = tokio::spawn(send_vote(
                Arc::clone(connect),
                Arc::clone(&state),
                req.clone(),
            ));
        }
    }
}

/// send vote request
async fn send_vote<C: Command + 'static>(
    connect: Arc<Connect>,
    state: Arc<RwLock<State<C>>>,
    req: VoteRequest,
) {
    let resp = connect.vote(req, RPC_TIMEOUT).await;
    match resp {
        Err(e) => error!("vote failed, {}", e),
        Ok(resp) => {
            let resp = resp.into_inner();

            // calibrate term
            let state = state.upgradable_read();
            if resp.term > state.term {
                let mut state = RwLockUpgradableReadGuard::upgrade(state);
                state.update_to_term(resp.term);
                return;
            }

            // is still a candidate
            if !matches!(state.role(), ServerRole::Candidate) {
                return;
            }

            #[allow(clippy::integer_arithmetic)]
            if resp.vote_granted {
                debug!("vote is granted by server {}", connect.addr);
                let mut state = RwLockUpgradableReadGuard::upgrade(state);
                state.votes_received += 1;

                // the majority has granted the vote
                let min_granted = (state.others.len() + 1) / 2 + 1;
                if state.votes_received >= min_granted {
                    state.set_role(ServerRole::Leader);
                    debug!("server becomes leader");

                    // init next_index
                    let last_log_index = state.last_log_index();
                    for index in state.next_index.values_mut() {
                        *index = last_log_index + 1; // iter from the end to front is more likely to match the follower
                    }

                    // trigger heartbeat immediately to establish leadership
                    state.calibrate_trigger.notify(1);
                }
            }
        }
    }
}

/// Leader should first enforce followers to be consistent with it when it comes to power
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn leader_calibrates_followers<C: Command + 'static>(
    connects: Vec<Arc<Connect>>,
    state: Arc<RwLock<State<C>>>,
) {
    let calibrate_trigger = Arc::clone(&state.read().calibrate_trigger);
    loop {
        calibrate_trigger.listen().await;
        for connect in connects.iter().cloned() {
            let state = Arc::clone(&state);
            let _handle = tokio::spawn(async move {
                loop {
                    // send append entry
                    #[allow(clippy::shadow_unrelated)] // clippy false positive
                    let (
                        term,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                        last_sent_index,
                    ) = state.map_read(|state| {
                        let next_index = state.next_index[&connect.addr];
                        (
                            state.term,
                            next_index - 1,
                            state.log[next_index - 1].term(),
                            state.log[next_index..].to_vec(),
                            state.commit_index,
                            state.log.len() - 1,
                        )
                    });
                    let req = match AppendEntriesRequest::new(
                        term,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    ) {
                        Err(e) => {
                            error!("unable to serialize append entries request: {}", e);
                            return;
                        }
                        Ok(req) => req,
                    };

                    let resp = connect.append_entries(req, RPC_TIMEOUT).await;

                    #[allow(clippy::unwrap_used)]
                    // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
                    match resp {
                        Err(e) => warn!("append_entries error: {}", e),
                        Ok(resp) => {
                            let resp = resp.into_inner();
                            // calibrate term
                            let state = state.upgradable_read();
                            if resp.term > state.term {
                                let mut state = RwLockUpgradableReadGuard::upgrade(state);
                                state.update_to_term(resp.term);
                                return;
                            }

                            let mut state = RwLockUpgradableReadGuard::upgrade(state);

                            // successfully calibrate
                            if resp.success {
                                *state.next_index.get_mut(&connect.addr).unwrap() =
                                    last_sent_index + 1;
                                *state.match_index.get_mut(&connect.addr).unwrap() =
                                    last_sent_index;
                                break;
                            }

                            *state.next_index.get_mut(&connect.addr).unwrap() =
                                (resp.commit_index + 1).numeric_cast();
                        }
                    };
                }
            });
        }
    }
}
