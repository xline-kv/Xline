#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::{iter, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use futures::future::Either;
use madsim::rand::{thread_rng, Rng};
use parking_lot::{lock_api::RwLockUpgradableReadGuard, Mutex, RwLock};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tracing::{debug, error, info, warn};
use utils::parking_lot_lock::RwLockMap;

use super::{
    cmd_board::{CmdState, CommandBoard},
    SyncMessage,
};
use crate::{
    cmd::{Command, CommandExecutor},
    log::LogEntry,
    rpc::{connect::ConnectInterface, AppendEntriesRequest, VoteRequest, WaitSyncedResponse},
    server::{
        cmd_execute_worker::{
            execute_worker, CmdExeReceiver, CmdExeSender, CmdExeSenderInterface, N_EXECUTE_WORKERS,
        },
        ServerRole, SpeculativePool, State,
    },
    shutdown::Shutdown,
    LogIndex,
};

/// Run background tasks
#[allow(clippy::too_many_arguments)] // we call this function once, it's ok
pub(super) async fn run_bg_tasks<
    C: Command + 'static,
    CE: 'static + CommandExecutor<C>,
    Conn: ConnectInterface,
>(
    state: Arc<RwLock<State<C>>>,
    sync_chan: flume::Receiver<SyncMessage<C>>,
    cmd_executor: CE,
    spec: Arc<Mutex<SpeculativePool<C>>>,
    cmd_exe_tx: CmdExeSender<C>,
    cmd_exe_rx: CmdExeReceiver<C>,
    mut shutdown: Shutdown,
    #[cfg(test)] reachable: Arc<AtomicBool>,
) {
    // establish connection with other servers
    let others = state.read().others.clone();

    #[cfg(test)]
    let connects = Conn::try_connect_test(others, reachable).await;
    #[cfg(not(test))]
    let connects = Conn::try_connect(others).await;

    // notify when a broadcast of append_entries is needed immediately
    let (ae_trigger, ae_trigger_rx) = mpsc::unbounded_channel::<usize>();

    let bg_ae_handle = tokio::spawn(bg_append_entries(
        connects.clone(),
        Arc::clone(&state),
        ae_trigger_rx,
    ));
    let bg_election_handle = tokio::spawn(bg_election(connects.clone(), Arc::clone(&state)));
    let bg_apply_handle = tokio::spawn(bg_apply(Arc::clone(&state), cmd_exe_tx, spec));
    let bg_heartbeat_handle = tokio::spawn(bg_heartbeat(connects.clone(), Arc::clone(&state)));
    let bg_get_sync_cmds_handle =
        tokio::spawn(bg_get_sync_cmds(Arc::clone(&state), sync_chan, ae_trigger));
    let calibrate_handle = tokio::spawn(bg_leader_calibrates_followers(connects, state));

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

        state.map_write(|mut state_w| {
            state_w.log.push(LogEntry::new(term, &[cmd]));
            if let Err(e) = ae_trigger.send(state_w.last_log_index()) {
                error!("ae_trigger failed: {}", e);
            }

            debug!(
                "received new log, index {:?}",
                state_w.log.len().checked_sub(1),
            );
        });
    }
}

/// Background `append_entries`, only works for the leader
async fn bg_append_entries<C: Command + 'static, Conn: ConnectInterface>(
    connects: Vec<Arc<Conn>>,
    state: Arc<RwLock<State<C>>>,
    mut ae_trigger_rx: mpsc::UnboundedReceiver<usize>,
) {
    let hb_reset_trigger = state.read().heartbeat_reset_trigger();
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
                state.id().clone(),
                i - 1,
                state.log[i - 1].term(),
                vec![state.log[i].clone()],
                state.commit_index,
            ) {
                Err(e) => {
                    error!("unable to serialize append entries request: {}", e);
                    continue;
                }
                Ok(req) => req,
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

        hb_reset_trigger.notify(1);
    }
}

/// Send `append_entries` containing a single log to a server
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn send_log_until_succeed<C: Command + 'static, Conn: ConnectInterface>(
    i: usize,
    req: AppendEntriesRequest,
    connect: Arc<Conn>,
    state: Arc<RwLock<State<C>>>,
) {
    let (retry_timeout, rpc_timeout) = state.map_read(|state_r| {
        (
            *state_r.timeout.retry_timeout(),
            *state_r.timeout.rpc_timeout(),
        )
    });
    // send log[i] until succeed
    loop {
        debug!("append_entries sent to {}", connect.id());
        let resp = connect.append_entries(req.clone(), *rpc_timeout).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {e}");
                // wait for some time until next retry
                tokio::time::sleep(*retry_timeout).await;
            }
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
                    let match_index = state.match_index.get_mut(connect.id()).unwrap();
                    if *match_index < i {
                        *match_index = i;
                    }
                    *state.next_index.get_mut(connect.id()).unwrap() = *match_index + 1;

                    let min_replicated = (state.others.len() + 1) / 2;
                    // If the majority of servers has replicated the log, commit
                    if state.commit_index < i
                        && state.log[i].term() == state.term
                        && state
                            .others
                            .iter()
                            .filter(|&(id, _)| state.match_index[id] >= i)
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
async fn bg_heartbeat<C: Command + 'static, Conn: ConnectInterface>(
    connects: Vec<Arc<Conn>>,
    state: Arc<RwLock<State<C>>>,
) {
    let (role_trigger, hb_reset_trigger, heartbeat_interval) = state.map_read(|state_r| {
        (
            state_r.role_trigger(),
            state_r.heartbeat_reset_trigger(),
            *state_r.timeout.heartbeat_interval(),
        )
    });
    #[allow(clippy::integer_arithmetic)] // tokio internal triggered
    loop {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(*heartbeat_interval) => break,
                _ = hb_reset_trigger.listen() => {}
            }
        }

        // only leader should run this task
        while !state.read().is_leader() {
            role_trigger.listen().await;
        }
        // send append_entries to each server in parallel
        for connect in &connects {
            let _handle = tokio::spawn(send_heartbeat(Arc::clone(connect), Arc::clone(&state)));
        }
    }
}

/// Send `append_entries` to a server
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn send_heartbeat<C: Command + 'static, Conn: ConnectInterface>(
    connect: Arc<Conn>,
    state: Arc<RwLock<State<C>>>,
) {
    // prepare append_entries request args
    let (req, rpc_timeout) = state.map_read(|state_r| {
        let next_index = state_r.next_index[connect.id()];
        (
            AppendEntriesRequest::new_heartbeat(
                state_r.term,
                state_r.id().clone(),
                next_index - 1,
                state_r.log[next_index - 1].term(),
                state_r.commit_index,
            ),
            *state.read().timeout.rpc_timeout(),
        )
    });

    // send append_entries request and receive response
    debug!("heartbeat sent to {}", connect.id());
    let resp = connect.append_entries(req, *rpc_timeout).await;

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
                *state.next_index.get_mut(connect.id()).unwrap() =
                    (resp.commit_index + 1).numeric_cast();
            }
        }
    };
}

/// Background apply
async fn bg_apply<C: Command + 'static, Tx: CmdExeSenderInterface<C>>(
    state: Arc<RwLock<State<C>>>,
    exe_tx: Tx,
    spec: Arc<Mutex<SpeculativePool<C>>>,
) {
    let (commit_trigger, cmd_board) =
        state.map_read(|state_r| (state_r.commit_trigger(), state_r.cmd_board()));
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
fn handle_after_sync_leader<C: Command + 'static, Tx: CmdExeSenderInterface<C>>(
    cmd_board: Arc<Mutex<CommandBoard>>,
    cmd: Arc<C>,
    index: LogIndex,
    exe_tx: &Tx,
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
                    error!("can't receive exe result from exe worker, {e}");
                    WaitSyncedResponse::new_from_result::<C>(None, None)
                },
                |(er, asr)| WaitSyncedResponse::new_from_result::<C>(Some(er), asr),
            )
        })
    } else {
        let result = exe_tx.send_after_sync(cmd, index);
        Either::Right(async move {
            result.await.map_or_else(
                |e| {
                    error!("can't receive exe result from exe worker, {e}");
                    WaitSyncedResponse::new_from_result::<C>(None, None)
                },
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
fn handle_after_sync_follower<C: Command + 'static, Tx: CmdExeSenderInterface<C>>(
    exe_tx: &Tx,
    cmd: Arc<C>,
    index: LogIndex,
) {
    // FIXME: should follower store er and asr in case it becomes leader later?
    let _ignore = exe_tx.send_exe_and_after_sync(cmd, index);
}

/// Background election
async fn bg_election<C: Command + 'static, Conn: ConnectInterface>(
    connects: Vec<Arc<Conn>>,
    state: Arc<RwLock<State<C>>>,
) {
    let last_rpc_time = state.map_read(|state_r| state_r.last_rpc_time());
    loop {
        wait_for_election(Arc::clone(&state)).await;

        // start election
        #[allow(clippy::integer_arithmetic)] // TODO: handle possible overflow
        let req = {
            let mut state = state.write();
            let new_term = state.term + 1;
            state.update_to_term(new_term);
            state.set_role(ServerRole::Candidate);
            state.voted_for = Some(state.id().clone());
            state.votes_received = 1;
            VoteRequest::new(
                state.term,
                state.id().clone(),
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

/// wait until an election is needed
async fn wait_for_election<C: Command + 'static>(state: Arc<RwLock<State<C>>>) {
    let (role_trigger, last_rpc_time, follower_timeout_range, candidate_timeout) =
        state.map_read(|state_r| {
            (
                state_r.role_trigger(),
                state_r.last_rpc_time(),
                state_r.timeout.follower_timeout_range().clone(),
                *state_r.timeout.candidate_timeout(),
            )
        });
    'outer: loop {
        // only follower or candidate should try to elect
        let role = state.read().role();
        match role {
            ServerRole::Follower => {
                let timeout = Duration::from_millis(
                    thread_rng().gen_range((*follower_timeout_range).clone()),
                );
                // wait until it needs to vote
                loop {
                    let next_check = last_rpc_time.read().to_owned() + timeout;
                    tokio::time::sleep_until(next_check).await;
                    if Instant::now() - *last_rpc_time.read() > timeout {
                        break 'outer;
                    }
                }
            }
            ServerRole::Candidate => 'inner: loop {
                let next_check = last_rpc_time.read().to_owned() + *candidate_timeout;
                tokio::time::sleep_until(next_check).await;
                // check election status
                match state.read().role() {
                    // election failed, becomes a follower || election succeeded, becomes a leader
                    ServerRole::Follower | ServerRole::Leader => {
                        break 'inner;
                    }
                    ServerRole::Candidate => {}
                }
                // another round of election is needed
                if Instant::now() - *last_rpc_time.read() > *candidate_timeout {
                    break 'outer;
                }
            },
            // leader should start election
            ServerRole::Leader => {
                role_trigger.listen().await;
            }
        };
    }
}

/// send vote request
async fn send_vote<C: Command + 'static, Conn: ConnectInterface>(
    connect: Arc<Conn>,
    state: Arc<RwLock<State<C>>>,
    req: VoteRequest,
) {
    let rpc_timeout = *state.read().timeout.rpc_timeout();
    let resp = connect.vote(req, *rpc_timeout).await;
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
                debug!("vote is granted by server {}", connect.id());
                let mut state = RwLockUpgradableReadGuard::upgrade(state);
                state.votes_received += 1;

                // the majority has granted the vote
                let min_granted = (state.others.len() + 1) / 2 + 1;
                if state.votes_received >= min_granted {
                    state.set_role(ServerRole::Leader);
                    let self_id = state.id().clone();
                    state.set_leader(self_id);
                    debug!("server {} becomes leader", state.id());

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
async fn bg_leader_calibrates_followers<C: Command + 'static, Conn: ConnectInterface>(
    connects: Vec<Arc<Conn>>,
    state: Arc<RwLock<State<C>>>,
) {
    let calibrate_trigger = Arc::clone(&state.read().calibrate_trigger);
    loop {
        calibrate_trigger.listen().await;
        for connect in connects.clone() {
            let _handle = tokio::spawn(leader_calibrates_follower(connect, Arc::clone(&state)));
        }
    }
}

/// Leader calibrates a follower
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn leader_calibrates_follower<C: Command + 'static, Conn: ConnectInterface>(
    connect: Arc<Conn>,
    state: Arc<RwLock<State<C>>>,
) {
    loop {
        // send append entry
        let (req, last_sent_index, retry_timeout, rpc_timeout) = {
            let state_r = state.read();
            let next_index = state_r.next_index[connect.id()];
            let retry_timeout = *state_r.timeout.retry_timeout();
            let rpc_timeout = *state_r.timeout.rpc_timeout();
            match AppendEntriesRequest::new(
                state_r.term,
                state_r.id().clone(),
                next_index - 1,
                state_r.log[next_index - 1].term(),
                state_r.log[next_index..].to_vec(),
                state_r.commit_index,
            ) {
                Err(e) => {
                    error!("unable to serialize append entries request: {}", e);
                    return;
                }
                Ok(req) => (req, state_r.log.len() - 1, retry_timeout, rpc_timeout),
            }
        };

        let resp = connect.append_entries(req, *rpc_timeout).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {}", e);
                tokio::time::sleep(*retry_timeout).await;
            }
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
                    *state.next_index.get_mut(connect.id()).unwrap() = last_sent_index + 1;
                    *state.match_index.get_mut(connect.id()).unwrap() = last_sent_index;
                    break;
                }

                *state.next_index.get_mut(connect.id()).unwrap() =
                    (resp.commit_index + 1).numeric_cast();
            }
        };
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use parking_lot::{Mutex, RwLock};
    use tokio::{sync::oneshot, time::Instant};
    use tracing_test::traced_test;
    use utils::config::ServerTimeout;

    use super::*;
    use crate::{
        cmd::Command,
        error::ProposeError,
        log::LogEntry,
        rpc::{
            connect::MockConnectInterface, AppendEntriesRequest, AppendEntriesResponse, SyncResult,
            VoteRequest, VoteResponse,
        },
        server::{
            bg_tasks::send_log_until_succeed, cmd_board::CommandBoard,
            cmd_execute_worker::MockCmdExeSenderInterface, state::State, ServerRole,
        },
        test_utils::{sleep_millis, sleep_secs, test_cmd::TestCommand},
    };

    const LEADER_ID: &str = "test-leader";
    const FOLLOWER_ID1: &str = "test-follower-1";
    const FOLLOWER_ID2: &str = "test-follower-2";

    fn new_test_state() -> Arc<RwLock<State<TestCommand>>> {
        let others = HashMap::from([
            (FOLLOWER_ID1.to_owned(), "127.0.0.1:8001".to_owned()),
            (FOLLOWER_ID2.to_owned(), "127.0.0.1:8002".to_owned()),
        ]);
        Arc::new(RwLock::new(State::new(
            LEADER_ID.to_owned(),
            ServerRole::Leader,
            others,
            Arc::new(Mutex::new(CommandBoard::new())),
            Arc::new(RwLock::new(Instant::now())),
            Arc::new(ServerTimeout::default()),
        )))
    }

    /*************** tests for send_log_until_succeed **************/

    #[traced_test]
    #[tokio::test]
    async fn logs_will_be_resent() {
        let state = new_test_state();

        let mut mock_connect = MockConnectInterface::default();
        mock_connect
            .expect_append_entries()
            .times(4)
            .returning(|_, _| Err(ProposeError::RpcStatus("timeout".to_owned())));
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                LEADER_ID.to_owned(),
                0,
                0,
                vec![LogEntry::new(1, &[Arc::new(TestCommand::default())])],
                0,
            )
            .unwrap();
            send_log_until_succeed(1, req, Arc::new(mock_connect), state).await;
        });
        sleep_secs(3).await;
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[traced_test]
    #[tokio::test]
    async fn send_log_will_stop_after_new_election() {
        let state = new_test_state();

        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_append_entries().returning(|_, _| {
            Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                2, 0,
            )))
        });
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        let state_c = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                LEADER_ID.to_owned(),
                0,
                0,
                vec![LogEntry::new(1, &[Arc::new(TestCommand::default())])],
                0,
            )
            .unwrap();
            send_log_until_succeed(1, req, Arc::new(mock_connect), state_c).await;
        });

        assert!(handle.await.is_ok());
        assert_eq!(state.read().term, 2);
    }

    #[traced_test]
    #[tokio::test]
    async fn send_log_will_succeed_and_commit() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state
                .log
                .push(LogEntry::new(0, &[Arc::new(TestCommand::default())]));
            let match_index = state.match_index.get_mut(FOLLOWER_ID1).unwrap();
            *match_index = 0;
            *state.next_index.get_mut(FOLLOWER_ID1).unwrap() = *match_index + 1;
        }

        let mut mock_connect = MockConnectInterface::default();
        mock_connect
            .expect_append_entries()
            .returning(|_, _| Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))));
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());
        let state_c = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            let req = AppendEntriesRequest::new(
                1,
                LEADER_ID.to_owned(),
                0,
                0,
                vec![LogEntry::new(1, &[Arc::new(TestCommand::default())])],
                0,
            )
            .unwrap();
            send_log_until_succeed(1, req, Arc::new(mock_connect), state_c).await;
        });

        assert!(handle.await.is_ok());
        assert_eq!(state.read().term, 0);
        assert_eq!(state.read().commit_index, 1);
        assert_eq!(state.read().match_index[FOLLOWER_ID1], 1);
        assert_eq!(state.read().next_index[FOLLOWER_ID1], 2);
    }

    /*************** tests for send_heartbeat **************/

    #[traced_test]
    #[tokio::test]
    async fn send_heartbeat_will_calibrate_term() {
        let state = new_test_state();

        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_append_entries().returning(|_, _| {
            Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                1, 0,
            )))
        });
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        send_heartbeat(Arc::new(mock_connect), Arc::clone(&state)).await;

        let state = state.read();
        assert_eq!(state.term, 1);
        assert_eq!(state.role(), ServerRole::Follower);
    }

    #[traced_test]
    #[tokio::test]
    async fn send_heartbeat_will_calibrate_next_index() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state
                .log
                .push(LogEntry::new(0, &[Arc::new(TestCommand::default())]));
            *state.next_index.get_mut(FOLLOWER_ID1).unwrap() = 2;
        }

        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_append_entries().returning(|_, _| {
            Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                0, 0,
            )))
        });
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        send_heartbeat(Arc::new(mock_connect), Arc::clone(&state)).await;

        let state = state.read();
        assert_eq!(state.term, 0);
        assert_eq!(state.next_index[FOLLOWER_ID1], 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn no_heartbeat_when_contention_is_high() {
        let state = new_test_state();
        let hb_reset_trigger = state.read().heartbeat_reset_trigger();
        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_append_entries().never();

        let connects = vec![Arc::new(mock_connect)];
        let handle = tokio::spawn(async move {
            bg_heartbeat(connects, state).await;
        });

        for _ in 0..50 {
            sleep_millis(50).await;
            hb_reset_trigger.notify(1);
        }

        handle.abort();
    }

    /*************** tests for bg_apply **************/

    #[traced_test]
    #[tokio::test]
    // this test will apply 2 log: one needs execution, the other doesn't
    async fn leader_will_apply_logs_after_commit() {
        let state = new_test_state();
        let mut spec = SpeculativePool::new();
        let (id1, id2) = {
            let mut state = state.write();
            let cmd_board = state.cmd_board();
            let mut cmd_board = cmd_board.lock();

            let cmd1 = TestCommand::new_get(vec![1]);
            let id1 = cmd1.id().clone();
            spec.push(cmd1.clone());
            cmd_board
                .cmd_states
                .insert(cmd1.id().clone(), CmdState::Execute);
            state.log.push(LogEntry::new(0, &[Arc::new(cmd1)]));

            let cmd2 = TestCommand::default();
            let id2 = cmd2.id().clone();
            spec.push(cmd2.clone());
            cmd_board
                .cmd_states
                .insert(cmd2.id().clone(), CmdState::AfterSync);
            state.log.push(LogEntry::new(0, &[Arc::new(cmd2)]));

            state.commit_index = 2;
            (id1, id2)
        };
        let spec = Arc::new(Mutex::new(spec));

        let mut exe_tx = MockCmdExeSenderInterface::default();
        let id1_c = id1.clone();
        exe_tx
            .expect_send_exe_and_after_sync()
            .return_once(move |cmd: Arc<TestCommand>, index| {
                assert_eq!(cmd.id(), &id1_c);
                assert_eq!(index, 1);
                let (tx, rx) = oneshot::channel();
                tx.send((Ok(vec![]), Some(Ok(1)))).unwrap();
                rx
            });
        let id2_c = id2.clone();
        exe_tx
            .expect_send_after_sync()
            .return_once(move |cmd: Arc<TestCommand>, index| {
                assert_eq!(cmd.id(), &id2_c);
                assert_eq!(index, 2);
                let (tx, rx) = oneshot::channel();
                tx.send(Ok(2)).unwrap();
                rx
            });

        let (state_c, spec_c) = (Arc::clone(&state), Arc::clone(&spec));
        let handle = tokio::spawn(async move {
            bg_apply(state_c, exe_tx, spec_c).await;
        });

        sleep_millis(500).await;

        // check
        let state = state.read();
        let cmd_board = state.cmd_board();
        let mut cmd_board = cmd_board.lock();
        let spec = spec.lock();

        let resp1 = cmd_board.cmd_states.remove(&id1).unwrap();
        let resp1: SyncResult<TestCommand> = match resp1 {
            CmdState::FinalResponse(resp1) => resp1.unwrap().into().unwrap(),
            _ => panic!(),
        };
        match resp1 {
            SyncResult::Success { er, asr } => {
                assert!(er.is_some());
                assert_eq!(asr, 1);
            }
            _ => panic!(),
        }

        let resp2 = cmd_board.cmd_states.remove(&id2).unwrap();
        let resp2: SyncResult<TestCommand> = match resp2 {
            CmdState::FinalResponse(resp2) => resp2.unwrap().into().unwrap(),
            _ => panic!(),
        };
        match resp2 {
            SyncResult::Success { er, asr } => {
                assert!(er.is_none());
                assert_eq!(asr, 2);
            }
            _ => panic!(),
        }

        assert!(spec.pool.is_empty());
        assert!(spec.ready.is_empty());
        assert_eq!(state.last_applied, 2);

        handle.abort();
    }

    /*************** tests for election **************/

    #[traced_test]
    #[tokio::test]
    async fn follower_will_not_start_election_when_heartbeats_are_received() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Follower);
        }
        let last_rpc_time = state.read().last_rpc_time();

        let handle = tokio::spawn(wait_for_election(state));

        for _ in 0..20 {
            sleep_millis(150).await;
            *last_rpc_time.write() = Instant::now();
        }

        assert!(!handle.is_finished());
        handle.abort();
    }

    #[traced_test]
    #[tokio::test]
    async fn follower_will_start_election_when_no_heartbeats() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Follower);
        }

        assert!(
            tokio::time::timeout(Duration::from_secs(3), wait_for_election(state))
                .await
                .is_ok()
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn candidate_will_start_another_round_of_election_if_not_succeed() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Candidate);
        }

        assert!(
            tokio::time::timeout(Duration::from_secs(4), wait_for_election(state))
                .await
                .is_ok()
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn candidate_will_not_start_election_if_it_becomes_leader() {
        let state = new_test_state();
        let last_rpc_time = state.read().last_rpc_time();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Candidate);
        }
        let handle = tokio::spawn(wait_for_election(Arc::clone(&state)));
        let hb_handle = tokio::spawn(async move {
            for _ in 0..20 {
                sleep_millis(150).await;
                *last_rpc_time.write() = Instant::now();
            }
        });

        sleep_secs(1).await;

        {
            let mut state = state.write();
            state.set_role(ServerRole::Leader);
        }

        hb_handle.await.unwrap();
        assert!(!handle.is_finished());
        handle.abort();
    }

    #[traced_test]
    #[tokio::test]
    async fn candidate_will_not_start_election_if_it_becomes_follower() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Candidate);
        }
        let handle = tokio::spawn(wait_for_election(Arc::clone(&state)));

        sleep_secs(1).await;

        {
            let mut state = state.write();
            state.set_role(ServerRole::Follower);
        }

        assert!(!handle.is_finished());
        handle.abort();
    }

    #[traced_test]
    #[tokio::test]
    async fn vote_will_calibrate_term() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.update_to_term(1);
            state.set_role(ServerRole::Candidate);
            state.voted_for = Some(state.id().clone());
            state.votes_received = 1;
        }

        let mut mock_connect = MockConnectInterface::default();
        mock_connect
            .expect_vote()
            .returning(|_, _| Ok(tonic::Response::new(VoteResponse::new_reject(2))));
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        let req = VoteRequest::new(2, FOLLOWER_ID1.to_owned(), 0, 0);
        send_vote(Arc::new(mock_connect), Arc::clone(&state), req).await;

        let state = state.read();
        assert_eq!(state.term, 2);
        assert_eq!(state.role(), ServerRole::Follower);
    }

    #[traced_test]
    #[tokio::test]
    async fn candidate_will_become_leader_after_election_succeeds() {
        let state = new_test_state();
        {
            // start election
            let mut state = state.write();
            state.update_to_term(1);
            state.set_role(ServerRole::Candidate);
            state.voted_for = Some(state.id().clone());
            state.votes_received = 1;
        }

        let mut mock_connect1 = MockConnectInterface::default();
        mock_connect1
            .expect_vote()
            .returning(|_, _| Ok(tonic::Response::new(VoteResponse::new_accept(1))));
        mock_connect1
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        let mut mock_connect2 = MockConnectInterface::default();
        mock_connect2
            .expect_vote()
            .returning(|_, _| Ok(tonic::Response::new(VoteResponse::new_accept(1))));
        mock_connect2
            .expect_id()
            .return_const(FOLLOWER_ID2.to_owned());

        let req = VoteRequest::new(2, FOLLOWER_ID1.to_owned(), 0, 0);

        send_vote(Arc::new(mock_connect1), Arc::clone(&state), req.clone()).await;
        {
            let state = state.read();
            assert_eq!(state.term, 1);
            assert_eq!(state.role(), ServerRole::Leader);
        }

        send_vote(Arc::new(mock_connect2), Arc::clone(&state), req.clone()).await;

        let state = state.read();
        assert_eq!(state.term, 1);
        assert_eq!(state.role(), ServerRole::Leader);
    }

    #[traced_test]
    #[tokio::test]
    async fn leader_will_calibrate_follower_until_succeed() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state
                .log
                .push(LogEntry::new(0, &[Arc::new(TestCommand::default())]));
            state
                .log
                .push(LogEntry::new(0, &[Arc::new(TestCommand::default())]));
            let next_index = state.next_index.get_mut(FOLLOWER_ID1).unwrap();
            *next_index = 3;
        }

        let mut mock_connect = MockConnectInterface::default();
        let mut call_cnt = 0;
        mock_connect.expect_append_entries().returning(move |_, _| {
            call_cnt += 1;
            match call_cnt {
                1 => Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                    0, 0,
                ))),
                _ => Ok(tonic::Response::new(AppendEntriesResponse::new_accept(0))),
            }
        });
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        leader_calibrates_follower(Arc::new(mock_connect), Arc::clone(&state)).await;
        {
            let state = state.read();
            assert_eq!(state.next_index[FOLLOWER_ID1], 3);
            assert_eq!(state.match_index[FOLLOWER_ID1], 2);
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn leader_calibrate_follower_will_calibrate_term() {
        let state = new_test_state();

        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_append_entries().returning(|_, _| {
            Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                1, 0,
            )))
        });
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());

        leader_calibrates_follower(Arc::new(mock_connect), Arc::clone(&state)).await;

        let state = state.read();
        assert_eq!(state.term, 1);
        assert_eq!(state.role(), ServerRole::Follower);
    }
}
