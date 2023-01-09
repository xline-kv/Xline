#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::{
    collections::{HashMap, HashSet},
    iter,
    ops::Range,
    sync::Arc,
    time::Duration,
};

use clippy_utilities::NumericCast;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use itertools::Itertools;
use lock_utils::parking_lot_lock::{MutexMap, RwLockMap};
use madsim::rand::{thread_rng, Rng};
use parking_lot::{
    lock_api::{RwLockUpgradableReadGuard, RwLockWriteGuard},
    RawRwLock, RwLock,
};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tracing::{debug, error, info, warn};

use super::SyncMessage;
use crate::{
    cmd::{Command, CommandExecutor, ProposeId},
    log::LogEntry,
    message::ServerId,
    rpc::{connect::ConnectInterface, AppendEntriesRequest, VoteRequest, VoteResponse},
    server::{
        cmd_execute_worker::{
            execute_worker, CmdExeReceiver, CmdExeSender, CmdExeSenderInterface, N_EXECUTE_WORKERS,
        },
        ServerRole, State,
    },
    shutdown::Shutdown,
};

/// Wait for sometime before next retry
// TODO: make it configurable
const RETRY_TIMEOUT: Duration = Duration::from_millis(800);

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
    let bg_apply_handle = tokio::spawn(bg_apply(Arc::clone(&state), cmd_exe_tx));
    let bg_heartbeat_handle = tokio::spawn(bg_heartbeat(connects.clone(), Arc::clone(&state)));
    let bg_get_sync_cmds_handle =
        tokio::spawn(bg_get_sync_cmds(Arc::clone(&state), sync_chan, ae_trigger));
    let calibrate_handle =
        tokio::spawn(bg_leader_calibrates_followers(connects, Arc::clone(&state)));

    // spawn cmd execute worker
    let bg_exe_worker_handles: Vec<JoinHandle<_>> =
        iter::repeat((cmd_exe_rx, state.read().cmd_board(), Arc::new(cmd_executor)))
            .take(N_EXECUTE_WORKERS)
            .map(|(rx, cmd_board, ce)| tokio::spawn(execute_worker(rx, cmd_board, ce)))
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

/// Interval between sending heartbeats
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
/// Rpc request timeout
const RPC_TIMEOUT: Duration = Duration::from_millis(1000);

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
    // send log[i] until succeed
    loop {
        debug!("append_entries sent to {}", connect.id());
        let resp = connect.append_entries(req.clone(), RPC_TIMEOUT).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {e}");
                // wait for some time until next retry
                tokio::time::sleep(RETRY_TIMEOUT).await;
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
    let (role_trigger, hb_reset_trigger) =
        state.map_read(|state_r| (state_r.role_trigger(), state_r.heartbeat_reset_trigger()));
    #[allow(clippy::integer_arithmetic)] // tokio internal triggered
    loop {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => break,
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
    let req = state.map_read(|state_r| {
        let next_index = state_r.next_index[connect.id()];
        AppendEntriesRequest::new_heartbeat(
            state_r.term,
            state_r.id().clone(),
            next_index - 1,
            state_r.log[next_index - 1].term(),
            state_r.commit_index,
        )
    });

    // send append_entries request and receive response
    debug!("heartbeat sent to {}", connect.id());
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
) {
    let (commit_trigger, spec) =
        state.map_read(|state_r| (state_r.commit_trigger(), state_r.spec()));
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
            state.log[i].cmds().iter().cloned().for_each(|cmd| {
                // clean up spec pool
                spec.lock().mark_ready(cmd.id());
                exe_tx.send_after_sync(cmd, i.numeric_cast());
            });
            state.last_applied = i;
            debug!("log[{i}] committed, last_applied updated to {}", i);
        }
    }
}

/// How long a candidate should wait before it starts another round of election
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(3);
/// How long a follower should wait before it starts a round of election (in millis)
const FOLLOWER_TIMEOUT: Range<u64> = 1000..2000;

/// Recover possibly speculatively executed cmds from spec pools
fn recover_from_spec_pools<C: Command>(
    state_w: &mut RwLockWriteGuard<'_, RawRwLock, State<C>>,
    spec_pools: Vec<Vec<Arc<C>>>,
    superquorum_cnt: usize,
) {
    let mut cmd_cnt: HashMap<ProposeId, (Arc<C>, usize)> = HashMap::new();
    #[allow(clippy::integer_arithmetic)] // should not overflow here
    for cmd in spec_pools.into_iter().flatten() {
        let entry = cmd_cnt.entry(cmd.id().clone()).or_insert((cmd, 0));
        entry.1 += 1;
    }

    // get all possibly executed(fast path) commands
    let existing_log_ids: HashSet<ProposeId> = state_w
        .log
        .iter()
        .flat_map(|log| log.cmds().iter().map(|cmd| cmd.id()).cloned())
        .collect();
    let recovered_cmds = cmd_cnt
        .into_values()
        // only cmds whose cnt >= 3/4 can be recovered
        .filter_map(|(cmd, cnt)| (cnt >= superquorum_cnt).then(|| cmd))
        // dedup in current logs
        .filter(|cmd| {
            // TODO: better dedup mechanism
            !existing_log_ids.contains(cmd.id())
        })
        .collect_vec();

    let cmd_board = state_w.cmd_board();
    let mut board_w = cmd_board.write();
    let term = state_w.term;
    for cmd in recovered_cmds {
        debug!(
            "recover new speculatively executed cmd {:?} in log[{}]",
            cmd.id(),
            state_w.log.len(),
        );

        assert!(
            board_w.needs_exe.insert(cmd.id().clone()),
            "shouldn't insert needs_exe twice"
        );
        state_w.log.push(LogEntry::new(term, &[cmd]));
    }
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

        let rpcs = connects
            .iter()
            .cloned()
            .zip(iter::repeat(req))
            .map(|(connect, request)| async move {
                (
                    connect.id().clone(),
                    connect.vote(request, CANDIDATE_TIMEOUT).await,
                )
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|(id, resp)| async move {
                match resp {
                    Err(e) => {
                        error!("vote failed, {}", e);
                        None
                    }
                    Ok(resp) => Some((id, resp.into_inner())),
                }
            });

        handle_vote_responses(rpcs, Arc::clone(&state)).await;
    }
}

/// Candidate handles vote responses
#[allow(clippy::integer_arithmetic)] // vote cnt won't overflow
async fn handle_vote_responses<C, S>(rpcs: S, state: Arc<RwLock<State<C>>>)
where
    C: Command + 'static,
    S: Stream<Item = (ServerId, VoteResponse)>,
{
    let mut spec_pools: Vec<Vec<Arc<C>>> = vec![];
    pin_mut!(rpcs);
    while let Some((id, resp)) = rpcs.next().await {
        // calibrate term
        let state = state.upgradable_read();
        if resp.term > state.term {
            let mut state = RwLockUpgradableReadGuard::upgrade(state);
            state.update_to_term(resp.term);
            break;
        }

        // is still a candidate
        if !matches!(state.role(), ServerRole::Candidate) {
            break;
        }

        // skip if not granted
        if !resp.vote_granted {
            continue;
        }
        debug!("vote is granted by server {}", id);

        // collect follower spec pool
        let follower_spec_pool = match resp.spec_pool() {
            Err(e) => {
                error!("get vote response spec pool failed, {e}");
                continue;
            }
            Ok(spec_pool) => spec_pool.into_iter().map(|cmd| Arc::new(cmd)).collect(),
        };
        spec_pools.push(follower_spec_pool);

        // update state
        let mut state_w = RwLockUpgradableReadGuard::upgrade(state);
        state_w.votes_received += 1;

        // the majority has granted the vote
        let min_granted = (state_w.others.len() + 1) / 2 + 1;
        if state_w.votes_received >= min_granted {
            // recover possibly speculatively executed cmds
            let self_spec = state_w
                .spec
                .map_lock(|spec_l| spec_l.pool.iter().map(|kv| kv.1).cloned().collect());
            spec_pools.push(self_spec);
            recover_from_spec_pools(&mut state_w, spec_pools, min_granted / 2 + 1);

            // set self to leader
            state_w.set_role(ServerRole::Leader);
            state_w.leader_id = Some(state_w.id().clone());
            debug!("server {} becomes leader", state_w.id());

            // init next_index
            let last_log_index = state_w.last_log_index();
            for index in state_w.next_index.values_mut() {
                *index = last_log_index + 1; // iter from the end to front is more likely to match the follower
            }

            state_w.calibrate_trigger.notify(1); // trigger calibration

            break; // other servers' response no longer matters
        }
    }
}

/// wait until an election is needed
async fn wait_for_election<C: Command + 'static>(state: Arc<RwLock<State<C>>>) {
    let (role_trigger, last_rpc_time) =
        state.map_read(|state_r| (state_r.role_trigger(), state_r.last_rpc_time()));
    'outer: loop {
        // only follower or candidate should try to elect
        let role = state.read().role();
        match role {
            ServerRole::Follower => {
                let timeout = Duration::from_millis(thread_rng().gen_range(FOLLOWER_TIMEOUT));
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
                let next_check = last_rpc_time.read().to_owned() + CANDIDATE_TIMEOUT;
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
                if Instant::now() - *last_rpc_time.read() > CANDIDATE_TIMEOUT {
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
        let (req, last_sent_index) = {
            let state_r = state.read();
            let next_index = state_r.next_index[connect.id()];
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
                Ok(req) => (req, state_r.log.len() - 1),
            }
        };

        let resp = connect.append_entries(req, RPC_TIMEOUT).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {}", e);
                tokio::time::sleep(RETRY_TIMEOUT).await;
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

    use futures::stream;
    use parking_lot::{Mutex, RwLock};
    use tokio::time::Instant;
    use tracing_test::traced_test;

    use super::*;
    use crate::{
        cmd::Command,
        error::ProposeError,
        log::LogEntry,
        rpc::{
            connect::MockConnectInterface, AppendEntriesRequest, AppendEntriesResponse,
            VoteResponse,
        },
        server::{
            bg_tasks::send_log_until_succeed, cmd_board::CommandBoard,
            cmd_execute_worker::MockCmdExeSenderInterface, spec_pool::SpeculativePool,
            state::State, ServerRole,
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
            Arc::new(RwLock::new(CommandBoard::new())),
            Arc::new(Mutex::new(SpeculativePool::new())),
            Arc::new(RwLock::new(Instant::now())),
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
        {
            let mut state = state.write();
            let spec = state.spec();
            let mut spec = spec.lock();
            let cmd_board = state.cmd_board();
            let mut cmd_board = cmd_board.write();

            let cmd1 = Arc::new(TestCommand::new_get(vec![1]));
            spec.insert(Arc::clone(&cmd1));
            cmd_board.needs_exe.insert(cmd1.id().clone());
            state.log.push(LogEntry::new(0, &[cmd1]));

            let cmd2 = Arc::new(TestCommand::default());
            spec.insert(Arc::clone(&cmd2));
            state.log.push(LogEntry::new(0, &[cmd2]));

            state.commit_index = 2;
        }

        let mut exe_tx = MockCmdExeSenderInterface::default();
        exe_tx
            .expect_send_after_sync()
            .times(2)
            .returning(move |_cmd: Arc<TestCommand>, _index| {});

        let state_c = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            bg_apply(state_c, exe_tx).await;
        });

        sleep_millis(500).await;

        assert!(state.read().spec().lock().pool.is_empty());
        assert_eq!(state.read().last_applied, 2);

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

        let rpcs = stream::iter(vec![(
            "NewLeaderID".to_owned(),
            VoteResponse::new_reject(2),
        )]);

        handle_vote_responses(rpcs, Arc::clone(&state)).await;

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

        let rpcs = stream::iter(vec![
            (
                FOLLOWER_ID1.to_owned(),
                VoteResponse::new_accept::<TestCommand>(1, vec![]).unwrap(),
            ),
            (
                FOLLOWER_ID2.to_owned(),
                VoteResponse::new_accept::<TestCommand>(1, vec![]).unwrap(),
            ),
        ]);

        handle_vote_responses(rpcs, Arc::clone(&state)).await;

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

    #[traced_test]
    #[test]
    fn recover_from_spec_pools_will_pick_the_correct_cmds() {
        let others = HashMap::from([
            ("S1".to_owned(), "127.0.0.1:8001".to_owned()),
            ("S2".to_owned(), "127.0.0.1:8002".to_owned()),
            ("S3".to_owned(), "127.0.0.1:8002".to_owned()),
            ("S4".to_owned(), "127.0.0.1:8002".to_owned()),
        ]);
        let state = Arc::new(RwLock::new(State::new(
            LEADER_ID.to_owned(),
            ServerRole::Leader,
            others,
            Arc::new(RwLock::new(CommandBoard::new())),
            Arc::new(Mutex::new(SpeculativePool::new())),
            Arc::new(RwLock::new(Instant::now())),
        )));
        // cmd1 has already been committed
        let cmd1 = Arc::new(TestCommand::new_put(vec![1], 1));
        // cmd2 has been speculatively successfully but not committed yet
        let cmd2 = Arc::new(TestCommand::new_put(vec![2], 1));
        // cmd3 has been speculatively successfully by the leader but not stored by the superquorum of the followers
        let cmd3 = Arc::new(TestCommand::new_put(vec![3], 1));
        {
            let mut state = state.write();
            state.log.push(LogEntry::new(0, &[Arc::clone(&cmd1)]));
        }

        let spec_pools = vec![
            vec![Arc::clone(&cmd2), Arc::clone(&cmd3)],
            vec![Arc::clone(&cmd2), Arc::clone(&cmd3)],
            vec![Arc::clone(&cmd2), Arc::clone(&cmd3)],
            vec![Arc::clone(&cmd2)],
            vec![],
        ];

        let mut state_w = state.write();
        recover_from_spec_pools(&mut state_w, spec_pools, 4);

        assert_eq!(state_w.log[1].cmds()[0].id(), cmd1.id());
        assert_eq!(state_w.log[2].cmds()[0].id(), cmd2.id());
        assert_eq!(state_w.log.len(), 3);
    }
}
