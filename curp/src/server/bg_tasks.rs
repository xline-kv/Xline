use std::{
    collections::{HashMap, HashSet},
    iter,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use clippy_utilities::NumericCast;
use event_listener::Event;
use futures::{future::Either, pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use itertools::Itertools;
use madsim::rand::{thread_rng, Rng};
use parking_lot::{
    lock_api::{RwLockUpgradableReadGuard, RwLockWriteGuard},
    RawRwLock,
};
use tokio::{sync::mpsc, task::JoinHandle, time::MissedTickBehavior};
use tracing::{debug, error, info, warn};
use utils::{
    config::ServerTimeout,
    parking_lot_lock::{MutexMap, RwLockMap},
};

use super::{cmd_worker::CmdAsReceiver, state::StateRef, SyncMessage};
use crate::{
    cmd::{Command, CommandExecutor, ProposeId},
    log::LogEntry,
    rpc::{
        self, connect::ConnectInterface, AppendEntriesRequest, AppendEntriesResponse, VoteRequest,
        VoteResponse,
    },
    server::{
        cmd_worker::{
            after_sync_worker, execute_worker, CmdExeReceiver, CmdExeSenderInterface,
            N_AFTER_SYNC_WORKERS, N_EXECUTE_WORKERS,
        },
        ServerRole, State,
    },
    TxFilter,
};

/// Run background tasks
#[allow(clippy::too_many_arguments)] // we call this function once, it's ok
pub(super) async fn run_bg_tasks<
    C: Command + 'static,
    CE: 'static + CommandExecutor<C>,
    Conn: ConnectInterface,
    ExeTx: CmdExeSenderInterface<C>,
>(
    state: StateRef<C, ExeTx>,
    sync_rx: flume::Receiver<SyncMessage<C>>,
    cmd_executor: CE,
    cmd_exe_tx: ExeTx,
    cmd_exe_rx: CmdExeReceiver<C>,
    cmd_as_rx: CmdAsReceiver<C>,
    shutdown_trigger: Arc<Event>,
    timeout: Arc<ServerTimeout>,
    tx_filter: Option<Box<dyn TxFilter>>,
) {
    // establish connection with other servers
    let (others, spec, cmd_board) =
        state.map_read(|state_r| (state_r.others.clone(), state_r.spec(), state_r.cmd_board()));

    let connects = rpc::connect(others, tx_filter).await;

    // notify when a broadcast of append_entries is needed immediately
    let (ae_tx, ae_rx) = mpsc::unbounded_channel::<usize>();

    let bg_tick_handle = tokio::spawn(bg_tick(
        connects.clone(),
        Arc::clone(&state),
        Arc::clone(&timeout),
    ));
    let bg_ae_handle = tokio::spawn(bg_append_entries(
        connects.clone(),
        Arc::clone(&state),
        ae_rx,
        Arc::clone(&timeout),
    ));
    let bg_apply_handle = tokio::spawn(bg_apply(Arc::clone(&state), cmd_exe_tx));
    let bg_get_sync_cmds_handle =
        tokio::spawn(bg_get_sync_cmds(Arc::clone(&state), sync_rx, ae_tx));

    // spawn cmd execute worker
    let cmd_executor = Arc::new(cmd_executor);
    let bg_exe_worker_handles: Vec<JoinHandle<_>> = iter::repeat((
        cmd_exe_rx,
        Arc::clone(&spec),
        Arc::clone(&cmd_board),
        Arc::clone(&cmd_executor),
    ))
    .take(N_EXECUTE_WORKERS)
    .map(|(rx, spec_c, cmd_board_c, ce)| tokio::spawn(execute_worker(rx, cmd_board_c, spec_c, ce)))
    .collect();
    let bg_as_worker_handles: Vec<JoinHandle<_>> = iter::repeat((
        cmd_as_rx,
        Arc::clone(&spec),
        Arc::clone(&cmd_board),
        cmd_executor,
    ))
    .take(N_AFTER_SYNC_WORKERS)
    .map(|(rx, spec_c, cmd_board_c, ce)| {
        tokio::spawn(after_sync_worker(rx, cmd_board_c, spec_c, ce))
    })
    .collect();

    shutdown_trigger.listen().await;
    bg_tick_handle.abort();
    bg_ae_handle.abort();
    bg_apply_handle.abort();
    bg_get_sync_cmds_handle.abort();
    for handle in bg_exe_worker_handles {
        handle.abort();
    }
    for handle in bg_as_worker_handles {
        handle.abort();
    }
    info!("all background task stopped");
}

/// Do periodical task
async fn bg_tick<C: Command + 'static, Conn: ConnectInterface, ExeTx: CmdExeSenderInterface<C>>(
    connects: Vec<Arc<Conn>>,
    state: StateRef<C, ExeTx>,
    timeout: Arc<ServerTimeout>,
) {
    let (
        rpc_timeout,
        heartbeat_interval,
        follower_timeout_ticks_base,
        candidate_timeout_ticks_base,
    ) = (
        *timeout.rpc_timeout(),
        *timeout.heartbeat_interval(),
        *timeout.follower_timeout_ticks(),
        *timeout.candidate_timeout_ticks(),
    );
    // randomize to minimize vote split possibility
    let mut follower_timeout_ticks =
        thread_rng().gen_range(follower_timeout_ticks_base..(2 * follower_timeout_ticks_base));
    let mut candidate_timeout_ticks =
        thread_rng().gen_range(candidate_timeout_ticks_base..(2 * candidate_timeout_ticks_base));

    // wait for some random time before tick starts to minimize vote split possibility
    let rand = thread_rng()
        .gen_range(0..heartbeat_interval.as_millis())
        .numeric_cast();
    tokio::time::sleep(Duration::from_millis(rand)).await;

    let mut ticker = tokio::time::interval(heartbeat_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let _now = ticker.tick().await;
        let task = {
            let state_c = Arc::clone(&state);
            let state_r = state.upgradable_read();
            if state_r.is_leader() {
                if state_r
                    .hb_opt
                    .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
                {
                    let resps = bcast_heartbeats(connects.clone(), state_r, rpc_timeout);
                    Either::Left(handle_heartbeat_responses(
                        resps,
                        state_c,
                        Arc::clone(&timeout),
                    ))
                } else {
                    continue;
                }
            } else {
                let tick = state_r.election_tick.fetch_add(1, Ordering::AcqRel);
                if tick
                    < (if state_r.role() == ServerRole::Follower {
                        follower_timeout_ticks
                    } else {
                        candidate_timeout_ticks
                    })
                {
                    continue;
                }
                // re-generate timeout to avoid split vote forever
                follower_timeout_ticks = thread_rng()
                    .gen_range(follower_timeout_ticks_base..(2 * follower_timeout_ticks_base));
                candidate_timeout_ticks = thread_rng()
                    .gen_range(candidate_timeout_ticks_base..(2 * candidate_timeout_ticks_base));
                state_r.reset_election_tick(); // reset tick

                let resps = bcast_votes(connects.clone(), state_r, rpc_timeout);
                Either::Right(handle_vote_responses(resps, state_c))
            }
        };
        task.await;
    }
}

/// Fetch commands need to be synced and add them to the log
async fn bg_get_sync_cmds<C: Command + 'static, ExeTx: CmdExeSenderInterface<C>>(
    state: StateRef<C, ExeTx>,
    sync_rx: flume::Receiver<SyncMessage<C>>,
    ae_tx: mpsc::UnboundedSender<usize>,
) {
    loop {
        let (term, cmd) = match sync_rx.recv_async().await {
            Ok(msg) => msg.inner(),
            Err(_) => {
                return;
            }
        };

        state.map_write(|mut state_w| {
            state_w.log.push(LogEntry::new(term, &[cmd]));
            if let Err(e) = ae_tx.send(state_w.last_log_index()) {
                error!("ae_tx failed: {}", e);
            }

            debug!(
                "received new log, index {:?}",
                state_w.log.len().checked_sub(1),
            );
        });
    }
}

/// Background `append_entries`, only works for the leader
async fn bg_append_entries<
    C: Command + 'static,
    Conn: ConnectInterface,
    ExeTx: CmdExeSenderInterface<C>,
>(
    connects: Vec<Arc<Conn>>,
    state: StateRef<C, ExeTx>,
    mut ae_rx: mpsc::UnboundedReceiver<usize>,
    timeout: Arc<ServerTimeout>,
) {
    while let Some(i) = ae_rx.recv().await {
        let req = {
            let state_r = state.read();
            if !state_r.is_leader() {
                warn!("Non leader receives sync log[{i}] request");
                continue;
            }

            debug!("{} sends append_entries to followers", state_r.id());

            // log.len() >= 1 because we have a fake log[0]
            #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
            match AppendEntriesRequest::new(
                state_r.term,
                state_r.id().clone(),
                i - 1,
                state_r.log[i - 1].term(),
                vec![state_r.log[i].clone()],
                state_r.commit_index,
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
                Arc::clone(&timeout),
            ));
        }
    }
}

/// Send `append_entries` containing a single log to a server
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn send_log_until_succeed<
    C: Command + 'static,
    Conn: ConnectInterface,
    ExeTx: CmdExeSenderInterface<C>,
>(
    i: usize,
    req: AppendEntriesRequest,
    connect: Arc<Conn>,
    state: StateRef<C, ExeTx>,
    timeout: Arc<ServerTimeout>,
) {
    // send log[i] until succeed
    loop {
        let resp = connect
            .append_entries(req.clone(), *timeout.rpc_timeout())
            .await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {e}");
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

                // no longer leader
                if !state.is_leader() {
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
        // wait for some time until next retry
        tokio::time::sleep(*timeout.retry_timeout()).await;
    }
}

/// Leader broadcast heartbeats
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
fn bcast_heartbeats<
    C: Command + 'static,
    Conn: ConnectInterface,
    ExeTx: CmdExeSenderInterface<C>,
>(
    connects: Vec<Arc<Conn>>,
    state_r: RwLockUpgradableReadGuard<'_, RawRwLock, State<C, ExeTx>>,
    rpc_timeout: Duration,
) -> impl Stream<Item = (Arc<Conn>, AppendEntriesResponse)> {
    let resps = connects
        .into_iter()
        .map(|connect| {
            let req = {
                let next_index = state_r.next_index[connect.id()];
                AppendEntriesRequest::new_heartbeat(
                    state_r.term,
                    state_r.id().clone(),
                    next_index - 1,
                    state_r.log[next_index - 1].term(),
                    state_r.commit_index,
                )
            };
            debug!("{} send heartbeat to {}", state_r.id(), connect.id());
            async move {
                let resp = connect.append_entries(req, rpc_timeout).await;
                (connect, resp)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|(connect, resp)| async move {
            match resp {
                Err(e) => {
                    error!("{}'s heartbeat failed, {}", connect.id(), e);
                    None
                }
                Ok(resp) => Some((connect, resp.into_inner())),
            }
        });
    drop(state_r);
    resps
}

/// Candidate broadcasts vote
fn bcast_votes<C: Command + 'static, Conn: ConnectInterface, ExeTx: CmdExeSenderInterface<C>>(
    connects: Vec<Arc<Conn>>,
    state_r: RwLockUpgradableReadGuard<'_, RawRwLock, State<C, ExeTx>>,
    rpc_timeout: Duration,
) -> impl Stream<Item = (Arc<Conn>, VoteResponse)> {
    #[allow(clippy::integer_arithmetic)] // TODO: handle possible overflow
    let req = {
        let mut state_w = RwLockUpgradableReadGuard::upgrade(state_r);
        let new_term = state_w.term + 1;
        state_w.update_to_term(new_term);
        state_w.set_role(ServerRole::Candidate);
        state_w.voted_for = Some(state_w.id().clone());
        state_w.votes_received = 1;
        VoteRequest::new(
            state_w.term,
            state_w.id().clone(),
            state_w.last_log_index(),
            state_w.last_log_term(),
        )
    };
    debug!("server {} starts election", req.candidate_id);
    connects
        .into_iter()
        .zip(iter::repeat(req))
        .map(|(connect, request)| async move {
            let resp = connect.vote(request, rpc_timeout).await;
            (connect, resp)
        })
        .collect::<FuturesUnordered<_>>()
        .filter_map(|(id, resp)| async move {
            match resp {
                Err(e) => {
                    warn!("vote failed, {}", e);
                    None
                }
                Ok(resp) => Some((id, resp.into_inner())),
            }
        })
}

/// Handle heartbeat responses
#[allow(
    clippy::integer_arithmetic,
    clippy::indexing_slicing,
    clippy::unwrap_used
)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn handle_heartbeat_responses<C, S, Conn, ExeTx>(
    rpcs: S,
    state: StateRef<C, ExeTx>,
    timeout: Arc<ServerTimeout>,
) where
    C: Command + 'static,
    S: Stream<Item = (Arc<Conn>, AppendEntriesResponse)>,
    Conn: ConnectInterface,
    ExeTx: CmdExeSenderInterface<C>,
{
    pin_mut!(rpcs);
    while let Some((connect, resp)) = rpcs.next().await {
        let state_r = state.upgradable_read();
        let id = connect.id();
        if resp.term > state_r.term {
            let mut state_w = RwLockUpgradableReadGuard::upgrade(state_r);
            state_w.update_to_term(resp.term);
            return;
        }

        if !resp.success {
            let mut state_w = RwLockUpgradableReadGuard::upgrade(state_r);
            *state_w.next_index.get_mut(id).unwrap() = (resp.commit_index + 1).numeric_cast();
            // spawn a task to calibrate the follower
            // why only calibrate to current state.last_log_index(): new log will be sent in send_log_until_succeed
            debug!("leader {} starts calibrating follower {}", state_w.id(), id);
            let _ig = tokio::spawn(leader_calibrates_follower(
                connect,
                Arc::clone(&state),
                state_w.last_log_index(),
                Arc::clone(&timeout),
            ));
        }
    }
}

/// Background apply
async fn bg_apply<C: Command + 'static, ExeTx: CmdExeSenderInterface<C>>(
    state: StateRef<C, ExeTx>,
    exe_tx: ExeTx,
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
        let mut board_w = cmd_board.write();
        #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
        // TODO: overflow of log index should be prevented
        for i in (state.last_applied + 1)..=state.commit_index {
            state.log[i].cmds().iter().cloned().for_each(|cmd| {
                if !state.is_leader() {
                    assert!(
                        board_w.needs_exe.insert(cmd.id().clone()),
                        "shouldn't insert needs_exe twice"
                    );
                }
                exe_tx.send_after_sync(cmd, i.numeric_cast());
            });
            state.last_applied = i;
            debug!(
                "{} committed log[{i}], last_applied updated to {}",
                state.id(),
                i
            );
        }
    }
}

/// Recover possibly speculatively executed cmds from spec pools
fn recover_from_spec_pools<C: Command, ExeTx: CmdExeSenderInterface<C>>(
    state_w: &mut RwLockWriteGuard<'_, RawRwLock, State<C, ExeTx>>,
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
    let spec = state_w.spec();
    let mut spec_l = spec.lock();

    let term = state_w.term;
    for cmd in recovered_cmds {
        debug!(
            "{} recovers speculatively executed cmd {:?} in log[{}]",
            state_w.id(),
            cmd.id(),
            state_w.log.len(),
        );
        let _ig_sync = board_w.sync.insert(cmd.id().clone()); // may have been inserted before
        let _ig_spec = spec_l.insert(Arc::clone(&cmd)); // may have been inserted before
        let _ig_needs_exe = board_w.needs_exe.insert(cmd.id().clone()); // may have been inserted before
        state_w.log.push(LogEntry::new(term, &[cmd]));
    }
}

/// Candidate handles vote responses
#[allow(clippy::integer_arithmetic)] // vote cnt won't overflow
async fn handle_vote_responses<C, S, ExeTx, Conn>(rpcs: S, state: StateRef<C, ExeTx>)
where
    C: Command + 'static,
    S: Stream<Item = (Arc<Conn>, VoteResponse)>,
    ExeTx: CmdExeSenderInterface<C>,
    Conn: ConnectInterface,
{
    let mut spec_pools: Vec<Vec<Arc<C>>> = vec![];
    pin_mut!(rpcs);
    while let Some((conn, resp)) = rpcs.next().await {
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

        // skip if not granted
        if !resp.vote_granted {
            continue;
        }
        debug!("vote is granted by server {}", conn.id());

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

            return; // other servers' response no longer matters
        }
    }
}

/// Leader calibrates a follower
#[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
async fn leader_calibrates_follower<
    C: Command + 'static,
    Conn: ConnectInterface,
    ExeTx: CmdExeSenderInterface<C>,
>(
    connect: Arc<Conn>,
    state: StateRef<C, ExeTx>,
    last_index: usize,
    timeout: Arc<ServerTimeout>,
) {
    loop {
        // send append entry
        let req = {
            let state_r = state.read();
            if !state_r.is_leader() {
                return;
            }
            let next_index = state_r.next_index[connect.id()];
            match AppendEntriesRequest::new(
                state_r.term,
                state_r.id().clone(),
                next_index - 1,
                state_r.log[next_index - 1].term(),
                state_r.log[next_index..=last_index].to_vec(),
                state_r.commit_index,
            ) {
                Err(e) => {
                    error!("unable to serialize append entries request: {}", e);
                    return;
                }
                Ok(req) => req,
            }
        };

        let resp = connect.append_entries(req, *timeout.rpc_timeout()).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => {
                warn!("append_entries error: {}", e);
                tokio::time::sleep(*timeout.retry_timeout()).await;
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
                    *state.next_index.get_mut(connect.id()).unwrap() = last_index + 1;
                    *state.match_index.get_mut(connect.id()).unwrap() = last_index;
                    debug!("successfully calibrates {}", connect.id());
                    let min_replicated = (state.others.len() + 1) / 2;
                    if state.commit_index < last_index
                        && state.log[last_index].term() == state.term
                        && state
                            .others
                            .iter()
                            .filter(|&(id, _)| state.match_index[id] >= last_index)
                            .count()
                            >= min_replicated
                    {
                        state.commit_index = last_index;
                        debug!("commit_index updated to {last_index}");
                        state.commit_trigger.notify(1);
                    }
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
    use madsim::time::sleep;
    use parking_lot::RwLock;
    use tracing_test::traced_test;
    use utils::config::{
        default_candidate_timeout_ticks, default_follower_timeout_ticks,
        default_heartbeat_interval, default_retry_timeout, ServerTimeout,
    };

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
            bg_tasks::send_log_until_succeed,
            cmd_worker::{cmd_exe_channel, MockCmdExeSenderInterface},
            state::State,
            ServerRole,
        },
        test_utils::{sleep_millis, test_cmd::TestCommand},
    };

    const LEADER_ID: &str = "test-leader";
    const FOLLOWER_ID1: &str = "test-follower-1";
    const FOLLOWER_ID2: &str = "test-follower-2";

    fn new_test_state() -> Arc<RwLock<State<TestCommand, MockCmdExeSenderInterface<TestCommand>>>> {
        let others = HashMap::from([
            (FOLLOWER_ID1.to_owned(), "127.0.0.1:8001".to_owned()),
            (FOLLOWER_ID2.to_owned(), "127.0.0.1:8002".to_owned()),
        ]);
        let mut exe_tx = MockCmdExeSenderInterface::default();
        exe_tx.expect_send_reset().returning(|| ());
        Arc::new(RwLock::new(State::new(
            LEADER_ID.to_owned(),
            ServerRole::Leader,
            others,
            exe_tx,
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
            .times(3..)
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
            send_log_until_succeed(
                1,
                req,
                Arc::new(mock_connect),
                state,
                Arc::new(ServerTimeout::default()),
            )
            .await;
        });
        sleep_millis(default_retry_timeout().as_millis() as u64 * 4).await;
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
            send_log_until_succeed(
                1,
                req,
                Arc::new(mock_connect),
                state_c,
                Arc::new(ServerTimeout::default()),
            )
            .await;
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
            send_log_until_succeed(
                1,
                req,
                Arc::new(mock_connect),
                state_c,
                Arc::new(ServerTimeout::default()),
            )
            .await;
        });

        assert!(handle.await.is_ok());
        assert_eq!(state.read().term, 0);
        assert_eq!(state.read().commit_index, 1);
        assert_eq!(state.read().match_index[FOLLOWER_ID1], 1);
        assert_eq!(state.read().next_index[FOLLOWER_ID1], 2);
    }

    /*************** tests for heartbeat **************/

    #[traced_test]
    #[tokio::test]
    async fn heartbeat_will_calibrate_term() {
        let state = new_test_state();

        let mut mock_connect = MockConnectInterface::default();
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());
        let rpcs = stream::iter(vec![(
            Arc::new(mock_connect),
            AppendEntriesResponse::new_reject(1, 0),
        )]);

        handle_heartbeat_responses(rpcs, Arc::clone(&state), Arc::new(ServerTimeout::default()))
            .await;

        let state = state.read();
        assert_eq!(state.term, 1);
        assert_eq!(state.role(), ServerRole::Follower);
    }

    #[traced_test]
    #[tokio::test]
    async fn heartbeat_will_calibrate_next_index() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state
                .log
                .push(LogEntry::new(0, &[Arc::new(TestCommand::default())]));
            *state.next_index.get_mut(FOLLOWER_ID1).unwrap() = 2;
        }

        let mut mock_connect = MockConnectInterface::default();
        mock_connect
            .expect_id()
            .return_const(FOLLOWER_ID1.to_owned());
        let rpcs = stream::iter(vec![(
            Arc::new(mock_connect),
            AppendEntriesResponse::new_reject(0, 0),
        )]);

        handle_heartbeat_responses(rpcs, Arc::clone(&state), Arc::new(ServerTimeout::default()))
            .await;

        let state = state.read();
        assert_eq!(state.term, 0);
        assert_eq!(state.next_index[FOLLOWER_ID1], 1);
    }

    #[traced_test]
    #[tokio::test]
    async fn no_heartbeat_when_contention_is_high() {
        let state = new_test_state();
        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_append_entries().never();

        let connects = vec![Arc::new(mock_connect)];
        let handle = tokio::spawn(bg_tick(
            connects,
            Arc::clone(&state),
            Arc::new(ServerTimeout::default()),
        ));

        for _ in 0..50 {
            sleep_millis(50).await;
            state.read().hb_opt.store(true, Ordering::Relaxed);
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

        let handle = tokio::spawn(bg_tick::<_, MockConnectInterface, _>(
            vec![],
            Arc::clone(&state),
            Arc::new(ServerTimeout::default()),
        ));

        for _ in 0..20 {
            sleep(default_heartbeat_interval()).await;
            state.read().election_tick.store(0, Ordering::Relaxed);
        }

        assert!(!handle.is_finished());
        handle.abort();
    }

    fn election_test_mock_connect() -> MockConnectInterface {
        let mut mock_connect = MockConnectInterface::default();
        mock_connect.expect_vote().times(3..).returning(|_, _| {
            Ok(tonic::Response::new(
                VoteResponse::new_accept::<TestCommand>(1, vec![]).unwrap(),
            ))
        });
        mock_connect
            .expect_append_entries()
            .returning(|_, _| Ok(tonic::Response::new(AppendEntriesResponse::new_accept(1))));
        mock_connect
    }

    #[traced_test]
    #[tokio::test]
    async fn follower_will_start_election_when_no_heartbeats() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Follower);
        }

        let mut mock_connect1 = election_test_mock_connect();
        mock_connect1
            .expect_id()
            .return_const(FOLLOWER_ID1.to_string());
        let mut mock_connect2 = election_test_mock_connect();
        mock_connect2
            .expect_id()
            .return_const(FOLLOWER_ID2.to_string());

        let handle = tokio::spawn(bg_tick::<_, MockConnectInterface, _>(
            vec![Arc::new(mock_connect1), Arc::new(mock_connect2)],
            Arc::clone(&state),
            Arc::new(ServerTimeout::default()),
        ));

        sleep_millis(
            (default_follower_timeout_ticks() as u64
                * 2
                * default_heartbeat_interval().as_millis() as u64)
                + 50,
        )
        .await;
        assert!(!handle.is_finished());
    }

    #[traced_test]
    #[tokio::test]
    async fn candidate_will_start_another_round_of_election_if_not_succeed() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Candidate);
        }

        let mut mock_connect1 = election_test_mock_connect();
        mock_connect1
            .expect_id()
            .return_const(FOLLOWER_ID1.to_string());
        let mut mock_connect2 = election_test_mock_connect();
        mock_connect2
            .expect_id()
            .return_const(FOLLOWER_ID2.to_string());

        let handle = tokio::spawn(bg_tick::<_, MockConnectInterface, _>(
            vec![Arc::new(mock_connect1), Arc::new(mock_connect2)],
            Arc::clone(&state),
            Arc::new(ServerTimeout::default()),
        ));

        sleep_millis(
            (default_follower_timeout_ticks() as u64
                * default_heartbeat_interval().as_millis() as u64
                * 2)
                + 50,
        )
        .await;
        assert!(!handle.is_finished());
    }

    #[traced_test]
    #[tokio::test]
    async fn candidate_will_not_start_election_if_it_becomes_leader() {
        let state = new_test_state();
        {
            let mut state = state.write();
            state.set_role(ServerRole::Candidate);
        }

        let mut mock_connect1 = MockConnectInterface::default();
        mock_connect1.expect_vote().never();
        let mut mock_connect2 = MockConnectInterface::default();
        mock_connect2.expect_vote().never();
        let handle = tokio::spawn(bg_tick::<_, MockConnectInterface, _>(
            vec![Arc::new(mock_connect1), Arc::new(mock_connect2)],
            Arc::clone(&state),
            Arc::new(ServerTimeout::default()),
        ));

        sleep_millis(
            (default_candidate_timeout_ticks() - 1) as u64
                * default_heartbeat_interval().as_millis() as u64,
        )
        .await;

        {
            let mut state = state.write();
            state.set_role(ServerRole::Leader);
        }

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

        let mut mock_connect1 = MockConnectInterface::default();
        mock_connect1.expect_vote().never();
        let mut mock_connect2 = MockConnectInterface::default();
        mock_connect2.expect_vote().never();

        let handle = tokio::spawn(bg_tick::<_, MockConnectInterface, _>(
            vec![Arc::new(mock_connect1), Arc::new(mock_connect2)],
            Arc::clone(&state),
            Arc::new(ServerTimeout::default()),
        ));

        sleep_millis(
            (default_candidate_timeout_ticks() - 1) as u64
                * default_heartbeat_interval().as_millis() as u64,
        )
        .await;

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
            Arc::new(MockConnectInterface::default()),
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

        let mut mock_connect1 = MockConnectInterface::default();
        mock_connect1
            .expect_id()
            .return_const(FOLLOWER_ID1.to_string());
        let mut mock_connect2 = MockConnectInterface::default();
        mock_connect2
            .expect_id()
            .return_const(FOLLOWER_ID2.to_string());
        let rpcs = stream::iter(vec![
            (
                Arc::new(mock_connect1),
                VoteResponse::new_accept::<TestCommand>(1, vec![]).unwrap(),
            ),
            (
                Arc::new(mock_connect2),
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

        leader_calibrates_follower(
            Arc::new(mock_connect),
            Arc::clone(&state),
            2,
            Arc::new(ServerTimeout::default()),
        )
        .await;
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

        leader_calibrates_follower(
            Arc::new(mock_connect),
            Arc::clone(&state),
            0,
            Arc::new(ServerTimeout::default()),
        )
        .await;

        let state = state.read();
        assert_eq!(state.term, 1);
        assert_eq!(state.role(), ServerRole::Follower);
    }

    #[traced_test]
    #[tokio::test]
    async fn recover_from_spec_pools_will_pick_the_correct_cmds() {
        let others = HashMap::from([
            ("S1".to_owned(), "127.0.0.1:8001".to_owned()),
            ("S2".to_owned(), "127.0.0.1:8002".to_owned()),
            ("S3".to_owned(), "127.0.0.1:8002".to_owned()),
            ("S4".to_owned(), "127.0.0.1:8002".to_owned()),
        ]);
        let (exe_tx, _exe_rx, _) = cmd_exe_channel();
        let state = Arc::new(RwLock::new(State::new(
            LEADER_ID.to_owned(),
            ServerRole::Leader,
            others,
            exe_tx,
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
