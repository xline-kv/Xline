use std::{iter, ops::Range, sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use futures::channel::oneshot;
use madsim::rand::{thread_rng, Rng};
use parking_lot::{lock_api::RwLockUpgradableReadGuard, Mutex, RwLock};
use tokio::{
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    time::Instant,
};
use tracing::{debug, error, info, warn};

use crate::{
    channel::{
        key_mpsc::MpscKeyBasedReceiver,
        key_spmc::{self, SpmcKeyBasedReceiver, SpmcKeyBasedSender},
        RecvError,
    },
    cmd::{Command, CommandExecutor},
    error::ExecuteError,
    log::LogEntry,
    message::TermNum,
    rpc::{self, AppendEntriesRequest, Connect, VoteRequest},
    server::{ServerRole, SpeculativePool, State},
    shutdown::Shutdown,
    util::RwLockMap,
    LogIndex,
};

/// Run background tasks
#[allow(clippy::too_many_arguments)] // we can this function once, it's ok
pub(crate) async fn run_bg_tasks<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    state: Arc<RwLock<State<C>>>,
    last_rpc_time: Arc<RwLock<Instant>>,
    sync_chan: MpscKeyBasedReceiver<C::K, SyncMessage<C>>,
    comp_chan: SpmcKeyBasedSender<C::K, SyncCompleteMessage<C>>,
    cmd_executor: Arc<CE>,
    spec: Arc<Mutex<SpeculativePool<C>>>,
    cmd_exe_tx: CmdExecuteSender<C>,
    cmd_exe_rx: UnboundedReceiver<ExecuteMessage<C>>,
    mut shutdown: Shutdown,
) {
    let others = state.read().others.clone();
    // establish connection with other servers
    let connects = rpc::try_connect(others.into_iter().collect()).await;

    // notify when a broadcast of append_entries is needed immediately
    let (ae_trigger, ae_trigger_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();

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
    let bg_apply_handle = tokio::spawn(bg_apply(
        Arc::clone(&state),
        cmd_exe_tx,
        Arc::clone(&cmd_executor),
        comp_chan,
        spec,
    ));
    let bg_heartbeat_handle = tokio::spawn(bg_heartbeat(connects.clone(), Arc::clone(&state)));
    let bg_get_sync_cmds_handle =
        tokio::spawn(bg_get_sync_cmds(Arc::clone(&state), sync_chan, ae_trigger));
    let calibrate_handle = tokio::spawn(leader_calibrates_followers(connects, state));
    let bg_cmd_exe_handle = tokio::spawn(bg_execute_cmd(cmd_executor, cmd_exe_rx));

    shutdown.recv().await;
    bg_ae_handle.abort();
    bg_election_handle.abort();
    bg_apply_handle.abort();
    bg_heartbeat_handle.abort();
    bg_get_sync_cmds_handle.abort();
    calibrate_handle.abort();
    bg_cmd_exe_handle.abort();
    info!("all background task stopped");
}

/// "sync task complete" message
pub(crate) struct SyncCompleteMessage<C>
where
    C: Command,
{
    /// The log index
    log_index: LogIndex,
    /// Command
    cmd: Arc<C>,
}

impl<C> SyncCompleteMessage<C>
where
    C: Command,
{
    /// Create a new `SyncCompleteMessage`
    fn new(log_index: LogIndex, cmd: Arc<C>) -> Self {
        Self { log_index, cmd }
    }

    /// Get Log Index
    pub(crate) fn log_index(&self) -> u64 {
        self.log_index
    }

    /// Get commands
    pub(crate) fn cmd(&self) -> Arc<C> {
        Arc::clone(&self.cmd)
    }
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
    fn inner(&mut self) -> (TermNum, Arc<C>) {
        (self.term, Arc::clone(&self.cmd))
    }
}

/// Fetch commands need to be synced and add them to the log
async fn bg_get_sync_cmds<C: Command + 'static>(
    state: Arc<RwLock<State<C>>>,
    mut sync_chan: MpscKeyBasedReceiver<<C as Command>::K, SyncMessage<C>>,
    ae_trigger: UnboundedSender<usize>,
) {
    loop {
        let (cmds, term) = match fetch_sync_msgs(&mut sync_chan).await {
            Ok((cmds, term)) => (cmds, term),
            Err(_) => return,
        };

        #[allow(clippy::shadow_unrelated)] // clippy false positive
        state.map_write(|mut state| {
            state.log.push(LogEntry::new(term, &cmds));
            if let Err(e) = ae_trigger.send(state.last_log_index()) {
                error!("ae_trigger failed: {}", e);
            }

            debug!(
                "received new log, index {:?}, contains {} cmds",
                state.log.len().checked_sub(1),
                cmds.len()
            );
        });
    }
}

/// Try to receive all the messages in `sync_chan`, preparing for the batch sync.
/// The term number comes from the command received.
// TODO: set a maximum value
async fn fetch_sync_msgs<C: Command + 'static>(
    sync_chan: &mut MpscKeyBasedReceiver<C::K, SyncMessage<C>>,
) -> Result<(Vec<Arc<C>>, u64), ()> {
    let mut term = 0;
    let mut met_msg = false;
    let mut cmds = vec![];
    loop {
        let sync_msg = match sync_chan.try_recv() {
            Ok(sync_msg) => sync_msg,
            Err(RecvError::ChannelStop) => return Err(()),
            Err(RecvError::NoAvailable) => {
                if met_msg {
                    break;
                }

                match sync_chan.async_recv().await {
                    Ok(msg) => msg,
                    Err(_) => return Err(()),
                }
            }
            Err(RecvError::Timeout) => unreachable!("try_recv won't return timeout error"),
        };
        met_msg = true;
        let (t, cmd) = sync_msg.map_msg(SyncMessage::inner);
        term = t;
        cmds.push(cmd);
    }
    Ok((cmds, term))
}

/// Interval between sending heartbeats
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
/// Rpc request timeout
const RPC_TIMEOUT: Duration = Duration::from_millis(50);

/// Background `append_entries`, only works for the leader
async fn bg_append_entries<C: Command + 'static>(
    connects: Vec<Arc<Connect>>,
    state: Arc<RwLock<State<C>>>,
    mut ae_trigger_rx: UnboundedReceiver<usize>,
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
async fn bg_apply<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    state: Arc<RwLock<State<C>>>,
    cmd_exe_tx: CmdExecuteSender<C>,
    ce: Arc<CE>,
    comp_chan: SpmcKeyBasedSender<C::K, SyncCompleteMessage<C>>,
    spec: Arc<Mutex<SpeculativePool<C>>>,
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
                // leader commits a log by sending it through compl_chan
                if state.is_leader() {
                    if comp_chan
                        .send(
                            cmd.keys(),
                            SyncCompleteMessage::new(i.numeric_cast(), Arc::clone(cmd)),
                        )
                        .is_err()
                    {
                        error!("The comp_chan is closed on the remote side");
                        break;
                    }
                } else {
                    // followers execute commands directly
                    // FIXME: should follower store er and asr?
                    let er = cmd_exe_tx.send(Arc::clone(cmd));
                    let cmd_cloned = Arc::clone(cmd);
                    let ce = Arc::clone(&ce);
                    let _asr_ignored = tokio::spawn(async move {
                        let _et_ignored = er.await;
                        cmd_cloned.after_sync(ce.as_ref(), i.numeric_cast()).await
                    });
                    spec.lock().mark_ready(cmd.id());
                }
            }
            state.last_applied = i;
            debug!("log[{i}] committed, last_applied updated to {}", i);
        }
    }
}

/// How long a candidate should wait before it starts another round of election
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a follower should wait before it starts a round of election (in millis)
const FOLLOWER_TIMEOUT: Range<u64> = 300..500;

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

/// Messages sent to the background cmd execution task
pub(crate) struct ExecuteMessage<C: Command + 'static> {
    /// The cmd to be executed
    cmd: Arc<C>,
    /// Send execution result
    er_tx: oneshot::Sender<Result<C::ER, ExecuteError>>,
}

/// Number of execute workers
const N_EXECUTE_WORKERS: usize = 8;

/// Worker that execute commands
async fn execute_worker<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    dispatch_rx: SpmcKeyBasedReceiver<C::K, Option<ExecuteMessage<C>>>,
    ce: Arc<CE>,
) {
    while let Ok((msg_wrapped, done)) = dispatch_rx.recv().await {
        #[allow(clippy::unwrap_used)]
        // it's a hack to bypass the map_msg(you can't do await in map_msg)
        // TODO: is there a better way to mark a spmc msg done instead of sending the msg back
        let msg = msg_wrapped.map_msg(Option::take).unwrap();

        let er = msg.cmd.execute(ce.as_ref()).await;
        debug!("cmd {:?} is executed", msg.cmd.id());

        // send er back
        let _ignore = msg.er_tx.send(er); // it's ok to ignore the result here because sometimes er is not needed

        if let Err(e) = done.send(msg_wrapped) {
            warn!("{e}");
        }
    }
}

/// The only place where cmds get executed
async fn bg_execute_cmd<C: Command + 'static, CE: 'static + CommandExecutor<C>>(
    cmd_executor: Arc<CE>,
    mut cmd_rx: UnboundedReceiver<ExecuteMessage<C>>,
) {
    // TODO: use KeyBasedMpsc to dispatch cmds to execute in parallel
    let (dispatch_tx, dispatch_rx) = key_spmc::channel();

    // spawn cmd executor worker
    iter::repeat((dispatch_rx, cmd_executor))
        .take(N_EXECUTE_WORKERS)
        .for_each(|(rx, ce)| {
            let _worker_handle = tokio::spawn(execute_worker(rx, ce));
        });

    // TODO: avoid re-dispatch by using a mpmc channel
    while let Some(msg) = cmd_rx.recv().await {
        let cmd = Arc::clone(&msg.cmd);
        if let Err(e) = dispatch_tx.send(cmd.keys(), Some(msg)) {
            warn!("failed to send cmd to execute worker, {e}");
        }
    }
    error!("bg execute cmd stopped unexpectedly");
}

/// Send cmd to background execute cmd
pub(crate) struct CmdExecuteSender<C: Command + 'static>(UnboundedSender<ExecuteMessage<C>>);

impl<C: Command + 'static> Clone for CmdExecuteSender<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<C: Command + 'static> CmdExecuteSender<C> {
    /// Send cmd to background cmd executor and return a oneshot receiver for the execution result
    pub(crate) fn send(
        &self,
        cmd: Arc<C>,
    ) -> oneshot::Receiver<Result<<C as Command>::ER, ExecuteError>> {
        let (er_tx, er_rx) = oneshot::channel();
        if let Err(e) = self.0.send(ExecuteMessage { cmd, er_tx }) {
            warn!("failed to send cmd to background execute cmd, {e}");
        }
        er_rx
    }
}

/// Create a channel to send cmds to background cmd execute tasks
pub(crate) fn cmd_execute_channel<C: Command + 'static>(
) -> (CmdExecuteSender<C>, UnboundedReceiver<ExecuteMessage<C>>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (CmdExecuteSender(tx), rx)
}
