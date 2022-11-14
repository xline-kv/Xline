use std::collections::VecDeque;
use std::{sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use futures::{stream::FuturesUnordered, StreamExt};
use madsim::rand::{thread_rng, Rng};
use parking_lot::lock_api::{Mutex, RwLockUpgradableReadGuard};
use parking_lot::{RawMutex, RwLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::cmd::CommandExecutor;
use crate::rpc::{AppendEntriesRequest, VoteRequest};
use crate::server::{Protocol, ServerRole, State};
use crate::util::RwLockMap;
use crate::{
    channel::{key_mpsc::MpscKeyBasedReceiver, key_spmc::SpmcKeyBasedSender, RecvError},
    cmd::Command,
    log::LogEntry,
    message::TermNum,
    rpc::{self, Connect},
    LogIndex,
};

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

/// The message sent to the `SyncManager`
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

/// The manager to sync commands to other follower servers
pub(crate) struct SyncManager<C: Command + 'static> {
    /// Current state
    state: Arc<RwLock<State<C>>>,
    /// Last time a rpc is received
    last_rpc_time: Arc<RwLock<Instant>>,
    /// Other addrs
    connects: Vec<Arc<Connect>>,
    /// Get cmd sync request from speculative command
    sync_chan: MpscKeyBasedReceiver<C::K, SyncMessage<C>>,
}

impl<C: Command + 'static> SyncManager<C> {
    /// Create a `SyncedManager`
    pub(crate) async fn new(
        sync_chan: MpscKeyBasedReceiver<C::K, SyncMessage<C>>,
        others: Vec<String>,
        state: Arc<RwLock<State<C>>>,
        last_rpc_time: Arc<RwLock<Instant>>,
    ) -> Self {
        Self {
            sync_chan,
            state,
            connects: rpc::try_connect(others.into_iter().map(|a| format!("http://{a}")).collect())
                .await,
            last_rpc_time,
        }
    }

    /// Get a clone of the connections
    fn connects(&self) -> Vec<Arc<Connect>> {
        self.connects.clone()
    }

    /// Try to receive all the messages in `sync_chan`, preparing for the batch sync.
    /// The term number comes from the command received.
    // TODO: set a maximum value
    async fn fetch_sync_msgs(&mut self) -> Result<(Vec<Arc<C>>, u64), ()> {
        let mut term = 0;
        let mut met_msg = false;
        let mut cmds = vec![];
        loop {
            let sync_msg = match self.sync_chan.try_recv() {
                Ok(sync_msg) => sync_msg,
                Err(RecvError::ChannelStop) => return Err(()),
                Err(RecvError::NoAvailable) => {
                    if met_msg {
                        break;
                    }

                    match self.sync_chan.async_recv().await {
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

    /// Run the `SyncManager`
    pub(crate) async fn run<CE: 'static + CommandExecutor<C>>(
        &mut self,
        comp_chan: SpmcKeyBasedSender<C::K, SyncCompleteMessage<C>>,
        cmd_executor: Arc<CE>,
        commit_trigger: UnboundedSender<()>,
        commit_trigger_rx: UnboundedReceiver<()>,
        spec: Arc<Mutex<RawMutex, VecDeque<C>>>,
    ) {
        // notify when a broadcast of append_entries is needed immediately(a new log is received or committed)
        let (ae_trigger, ae_trigger_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
        // notify when heartbeat is needed immediately
        let (heartbeat_trigger, heartbeat_trigger_rx) =
            tokio::sync::mpsc::unbounded_channel::<()>();
        // TODO: gracefully stop the background tasks
        let _background_ae_handle = tokio::spawn(Self::bg_append_entries(
            self.connects(),
            Arc::clone(&self.state),
            commit_trigger,
            ae_trigger_rx,
        ));
        let _background_election_handle = tokio::spawn(Self::bg_election(
            self.connects(),
            Arc::clone(&self.state),
            Arc::clone(&self.last_rpc_time),
            heartbeat_trigger,
        ));
        let _background_apply_handle = tokio::spawn(Self::bg_apply(
            Arc::clone(&self.state),
            cmd_executor,
            comp_chan,
            commit_trigger_rx,
            spec,
        ));
        let _background_heartbeat_handle = tokio::spawn(Self::bg_heartbeat(
            self.connects(),
            Arc::clone(&self.state),
            heartbeat_trigger_rx,
        ));

        loop {
            let (cmds, term) = match self.fetch_sync_msgs().await {
                Ok((cmds, term)) => (cmds, term),
                Err(_) => return,
            };

            self.state.map_write(|mut state| {
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

    /// Interval between sending heartbeats
    const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
    /// Rpc request timeout
    const RPC_TIMEOUT: Duration = Duration::from_millis(50);

    /// Background `append_entries`, only works for the leader
    async fn bg_append_entries(
        connects: Vec<Arc<Connect>>,
        state: Arc<RwLock<State<C>>>,
        commit_trigger: UnboundedSender<()>,
        mut ae_trigger_rx: UnboundedReceiver<usize>,
    ) {
        while let Some(i) = ae_trigger_rx.recv().await {
            if state.read().is_leader() {
                // send append_entries to each server in parallel
                let rpcs: FuturesUnordered<_> = connects
                    .iter()
                    .map(|connect| {
                        Self::send_log_until_succeed(
                            i,
                            Arc::clone(connect),
                            Arc::clone(&state),
                            commit_trigger.clone(),
                        )
                    })
                    .collect();
                let _drop = tokio::spawn(async move {
                    let _drop: Vec<_> = rpcs.collect().await;
                });
            }
        }
    }

    /// Send `append_entries` containing a single log to a server
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn send_log_until_succeed(
        i: usize,
        connect: Arc<Connect>,
        state: Arc<RwLock<State<C>>>,
        commit_trigger: UnboundedSender<()>,
    ) {
        // prepare append_entries request args
        #[allow(clippy::shadow_unrelated)] // clippy false positive
        let (term, prev_log_index, prev_log_term, entries, leader_commit) =
            state.map_read(|state| {
                (
                    state.term,
                    i - 1,
                    state.log[i - 1].term(),
                    vec![state.log[i].clone()],
                    state.commit_index,
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

        // send log[i] until succeed
        loop {
            debug!("append_entries sent to {}", connect.addr);
            let resp = connect.append_entries(req.clone(), Self::RPC_TIMEOUT).await;

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
                            if let Err(e) = commit_trigger.send(()) {
                                error!("commit_trigger failed: {}", e);
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    /// Background `append_entries`, only works for the leader
    async fn bg_heartbeat(
        connects: Vec<Arc<Connect>>,
        state: Arc<RwLock<State<C>>>,
        mut heartbeat_trigger_rx: UnboundedReceiver<()>,
    ) {
        #[allow(clippy::integer_arithmetic)] // tokio internal triggered
        loop {
            // TODO: notify when server's role changes instead of busy polling
            if state.read().is_leader() {
                // send append_entries to each server in parallel
                let rpcs: FuturesUnordered<_> = connects
                    .iter()
                    .map(|connect| Self::send_heartbeat(Arc::clone(connect), Arc::clone(&state)))
                    .collect();
                let _drop = tokio::spawn(async move {
                    let _drop: Vec<_> = rpcs.collect().await;
                });
            }
            tokio::select! {
                _ = heartbeat_trigger_rx.recv() => {},
                _ = tokio::time::sleep(Self::HEARTBEAT_INTERVAL) => {}
            }
        }
    }

    /// Send `append_entries` to a server
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn send_heartbeat(connect: Arc<Connect>, state: Arc<RwLock<State<C>>>) {
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
        let resp = connect.append_entries(req, Self::RPC_TIMEOUT).await;

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
    async fn bg_apply<CE: 'static + CommandExecutor<C>>(
        state: Arc<RwLock<State<C>>>,
        cmd_executor: Arc<CE>,
        comp_chan: SpmcKeyBasedSender<C::K, SyncCompleteMessage<C>>,
        mut commit_notify: UnboundedReceiver<()>,
        spec: Arc<Mutex<RawMutex, VecDeque<C>>>,
    ) {
        while commit_notify.recv().await.is_some() {
            let state = state.upgradable_read();
            if !state.need_commit() {
                continue;
            }

            let mut state = RwLockUpgradableReadGuard::upgrade(state);
            #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
            // TODO: overflow of log index should be prevented
            for i in (state.last_applied + 1)..=state.commit_index {
                for cmd in state.log[i].cmds().iter() {
                    // TODO: execution of leaders and followers should be merged
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
                        let cmd_executor = Arc::clone(&cmd_executor);
                        let cmd = Arc::clone(cmd);
                        let spec = Arc::clone(&spec);
                        let _handle = tokio::spawn(async move {
                            // TODO: execute command in parallel
                            // TODO: handle execution error
                            let _execute_result = cmd.execute(cmd_executor.as_ref()).await;
                            let _after_sync_result = cmd
                                .after_sync(cmd_executor.as_ref(), i.numeric_cast())
                                .await;
                            if Protocol::<C, CE>::spec_remove_cmd(&spec, cmd.id()).is_none() {
                                unreachable!("{:?} should be in the spec pool", cmd.id());
                            }
                        });
                    }
                }
                state.last_applied = i;
                debug!("log[{i}] committed, last_applied updated to {}", i);
            }
        }
        info!("background apply stopped");
    }

    /// The time a candidate should wait before it starts another round of election
    const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(1);

    /// Background election
    async fn bg_election(
        connects: Vec<Arc<Connect>>,
        state: Arc<RwLock<State<C>>>,
        last_rpc_time: Arc<RwLock<Instant>>,
        heartbeat_trigger: UnboundedSender<()>,
    ) {
        loop {
            // TODO: notify when server's role changes instead of busy polling
            tokio::time::sleep(Duration::from_millis(10)).await;
            let timeout = {
                let mut rng = thread_rng();
                match state.read().role {
                    ServerRole::Follower => Duration::from_millis(rng.gen_range(300..500)),
                    ServerRole::Candidate => Self::CANDIDATE_TIMEOUT,
                    ServerRole::Leader => Duration::from_secs(1), // it's a fake one, since leader will never timeout
                }
            };
            if state.read().is_leader() || Instant::now() - *last_rpc_time.read() < timeout {
                continue;
            }

            // start election
            #[allow(clippy::integer_arithmetic)] // TODO: handle possible overflow
            let req = {
                let mut state = state.write();
                if state.is_leader() {
                    return;
                }
                let new_term = state.term + 1;
                state.update_to_term(new_term);
                state.role = ServerRole::Candidate;
                VoteRequest::new(
                    state.term,
                    state.id.clone(),
                    state.last_log_index(),
                    state.last_log_term(),
                )
            };
            debug!("server {} starts election", req.candidate_id);

            for connect in &connects {
                let _ignore = tokio::spawn(Self::send_vote(
                    Arc::clone(connect),
                    Arc::clone(&state),
                    heartbeat_trigger.clone(),
                    req.clone(),
                ));
            }
        }
    }

    /// send vote request
    async fn send_vote(
        connect: Arc<Connect>,
        state: Arc<RwLock<State<C>>>,
        heartbeat_trigger: UnboundedSender<()>,
        req: VoteRequest,
    ) {
        let resp = connect.vote(req, Self::RPC_TIMEOUT).await;
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
                if !matches!(state.role, ServerRole::Candidate) {
                    return;
                }

                #[allow(clippy::integer_arithmetic)]
                if resp.vote_granted {
                    debug!("vote is granted by server {}", connect.addr);
                    let mut state = RwLockUpgradableReadGuard::upgrade(state);
                    state.votes_received += 1;

                    // the majority has granted the vote
                    let min_granted = (state.others.len() + 1) / 2;
                    if state.votes_received > min_granted {
                        state.role = ServerRole::Leader;
                        let last_log_index = state.last_log_index();
                        // init next_index
                        for index in state.next_index.values_mut() {
                            *index = last_log_index + 1; // iter from the end to front is more likely to match the follower
                        }
                        debug!("server becomes leader");

                        // trigger heartbeat immediately to establish leadership
                        if let Err(e) = heartbeat_trigger.send(()) {
                            error!("commit_trigger failed: {}", e);
                        }
                    }
                }
            }
        }
    }
}
