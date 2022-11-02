use std::cmp::max;
use std::{sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use tracing::{debug, error, warn};

use crate::cmd::CommandExecutor;
use crate::rpc::AppendEntriesRequest;
use crate::server::State;
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
    ) -> Self {
        Self {
            sync_chan,
            state,
            connects: rpc::try_connect(others.into_iter().map(|a| format!("http://{a}")).collect())
                .await,
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
    ) {
        // TODO: gracefully stop the background tasks
        let _background_ping_handle = tokio::spawn(Self::background_ping(
            self.connects(),
            Arc::clone(&self.state),
        ));
        let _background_commit_handle = tokio::spawn(Self::background_commit(
            self.connects(),
            Arc::clone(&self.state),
        ));
        let _background_apply_handle = tokio::spawn(Self::background_apply(
            Arc::clone(&self.state),
            cmd_executor,
            comp_chan,
        ));

        loop {
            let (cmds, term) = match self.fetch_sync_msgs().await {
                Ok((cmds, term)) => (cmds, term),
                Err(_) => return,
            };

            self.state.map_write(|mut state| {
                state.log.push(LogEntry::new(term, &cmds));
                debug!(
                    "received new log, index {:?}",
                    state.log.len().checked_sub(1)
                );
            });
        }
    }

    /// Heartbeat Interval
    const HEART_BEAT_INTERVAL: Duration = Duration::from_millis(150);
    /// Heartbeat Timeout
    const HEART_BEAT_TIMEOUT: Duration = Duration::from_millis(100);

    /// Background ping, only work for the leader
    async fn background_ping(connects: Vec<Arc<Connect>>, state: Arc<RwLock<State<C>>>) {
        loop {
            tokio::time::sleep(Self::HEART_BEAT_INTERVAL).await;
            if state.read().is_leader() {
                // send out heartbeat to each server in parallel

                let rpcs: FuturesUnordered<_> = connects
                    .iter()
                    .map(|connect| Self::heartbeat(Arc::clone(connect), Arc::clone(&state)))
                    .collect();
                let _drop = tokio::spawn(async move {
                    let _drop: Vec<_> = rpcs.collect().await;
                });
            }
        }
    }

    /// Send heartbeat to a server
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn heartbeat(connect: Arc<Connect>, state: Arc<RwLock<State<C>>>) {
        let (term, commit_index, prev_log_index, prev_log_term) = state.map_read(|st| {
            (
                st.term,
                st.commit_index,
                st.next_index[&connect.addr] - 1,
                st.log[st.next_index[&connect.addr] - 1].term(),
            )
        });
        debug!("heartbeat sent to {}", connect.addr);
        let res = connect
            .append_entries(
                AppendEntriesRequest::new_heart_beat(
                    term,
                    0,
                    commit_index,
                    prev_log_index,
                    prev_log_term,
                ),
                Self::HEART_BEAT_TIMEOUT,
            )
            .await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match res {
            Err(e) => warn!("Heartbeat error: {}", e),
            Ok(res) => {
                // TODO: update term

                let res = res.into_inner();
                let mut state = state.write();
                if res.success {
                    if let Some(index) = state.match_index.get_mut(&connect.addr) {
                        *index = max(*index, prev_log_index);
                    } else {
                        unreachable!("no match_index for server {}", connect.addr);
                    }
                } else {
                    *state.next_index.get_mut(&connect.addr).unwrap() -= 1;
                }
            }
        };
    }

    /// How frequently leader should check if a follower has lagged behind
    const SYNC_INTERVAL: Duration = Duration::from_millis(10);
    /// Sync timeout
    const SYNC_TIMEOUT: Duration = Duration::from_millis(8);

    /// Background sync
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn background_commit(connects: Vec<Arc<Connect>>, state: Arc<RwLock<State<C>>>) {
        let majority_cnt = connects.len() / 2 + 1;
        loop {
            tokio::time::sleep(Self::SYNC_INTERVAL).await;
            if !state.read().is_leader() {
                continue;
            }

            // recalculate the commit index: find the largest committed log index
            if let Some(i) = state.map_read(|state| {
                for i in ((state.commit_index + 1)..=state.last_log_index()).rev() {
                    // should not commit logs from previous term
                    if state.log[i].term() != state.term {
                        break;
                    }

                    // calculate the number of servers that has matched log[i]
                    let match_count = connects
                        .iter()
                        .filter(|connect| state.match_index[&connect.addr] >= i)
                        .count();

                    // If the majority of servers has replicated the log, commit
                    // + 1 means including the leader itself
                    if match_count + 1 >= majority_cnt {
                        debug!("commit_index updated to {i}");
                        return Some(i);
                    }
                }
                None
            }) {
                state.write().commit_index = i;
            }

            // send new logs to followers if possible
            let rpcs: FuturesUnordered<_> = connects
                .iter()
                .map(|connect| Self::append_entries(Arc::clone(connect), Arc::clone(&state)))
                .collect();
            let _drop = tokio::spawn(async move {
                let _drop: Vec<_> = rpcs.collect().await;
            });
        }
    }

    /// Send `append_entries` request
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn append_entries(connect: Arc<Connect>, state: Arc<RwLock<State<C>>>) {
        // fetch request args
        let (term, leader_id, prev_log_index, prev_log_term, entries, leader_commit) = {
            let state = state.read();
            // return if the follower is not lagging behind
            if state.next_index[&connect.addr] > state.last_log_index() {
                return;
            }
            let next_index = state.next_index[&connect.addr];
            (
                state.term,
                0, // TODO: add leader_id
                next_index - 1,
                state.log[next_index - 1].term(),
                state.log[next_index..].to_vec(),
                state.commit_index,
            )
        };
        let last_sent_index = prev_log_index + entries.len();
        let req = match AppendEntriesRequest::new(
            term,
            leader_id,
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

        // send the request
        debug!("send append_entries request: prev_log_index({}), prev_log_term({}), last_sent_index({}), leader_commit({})", prev_log_index, prev_log_term, last_sent_index, leader_commit);
        let resp = connect.append_entries(req, Self::SYNC_TIMEOUT).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(e) => warn!("Send append_entries error: {}", e),
            Ok(resp) => {
                // TODO: update term

                let resp = resp.into_inner();
                let mut state = state.write();
                if resp.success {
                    *state.match_index.get_mut(&connect.addr).unwrap() = last_sent_index;
                    *state.next_index.get_mut(&connect.addr).unwrap() = last_sent_index + 1;
                } else {
                    *state.next_index.get_mut(&connect.addr).unwrap() -= 1;
                }
            }
        };
    }

    /// How frequently the server should check if there are any logs that have been committed and needed to be executed
    const APPLY_INTERVAL: Duration = Duration::from_millis(10);

    /// Background apply
    async fn background_apply<CE: 'static + CommandExecutor<C>>(
        state: Arc<RwLock<State<C>>>,
        cmd_executor: Arc<CE>,
        comp_chan: SpmcKeyBasedSender<C::K, SyncCompleteMessage<C>>,
    ) {
        loop {
            tokio::time::sleep(Self::APPLY_INTERVAL).await;
            if state.read().need_commit() {
                let mut state = state.write();
                #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
                // TODO: will last_applied overflow?
                for i in (state.last_applied + 1)..=state.commit_index {
                    debug!("commit log[{}]", i);
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
                            let _handle = tokio::spawn(async move {
                                // TODO: execute command in parallel
                                // TODO: handle execution error
                                let _execute_result = cmd.execute(cmd_executor.as_ref()).await;
                                let _after_sync_result = cmd
                                    .after_sync(cmd_executor.as_ref(), i.numeric_cast())
                                    .await;
                            });
                        }
                        // TODO: remove the cmd from speculative command pool
                    }
                    debug!("last_applied updated to {}", i);
                    state.last_applied = i;
                }
            }
        }
    }
}
