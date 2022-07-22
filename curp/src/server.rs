use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
};

use clippy_utilities::NumericCast;
use futures::FutureExt;
use madsim::net::Endpoint;
use parking_lot::{Mutex, MutexGuard, RwLock};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    cmd::{Command, CommandExecutor, ProposeId},
    error::{ProposeError, ServerError},
    keybased_channel::{self, KeybasedChannelSender},
    log::{EntryStatus, LogEntry},
    message::{
        Propose, ProposeResponse, SyncCommand, SyncResponse, TermNum, WaitSynced,
        WaitSyncedResponse,
    },
    sync_manager::{SyncManager, SyncMessage},
    util::MutexMap,
};

/// Default server serving port
pub(crate) static DEFAULT_SERVER_PORT: u16 = 12345;

/// The Rpc Server to handle rpc requests
/// This Wrapper is introduced due to the madsim rpc lib
#[derive(Clone, Debug)]
pub struct Rpc<C: Command + 'static, CE: CommandExecutor<C> + 'static> {
    /// The inner server wrappped in an Arc so that we share state while clone the rpc wrapper
    inner: Arc<Protocol<C, CE>>,
}

#[allow(missing_docs)]
#[madsim::service]
impl<C: Command + 'static, CE: CommandExecutor<C> + 'static> Rpc<C, CE> {
    /// Handle propose request, from client
    #[rpc]
    async fn propose(&self, p: Propose<C>) -> ProposeResponse<C> {
        self.inner.propose(p).await
    }

    /// Handle sync request, from server
    #[rpc]
    async fn sync(&self, sc: SyncCommand<C>) -> SyncResponse<C> {
        self.inner.sync(sc).await
    }

    /// Handle wait_sync request, from client
    #[rpc]
    async fn wait_sync(&self, sc: WaitSynced<C>) -> WaitSyncedResponse<C> {
        self.inner.wait_sync(sc).await
    }

    /// Run a new rpc server
    #[inline]
    pub async fn run(
        is_leader: bool,
        term: u64,
        others: Vec<SocketAddr>,
        server_port: Option<u16>,
        executor: CE,
    ) -> Result<(), ServerError> {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        let rx_ep = Endpoint::bind(format!("0.0.0.0:{port}")).await?;
        let tx_ep = Endpoint::bind("127.0.0.1:0").await?;
        let server = Self {
            inner: Arc::new(Protocol::new(is_leader, term, tx_ep, &others, executor)),
        };
        Self::serve_on(server, rx_ep)
            .await
            .map_err(|e| ServerError::RpcServiceError(format!("{e}")))
    }
}

/// The join handler of synced task
#[derive(Debug)]
struct SyncedTaskJoinHandle<C: Command + 'static>(JoinHandle<WaitSyncedResponse<C>>);

/// The server that handles client request and server consensus protocol
pub struct Protocol<C: Command + 'static, CE: CommandExecutor<C> + 'static> {
    /// the server role
    role: RwLock<ServerRole>,
    /// Current term
    term: RwLock<TermNum>,
    /// The speculative cmd pool, shared with executor
    spec: Arc<Mutex<VecDeque<C>>>,
    /// Command executor
    cmd_executor: Arc<CE>,
    /// Consensus log
    log: Arc<Mutex<Vec<LogEntry<C>>>>,
    /// The channel to send synced command
    sync_chan: KeybasedChannelSender<C::K, SyncMessage<C>>,
    /// Synced manager handler
    sm_handle: JoinHandle<()>,
    /// Sync event listener map
    synced_events: Mutex<HashMap<ProposeId, SyncedTaskJoinHandle<C>>>,
}

impl<C, CE> Debug for Protocol<C, CE>
where
    C: Command,
    CE: CommandExecutor<C>,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("role", &self.role)
            .field("term", &self.term)
            .field("spec", &self.spec)
            .field("cmd_executor", &self.cmd_executor)
            .field("log", &self.log)
            .field("synced_events", &self.synced_events)
            .finish()
    }
}

/// The server role same as Raft
#[derive(Debug, Clone, Copy)]
enum ServerRole {
    /// A follower
    Follower,
    /// A candidate
    #[allow(dead_code)]
    Candidate,
    /// A leader
    Leader,
}

impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Protocol<C, CE> {
    /// Create a new server instance
    #[must_use]
    #[inline]
    pub fn new(
        is_leader: bool,
        term: u64,
        send_ep: Endpoint,
        others: &[SocketAddr],
        cmd_executor: CE,
    ) -> Self {
        let (tx, rx) = keybased_channel::channel();
        let other_addrs = Arc::new(RwLock::new(others.to_vec()));
        let log = Arc::new(Mutex::new(Vec::new()));

        let log_clone = Arc::<_>::clone(&log);
        let sm_handle = tokio::spawn(async {
            let mut sm = SyncManager::new(send_ep, rx, other_addrs, log_clone);
            sm.run().await;
        });

        Self {
            role: RwLock::new(if is_leader {
                ServerRole::Leader
            } else {
                ServerRole::Follower
            }),
            term: RwLock::new(term),
            spec: Arc::new(Mutex::new(VecDeque::new())),
            cmd_executor: Arc::new(cmd_executor),
            log,
            sync_chan: tx,
            sm_handle,
            synced_events: Mutex::new(HashMap::new()),
        }
    }

    /// Check if the `cmd` conflict with any conflicts in the speculative cmd pool
    fn is_spec_conflict(spec: &MutexGuard<'_, VecDeque<C>>, cmd: &C) -> bool {
        spec.iter().any(|spec_cmd| spec_cmd.is_conflict(cmd))
    }

    /// Check if the current server is leader
    fn is_leader(&self) -> bool {
        matches!(*self.role.read(), ServerRole::Leader)
    }

    /// Remove command from the speculative cmd pool
    fn spec_remove_cmd(spec: &Arc<Mutex<VecDeque<C>>>, cmd_id: &ProposeId) -> Option<C> {
        spec.map_lock(|mut spec_unlocked| {
            spec_unlocked
                .iter()
                .position(|s| s.id() == cmd_id)
                .map(|index| spec_unlocked.swap_remove_back(index))
        })
        .flatten()
    }

    /// Send sync event to the `SyncManager`, it's not a blocking function
    fn sync_to_others(&self, term: TermNum, cmd: &C, need_execute: bool) -> bool {
        let (complete_sender, sync_comp_rx) = oneshot::channel();

        let complete_notify = self.sync_chan.send(
            cmd.keys(),
            SyncMessage::new(term, cmd.clone(), complete_sender),
        );
        let spec = Arc::<_>::clone(&self.spec);
        let cmd2exe = cmd.clone();
        let dispatch_executor = Arc::<CE>::clone(&self.cmd_executor);

        let handle = tokio::spawn(async move {
            let sync_compl_result = sync_comp_rx.await;
            let cmd_id = cmd2exe.id().clone();
            if Self::spec_remove_cmd(&spec, &cmd_id).is_none() {
                unreachable!("{:?} should be in the spec pool", cmd_id);
            }

            let call_after_sync = |index, er| {
                cmd2exe
                    .after_sync(dispatch_executor.as_ref(), index)
                    .map(|after_sync_result| match after_sync_result {
                        Ok(asr) => WaitSyncedResponse::<C>::new_success(asr, er),
                        Err(e) => WaitSyncedResponse::<C>::new_error(format!(
                            "after_sync execution error: {:?}",
                            e
                        )),
                    })
            };

            match sync_compl_result {
                Ok(reply) => {
                    let (index, keys_msg) = (reply.log_index(), reply.keys_msg());
                    let after_sync_result = if need_execute {
                        match cmd2exe.execute(dispatch_executor.as_ref()).await {
                            Ok(er) => call_after_sync(index, Some(er)).await,
                            Err(e) => WaitSyncedResponse::new_error(format!(
                                "cmd execution error: {:?}",
                                e
                            )),
                        }
                    } else {
                        call_after_sync(index, None).await
                    };
                    let _ignore = complete_notify.send(keys_msg);
                    after_sync_result
                }
                // TODO: handle the sync task stop, usually we should stop working
                Err(e) => unreachable!("{:?} sync task failed, {}", cmd_id, e),
            }
        });
        self.synced_events.map_lock(|mut events| {
            if events
                .insert(cmd.id().clone(), SyncedTaskJoinHandle(handle))
                .is_some()
            {
                unreachable!("{:?} should not be inserted in events map", cmd.id());
            }
        });
        true
    }

    /// Propose request handler
    async fn propose(&self, p: Propose<C>) -> ProposeResponse<C> {
        let is_leader = self.is_leader();
        let self_term = *self.term.read();
        self.spec
            .map_lock(|mut spec| {
                let is_conflict = Self::is_spec_conflict(&spec, p.cmd());
                spec.push_back(p.cmd().clone());

                match (is_conflict, is_leader) {
                    (true, true) => async {
                        // the leader need to sync cmd
                        if self.sync_to_others(self_term, p.cmd(), true) {
                            ProposeResponse::new_error(
                                is_leader,
                                self_term,
                                ProposeError::KeyConflict,
                            )
                        } else {
                            ProposeResponse::new_error(
                                is_leader,
                                *self.term.read(),
                                ProposeError::SyncedError("Sync cmd channel closed".to_owned()),
                            )
                        }
                    }
                    .left_future()
                    .left_future(),
                    (true, false) => async {
                        ProposeResponse::new_error(is_leader, self_term, ProposeError::KeyConflict)
                    }
                    .left_future()
                    .right_future(),
                    (false, true) => {
                        // only the leader executes the command in speculative pool
                        p.cmd()
                            .execute(self.cmd_executor.as_ref())
                            .map(|er| {
                                er.map_or_else(
                                    |err| {
                                        ProposeResponse::new_error(
                                            is_leader,
                                            self_term,
                                            ProposeError::ExecutionError(err.to_string()),
                                        )
                                    },
                                    |rv| {
                                        // the leader need to sync cmd
                                        if self.sync_to_others(self_term, p.cmd(), false) {
                                            ProposeResponse::new_ok(is_leader, self_term, rv)
                                        } else {
                                            ProposeResponse::new_error(
                                                is_leader,
                                                *self.term.read(),
                                                ProposeError::SyncedError(
                                                    "Sync cmd channel closed".to_owned(),
                                                ),
                                            )
                                        }
                                    },
                                )
                            })
                            .right_future()
                            .left_future()
                    }
                    (false, false) => {
                        async { ProposeResponse::new_empty(false, *self.term.read()) }
                            .right_future()
                            .right_future()
                    }
                }
            })
            .await
    }

    /// Handle wait sync request
    async fn wait_sync(&self, ws: WaitSynced<C>) -> WaitSyncedResponse<C> {
        let handle = match self
            .synced_events
            .map_lock(|mut events| events.remove(ws.id()))
        {
            Some(handle) => handle,
            None => {
                return WaitSyncedResponse::new_error(format!(
                    "command {:?} is not in the event hashmap",
                    ws.id()
                ))
            }
        };
        match handle.0.await {
            Ok(wsr) => wsr,
            Err(e) => WaitSyncedResponse::new_error(format!("sync task failed, {:?}", e)),
        }
    }

    /// Handle sync request
    async fn sync(&self, sc: SyncCommand<C>) -> SyncResponse<C> {
        self.log.map_lock(|mut log| {
            let local_len = log.len();
            match sc.index() {
                t if t > local_len.numeric_cast() => {
                    SyncResponse::PrevNotReady(local_len.numeric_cast())
                }
                t if t < local_len.numeric_cast() => {
                    // checked in the if condition
                    #[allow(clippy::unwrap_used)]
                    let entry: &LogEntry<C> = log.get(t.numeric_cast::<usize>()).unwrap();
                    SyncResponse::EntryNotEmpty((entry.term(), entry.cmd().clone()))
                }
                _ => {
                    let self_term = *self.term.read();
                    if sc.term() < self_term {
                        SyncResponse::WrongTerm(self_term)
                    } else {
                        log.push(LogEntry::new(
                            sc.term(),
                            sc.cmd().clone(),
                            EntryStatus::Unsynced,
                        ));
                        SyncResponse::Synced
                    }
                }
            }
        })
    }
}

impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Drop for Protocol<C, CE> {
    #[inline]
    fn drop(&mut self) {
        self.sm_handle.abort();
    }
}
