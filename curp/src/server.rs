use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::Arc,
};

use clippy_utilities::NumericCast;
use event_listener::Event;
use futures::FutureExt;
use parking_lot::{Mutex, MutexGuard, RwLock};
use tokio::task::JoinHandle;

use crate::{
    channel::{
        key_mpsc::{self, MpscKeybasedSender},
        key_spmc::{self, SpmcKeybasedReceiver},
        SendError,
    },
    cmd::{Command, CommandExecutor, ProposeId},
    error::{ProposeError, ServerError},
    log::{EntryStatus, LogEntry},
    message::TermNum,
    rpc::{
        ProposeRequest, ProposeResponse, ProtocolServer, SyncRequest, SyncResponse,
        WaitSyncedRequest, WaitSyncedResponse,
    },
    sync_manager::{SyncCompleteMessage, SyncManager, SyncMessage},
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

#[tonic::async_trait]
impl<C, CE> crate::rpc::Protocol for Rpc<C, CE>
where
    C: 'static + Command,
    CE: 'static + CommandExecutor<C>,
{
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        self.inner.propose(request).await
    }
    async fn wait_synced(
        &self,
        request: tonic::Request<WaitSyncedRequest>,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status> {
        self.inner.wait_synced(request).await
    }
    async fn sync(
        &self,
        request: tonic::Request<SyncRequest>,
    ) -> Result<tonic::Response<SyncResponse>, tonic::Status> {
        self.inner.sync(request).await
    }
}

impl<C: Command + 'static, CE: CommandExecutor<C> + 'static> Rpc<C, CE> {
    /// Run a new rpc server
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[inline]
    pub async fn run(
        is_leader: bool,
        term: u64,
        others: Vec<String>,
        server_port: Option<u16>,
        executor: CE,
    ) -> Result<(), ServerError> {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        let server = Self {
            inner: Arc::new(Protocol::new(is_leader, term, others, executor)),
        };
        tonic::transport::Server::builder()
            .add_service(ProtocolServer::new(server))
            .serve(
                format!("0.0.0.0:{}", port)
                    .parse()
                    .map_err(|e| ServerError::ParsingError(format!("{}", e)))?,
            )
            .await?;
        Ok(())
    }
}

/// The join handler of synced task
#[derive(Debug)]
struct SyncedTaskJoinHandle(JoinHandle<Result<WaitSyncedResponse, bincode::Error>>);

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
    /// The channel to send synced command to sync manager
    sync_chan: MpscKeybasedSender<C::K, SyncMessage<C>>,
    /// The channel to receive synced command from sync manager
    comp_chan: SpmcKeybasedReceiver<C::K, SyncCompleteMessage>,
    /// Synced manager handler
    sm_handle: JoinHandle<()>,
    /// Sync event listener map
    synced_events: Mutex<HashMap<ProposeId, SyncedTaskJoinHandle>>,
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
    pub fn new(is_leader: bool, term: u64, others: Vec<String>, cmd_executor: CE) -> Self {
        let (sync_tx, sync_rx) = key_mpsc::channel();
        let (comp_tx, comp_rx) = key_spmc::channel();
        let log = Arc::new(Mutex::new(Vec::new()));

        let log_clone = Arc::<_>::clone(&log);
        let sm_handle = tokio::spawn(async {
            let mut sm = SyncManager::new(sync_rx, comp_tx, others, log_clone).await;
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
            sync_chan: sync_tx,
            comp_chan: comp_rx,
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
    fn sync_to_others(
        &self,
        term: TermNum,
        cmd: &C,
        need_execute: bool,
    ) -> Result<Event, SendError> {
        let comp_rx = self.comp_chan.clone();
        let ready_notify = self
            .sync_chan
            .send(cmd.keys(), SyncMessage::new(term, cmd.clone()))?;
        let spec = Arc::<_>::clone(&self.spec);
        let cmd2exe = cmd.clone();
        let dispatch_executor = Arc::<CE>::clone(&self.cmd_executor);

        let handle = tokio::spawn(async move {
            let sync_compl_result = comp_rx.recv().await;
            let cmd_id = cmd2exe.id().clone();

            let call_after_sync = |index, er| {
                cmd2exe.after_sync(dispatch_executor.as_ref(), index).map(
                    move |after_sync_result| match after_sync_result {
                        Ok(asr) => WaitSyncedResponse::new_success::<C>(&asr, &er),
                        Err(e) => WaitSyncedResponse::new_error::<C>(&format!(
                            "after_sync execution error: {:?}",
                            e
                        )),
                    },
                )
            };

            let after_sync_result = match sync_compl_result {
                Ok((reply, notifier)) => {
                    let index = reply.map_msg(|csm| csm.log_index());
                    let after_sync_result = if need_execute {
                        match cmd2exe.execute(dispatch_executor.as_ref()).await {
                            Ok(er) => call_after_sync(index, Some(er)).await,
                            Err(e) => WaitSyncedResponse::new_error::<C>(&format!(
                                "cmd execution error: {:?}",
                                e
                            )),
                        }
                    } else {
                        call_after_sync(index, None).await
                    };
                    let _ignore = notifier.send(reply);
                    after_sync_result
                }
                // TODO: handle the sync task stop, usually we should stop working
                Err(e) => unreachable!("{:?} sync task failed, {}", cmd_id, e),
            };

            if Self::spec_remove_cmd(&spec, &cmd_id).is_none() {
                unreachable!("{:?} should be in the spec pool", cmd_id);
            }

            after_sync_result
        });
        self.synced_events.map_lock(|mut events| {
            if events
                .insert(cmd.id().clone(), SyncedTaskJoinHandle(handle))
                .is_some()
            {
                unreachable!(
                    "{:?} should not be inserted in events map, it's synced before?",
                    cmd.id()
                );
            }
        });
        Ok(ready_notify)
    }

    /// handle "propose" request
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        let is_leader = self.is_leader();
        let self_term = *self.term.read();
        let p = request.into_inner();
        let cmd = p.cmd().map_err(|e| {
            tonic::Status::invalid_argument(format!("propose cmd decode failed: {}", e))
        })?;
        self.spec
            .map_lock(|mut spec| {
                let is_conflict = Self::is_spec_conflict(&spec, &cmd);
                spec.push_back(cmd.clone());

                match (is_conflict, is_leader) {
                    // conflict and leader
                    (true, true) => async {
                        // the leader need to sync cmd
                        match self.sync_to_others(self_term, &cmd, true) {
                            Ok(notifier) => {
                                notifier.notify(1);
                                ProposeResponse::new_error(
                                    is_leader,
                                    self_term,
                                    &ProposeError::KeyConflict,
                                )
                            }
                            Err(_err) => ProposeResponse::new_error(
                                is_leader,
                                *self.term.read(),
                                &ProposeError::SyncedError("Sync cmd channel closed".to_owned()),
                            ),
                        }
                    }
                    .left_future()
                    .left_future(),
                    (true, false) => async {
                        ProposeResponse::new_error(is_leader, self_term, &ProposeError::KeyConflict)
                    }
                    .left_future()
                    .right_future(),
                    (false, true) => {
                        let notifier = match self.sync_to_others(self_term, &cmd, false) {
                            Ok(notifier) => Some(notifier),
                            Err(_) => None,
                        };
                        // only the leader executes the command in speculative pool
                        cmd.execute(self.cmd_executor.as_ref())
                            .map(|er| {
                                er.map_or_else(
                                    |err| {
                                        ProposeResponse::new_error(
                                            is_leader,
                                            self_term,
                                            &ProposeError::ExecutionError(err.to_string()),
                                        )
                                    },
                                    |rv| {
                                        // the leader need to sync cmd
                                        if let Some(n) = notifier {
                                            n.notify(1);
                                            ProposeResponse::new_result::<C>(
                                                is_leader, self_term, &rv,
                                            )
                                        } else {
                                            // TODO: this error should be reported ealier?
                                            ProposeResponse::new_error(
                                                is_leader,
                                                *self.term.read(),
                                                &ProposeError::SyncedError(
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
            .map(tonic::Response::new)
            .map_err(|err| tonic::Status::internal(format!("encode or decode error, {}", err)))
    }

    /// handle "wait synced" request
    async fn wait_synced(
        &self,
        request: tonic::Request<WaitSyncedRequest>,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status> {
        let ws = request.into_inner();
        let id = ws.id().map_err(|e| {
            tonic::Status::invalid_argument(format!("wait_synced id decode failed: {}", e))
        })?;

        let handle = match self.synced_events.map_lock(|mut events| events.remove(&id)) {
            Some(handle) => Ok(handle),
            None => Err(WaitSyncedResponse::new_error::<C>(&format!(
                "command {:?} is not in the event hashmap",
                &id
            ))),
        };
        let handle = match handle {
            Ok(h) => h,
            Err(resp) => {
                return Ok(tonic::Response::new(resp.map_err(|e| {
                    tonic::Status::internal(format!("encode or decode error, {}", e))
                })?))
            }
        };
        Ok(tonic::Response::new(
            match handle.0.await {
                Ok(wsr) => wsr,
                Err(e) => WaitSyncedResponse::new_error::<C>(&format!("sync task failed, {:?}", e)),
            }
            .map_err(|e| tonic::Status::internal(format!("encode or decode error, {}", e)))?,
        ))
    }

    /// handle "sync" request
    async fn sync(
        &self,
        request: tonic::Request<SyncRequest>,
    ) -> Result<tonic::Response<SyncResponse>, tonic::Status> {
        let sr = request.into_inner();

        self.log
            .map_lock(|mut log| {
                let local_len = log.len();
                match sr.index() {
                    t if t > local_len.numeric_cast() => {
                        Ok(SyncResponse::new_prev_not_ready(local_len.numeric_cast()))
                    }
                    t if t < local_len.numeric_cast() => {
                        // checked in the if condition
                        #[allow(clippy::unwrap_used)]
                        let entry: &LogEntry<C> = log.get(t.numeric_cast::<usize>()).unwrap();
                        Ok(SyncResponse::new_entry_not_empty(
                            entry.term(),
                            entry.cmd(),
                        )?)
                    }
                    _ => {
                        let self_term = *self.term.read();
                        if sr.term() < self_term {
                            Ok(SyncResponse::new_wrong_term(self_term))
                        } else {
                            let cmd: C = sr.cmd()?;
                            let id = cmd.id().clone();
                            log.push(LogEntry::new(sr.term(), cmd, EntryStatus::Unsynced));
                            // TODO: remove this workaround after commit feature is done.
                            let _skip = Self::spec_remove_cmd(&Arc::clone(&self.spec), &id);
                            Ok(SyncResponse::new_synced())
                        }
                    }
                }
            })
            .map(tonic::Response::new)
            .map_err(|e: bincode::Error| {
                tonic::Status::internal(format!("encode or decode error, {}", e))
            })
    }
}

impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Drop for Protocol<C, CE> {
    #[inline]
    fn drop(&mut self) {
        self.sm_handle.abort();
    }
}
