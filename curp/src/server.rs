use std::cmp::min;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    iter,
    sync::Arc,
    vec,
};

use clippy_utilities::NumericCast;
use event_listener::{Event, EventListener};
use futures::FutureExt;
use opentelemetry::global;
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use parking_lot::{Mutex, MutexGuard, RwLock};
use tokio::time::Instant;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, error, info, instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::rpc::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use crate::util::RwLockMap;
use crate::{
    bg_tasks::{run_bg_tasks, SyncCompleteMessage, SyncMessage},
    channel::{
        key_mpsc::{self, MpscKeyBasedSender},
        key_spmc::{self, SpmcKeybasedReceiver},
        SendError,
    },
    cmd::{Command, CommandExecutor, ProposeId},
    error::{ProposeError, ServerError},
    log::LogEntry,
    message::TermNum,
    rpc::{ProposeRequest, ProposeResponse, ProtocolServer, WaitSyncedRequest, WaitSyncedResponse},
    util::{ExtractMap, MutexMap},
};

/// Default server serving port
pub(crate) static DEFAULT_SERVER_PORT: u16 = 12345;

/// Default after sync task count
pub static DEFAULT_AFTER_SYNC_CNT: usize = 10;

/// The Rpc Server to handle rpc requests
/// This Wrapper is introduced due to the `MadSim` rpc lib
#[derive(Clone, Debug)]
pub struct Rpc<C: Command + 'static, CE: CommandExecutor<C> + 'static> {
    /// The inner server is wrapped in an Arc so that its state can be shared while cloning the rpc wrapper
    inner: Arc<Protocol<C, CE>>,
}

#[tonic::async_trait]
impl<C, CE> crate::rpc::Protocol for Rpc<C, CE>
where
    C: 'static + Command,
    CE: 'static + CommandExecutor<C>,
{
    #[instrument(skip(self), name = "server propose")]
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        Span::current().set_parent(global::get_text_map_propagator(|prop| {
            prop.extract(&ExtractMap(request.metadata()))
        }));
        self.inner.propose(request).await
    }

    #[instrument(skip(self), name = "server wait_synced")]
    async fn wait_synced(
        &self,
        request: tonic::Request<WaitSyncedRequest>,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status> {
        Span::current().set_parent(global::get_text_map_propagator(|prop| {
            prop.extract(&ExtractMap(request.metadata()))
        }));
        self.inner.wait_synced(request).await
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        self.inner.append_entries(request)
    }

    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        self.inner.vote(request)
    }
}

impl<C: Command + 'static, CE: CommandExecutor<C> + 'static> Rpc<C, CE> {
    /// New `Rpc`
    #[inline]
    pub fn new(id: &str, is_leader: bool, term: u64, others: Vec<String>, executor: CE) -> Self {
        Self {
            inner: Arc::new(Protocol::new(
                id,
                is_leader,
                term,
                others,
                executor,
                DEFAULT_AFTER_SYNC_CNT,
            )),
        }
    }

    /// Run a new rpc server
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[inline]
    pub async fn run(
        id: &str,
        is_leader: bool, // TODO: remove this option
        term: u64,
        others: Vec<String>,
        server_port: Option<u16>,
        executor: CE,
    ) -> Result<(), ServerError> {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        info!("RPC server {id} started, listening on port {port}");
        let server = Self::new(id, is_leader, term, others, executor);

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

    /// Run a new rpc server from a listener, designed to be used in the test
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[inline]
    pub async fn run_from_listener(
        id: &str,
        is_leader: bool,
        term: u64,
        others: Vec<String>,
        listener: TcpListener,
        executor: CE,
    ) -> Result<(), ServerError> {
        let server = Self {
            inner: Arc::new(Protocol::new(
                id,
                is_leader,
                term,
                others,
                executor,
                DEFAULT_AFTER_SYNC_CNT,
            )),
        };
        tonic::transport::Server::builder()
            .add_service(ProtocolServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await?;
        Ok(())
    }
}

/// The join handler of synced task
#[derive(Debug)]
struct SyncedTaskJoinHandle(JoinHandle<Result<WaitSyncedResponse, bincode::Error>>);

/// The state of a command in cmd watch board
enum CmdBoardState {
    /// Command need execute
    NeedExecute,
    /// Command need not execute
    NoExecute,
    /// Command gotten the final result
    FinalResult(Result<WaitSyncedResponse, bincode::Error>),
}

/// Command board value stored in the command board map
enum CmdBoardValue {
    /// There's an wait_sync request waiting
    EarlyArrive(Event),
    /// Wait for the sync procedure complete
    Wait4Sync(Box<(Event, CmdBoardState)>),
}

impl CmdBoardValue {
    /// Create an early arrive variat
    fn new_early_arrive(event: Event) -> Self {
        Self::EarlyArrive(event)
    }

    /// Create an wait sync variat
    fn new_wait_sync(event: Event, state: CmdBoardState) -> Self {
        Self::Wait4Sync(Box::new((event, state)))
    }
}

/// The server that handles client request and server consensus protocol
pub struct Protocol<C: Command + 'static, CE: CommandExecutor<C> + 'static> {
    /// Current state
    // TODO: apply fine-grain locking
    state: Arc<RwLock<State<C>>>,
    /// Last time a rpc is received
    last_rpc_time: Arc<RwLock<Instant>>,
    /// The speculative cmd pool, shared with executor
    spec: Arc<Mutex<VecDeque<C>>>,
    /// Command executor
    cmd_executor: Arc<CE>,
    /// The channel to send synced command to sync manager
    sync_chan: MpscKeyBasedSender<C::K, SyncMessage<C>>,
    /// Synced manager handler
    bg_handle: JoinHandle<()>,
    /// After sync tasks handle
    after_sync_handle: Vec<JoinHandle<()>>,
    // TODO: clean up the board when the size is too large
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: Arc<Mutex<HashMap<ProposeId, CmdBoardValue>>>,
}

/// State of the server
pub(crate) struct State<C: Command + 'static> {
    /// Id of the server
    pub(crate) id: String,
    /// Role of the server
    role: ServerRole,
    /// Current term
    pub(crate) term: TermNum,
    /// Consensus log
    pub(crate) log: Vec<LogEntry<C>>,
    /// Candidate id that received vote in current term
    pub(crate) voted_for: Option<String>,
    /// Votes received in the election
    pub(crate) votes_received: usize,
    /// Index of highest log entry known to be committed
    pub(crate) commit_index: usize,
    /// Index of highest log entry applied to state machine
    pub(crate) last_applied: usize,
    /// For each server, index of the next log entry to send to that server
    // TODO: this should be indexed by server id and changed into a vec for efficiency
    pub(crate) next_index: HashMap<String, usize>,
    /// For each server, index of highest log entry known to be replicated on server
    pub(crate) match_index: HashMap<String, usize>,
    /// Other server ids
    pub(crate) others: Vec<String>,
    /// Trigger when server role changes
    pub(crate) role_trigger: Arc<Event>,
    /// Trigger when there might be some logs to commit
    pub(crate) commit_trigger: Arc<Event>,
}

impl<C: Command + 'static> State<C> {
    /// Init server state
    pub(crate) fn new(id: &str, role: ServerRole, term: TermNum, others: Vec<String>) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        let others: Vec<String> = others
            .into_iter()
            .map(|other| format!("http://{}", other))
            .collect();
        for other in &others {
            assert!(next_index.insert(other.clone(), 1).is_none());
            assert!(match_index.insert(other.clone(), 0).is_none());
        }
        Self {
            id: format!("http://{}", id),
            role,
            term,
            log: vec![LogEntry::new(0, &[])], // a fake log[0] will simplify the boundary check significantly
            voted_for: None,
            votes_received: 0,
            commit_index: 0,
            last_applied: 0,
            next_index, // TODO: next_index should be initialized upon becoming a leader
            match_index,
            others,
            role_trigger: Arc::new(Event::new()),
            commit_trigger: Arc::new(Event::new()),
        }
    }

    /// Is leader?
    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.role, ServerRole::Leader)
    }

    /// Last log index
    #[allow(clippy::integer_arithmetic)] // log.len() >= 1 because we have a fake log[0]
    pub(crate) fn last_log_index(&self) -> usize {
        self.log.len() - 1
    }

    /// Last log term
    #[allow(dead_code, clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0]
    pub(crate) fn last_log_term(&self) -> TermNum {
        self.log[self.log.len() - 1].term()
    }

    /// Need to commit
    pub(crate) fn need_commit(&self) -> bool {
        self.last_applied < self.commit_index
    }

    /// Update to `term`
    pub(crate) fn update_to_term(&mut self, term: TermNum) {
        debug_assert!(self.term <= term);
        self.term = term;
        self.set_role(ServerRole::Follower);
        self.voted_for = None;
        self.votes_received = 0;
        debug!("updated to term {term}");
    }

    /// Set server role
    pub(crate) fn set_role(&mut self, role: ServerRole) {
        let prev_role = self.role;
        self.role = role;
        if prev_role != role {
            self.role_trigger.notify(usize::MAX);
        }
    }

    /// Get server role
    pub(crate) fn role(&self) -> ServerRole {
        self.role
    }

    /// Get role trigger
    pub(crate) fn role_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.role_trigger)
    }

    /// Get commit trigger
    pub(crate) fn commit_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.commit_trigger)
    }
}

impl<C, CE> Debug for Protocol<C, CE>
where
    C: Command,
    CE: CommandExecutor<C>,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.map_read(|state| {
            f.debug_struct("Server")
                .field("role", &state.role)
                .field("term", &state.term)
                .field("spec", &self.spec)
                .field("cmd_executor", &self.cmd_executor)
                .field("log", &state.log)
                .finish()
        })
    }
}

/// The server role same as Raft
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ServerRole {
    /// A follower
    Follower,
    /// A candidate
    Candidate,
    /// A leader
    Leader,
}

impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Protocol<C, CE> {
    /// Init `after_sync` tasks
    #[allow(clippy::too_many_lines)] // FIXME: refactor too long function
    fn init_after_sync_tasks(
        cnt: usize,
        comp_rx: &SpmcKeybasedReceiver<C::K, SyncCompleteMessage<C>>,
        spec: &Arc<Mutex<VecDeque<C>>>,
        cmd_board: &Arc<Mutex<HashMap<ProposeId, CmdBoardValue>>>,
        cmd_executor: &Arc<CE>,
    ) -> Vec<JoinHandle<()>> {
        iter::repeat((
            comp_rx.clone(),
            Arc::clone(spec),
            Arc::clone(cmd_board),
            Arc::clone(cmd_executor),
        ))
        .take(cnt)
        .map(|(rx, spec_clone, cmd_board_clone, dispatch_executor)| {
            tokio::spawn(async move {
                loop {
                    let sync_compl_result = rx.recv().await;

                    /// Call `after_sync` function. As async closure is still nightly, we
                    /// use macro here.
                    macro_rules! call_after_sync {
                        ($cmd: ident, $index: ident, $er: expr) => {
                            $cmd.after_sync(dispatch_executor.as_ref(), $index)
                                .map(move |after_sync_result| match after_sync_result {
                                    Ok(asr) => WaitSyncedResponse::new_success::<C>(&asr, &$er),
                                    Err(e) => WaitSyncedResponse::new_error::<C>(&format!(
                                        "after_sync execution error: {:?}",
                                        e
                                    )),
                                })
                                .await
                        };
                    }

                    let (after_sync_result, cmd_id) = match sync_compl_result {
                        Ok((reply, notifier)) => {
                            let (index, cmd): (_, Arc<C>) =
                                reply.map_msg(|csm| (csm.log_index(), csm.cmd()));

                            let cmd_id = cmd.id().clone();

                            let option = cmd_board_clone.map_lock(|board| {
                                match board.get(&cmd_id) {
                                    Some(value) => match *value {
                                        CmdBoardValue::Wait4Sync(ref event_state) => {
                                            match event_state.1 {
                                                CmdBoardState::NeedExecute => Some((true, false)),
                                                CmdBoardState::NoExecute => Some((false, false)),
                                                CmdBoardState::FinalResult(_) => {
                                                    // Should not hit this state, but just log it
                                                    error!("Should not get final result");
                                                    None
                                                }
                                            }
                                        }

                                        CmdBoardValue::EarlyArrive(_) => {
                                            error!("Should not get early arrive while executing");
                                            None
                                        }
                                    },
                                    None => Some((false, true)),
                                }
                            });
                            if let Some((need_execute, miss_entry)) = option {
                                let after_sync_result = if miss_entry {
                                    WaitSyncedResponse::new_error::<C>(&format!(
                                        "cmd {:?} is not to be waited",
                                        cmd_id
                                    ))
                                } else if need_execute {
                                    match cmd.execute(dispatch_executor.as_ref()).await {
                                        Ok(er) => call_after_sync!(cmd, index, Some(er)),
                                        Err(e) => WaitSyncedResponse::new_error::<C>(&format!(
                                            "cmd execution error: {:?}",
                                            e
                                        )),
                                    }
                                } else {
                                    call_after_sync!(cmd, index, None)
                                };

                                let _ignore = notifier.send(reply);
                                (after_sync_result, cmd_id)
                            } else {
                                continue;
                            }
                        }
                        // TODO: handle the sync task stop, usually we should stop working
                        Err(e) => unreachable!("sync manager stopped, {}", e),
                    };

                    cmd_board_clone.map_lock(|mut board| {
                        if let Some(CmdBoardValue::Wait4Sync(event_state)) = board.remove(&cmd_id) {
                            let entry = board.entry(cmd_id.clone());
                            let value = entry.or_insert_with(|| {
                                CmdBoardValue::new_wait_sync(
                                    event_state.0,
                                    CmdBoardState::FinalResult(after_sync_result),
                                )
                            });

                            if let CmdBoardValue::Wait4Sync(ref es) = *value {
                                es.0.notify(1);
                            }
                        }
                    });
                    if Self::spec_remove_cmd(&spec_clone, &cmd_id).is_none() {
                        unreachable!("{:?} should be in the spec pool", cmd_id);
                    }
                }
            })
        })
        .collect()
    }

    /// Create a new server instance
    #[must_use]
    #[inline]
    pub fn new(
        id: &str,
        is_leader: bool,
        term: u64,
        others: Vec<String>,
        cmd_executor: CE,
        after_sync_cnt: usize,
    ) -> Self {
        let (sync_tx, sync_rx) = key_mpsc::channel();
        let (comp_tx, comp_rx) = key_spmc::channel();
        let cmd_executor = Arc::new(cmd_executor);
        let cmd_board = Arc::new(Mutex::new(HashMap::new()));
        let spec = Arc::new(Mutex::new(VecDeque::new()));
        let last_rpc_time = Arc::new(RwLock::new(Instant::now()));

        let state = Arc::new(RwLock::new(State::new(
            id,
            if is_leader {
                ServerRole::Leader
            } else {
                ServerRole::Follower
            },
            term,
            others.clone(),
        )));

        // run background tasks
        let bg_handle = tokio::spawn(run_bg_tasks(
            Arc::clone(&state),
            Arc::clone(&last_rpc_time),
            sync_rx,
            comp_tx,
            Arc::clone(&cmd_executor),
            Arc::clone(&spec),
        ));

        let after_sync_handle =
            Self::init_after_sync_tasks(after_sync_cnt, &comp_rx, &spec, &cmd_board, &cmd_executor);

        Self {
            state,
            last_rpc_time,
            spec,
            cmd_executor,
            sync_chan: sync_tx,
            bg_handle,
            cmd_board,
            after_sync_handle,
        }
    }

    /// Check if the `cmd` conflicts with any conflicts in the speculative cmd pool
    fn is_spec_conflict(spec: &MutexGuard<'_, VecDeque<C>>, cmd: &C) -> bool {
        spec.iter().any(|spec_cmd| spec_cmd.is_conflict(cmd))
    }

    /// Check if the current server is leader
    fn is_leader(&self) -> bool {
        self.state.read().is_leader()
    }

    /// Remove command from the speculative cmd pool
    pub(crate) fn spec_remove_cmd(spec: &Arc<Mutex<VecDeque<C>>>, cmd_id: &ProposeId) -> Option<C> {
        debug!("remove cmd {:?} from spec pool", cmd_id);
        spec.map_lock(|mut spec_unlocked| {
            spec_unlocked
                .iter()
                .position(|s| s.id() == cmd_id)
                .map(|index| spec_unlocked.swap_remove_back(index))
        })
        .flatten()
    }

    /// Send sync event to the `SyncManager`, it's not a blocking function
    #[instrument(skip(self))]
    fn sync_to_others(
        &self,
        term: TermNum,
        cmd: &C,
        need_execute: bool,
    ) -> Result<Event, SendError> {
        self.cmd_board.map_lock(|mut board| {
            let old_value = board.remove(cmd.id());
            let _ignore = board.insert(
                cmd.id().clone(),
                CmdBoardValue::new_wait_sync(
                    Event::new(),
                    if need_execute {
                        CmdBoardState::NeedExecute
                    } else {
                        CmdBoardState::NoExecute
                    },
                ),
            );

            if let Some(CmdBoardValue::EarlyArrive(event)) = old_value {
                // FIXME: Maybe more than one waiting?
                event.notify(1);
            }
        });
        let ready_notify = self
            .sync_chan
            .send(cmd.keys(), SyncMessage::new(term, Arc::new(cmd.clone())))?;

        Ok(ready_notify)
    }

    /// Handle "propose" requests
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        let is_leader = self.is_leader();
        let self_term = self.state.read().term;
        let p = request.into_inner();
        let cmd = p.cmd().map_err(|e| {
            tonic::Status::invalid_argument(format!("propose cmd decode failed: {}", e))
        })?;
        self.spec
            .map_lock(|mut spec| {
                let is_conflict = Self::is_spec_conflict(&spec, &cmd);
                spec.push_back(cmd.clone());
                match (is_conflict, is_leader) {
                    // conflict and is leader
                    (true, true) => async {
                        // the leader needs to sync cmd
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
                                self.state.read().term,
                                &ProposeError::SyncedError("Sync cmd channel closed".to_owned()),
                            ),
                        }
                    }
                    .left_future()
                    .left_future(),
                    // conflict but not leader
                    (true, false) => async {
                        ProposeResponse::new_error(is_leader, self_term, &ProposeError::KeyConflict)
                    }
                    .left_future()
                    .right_future(),
                    // not conflict but is leader
                    (false, true) => {
                        let notifier = match self.sync_to_others(self_term, &cmd, false) {
                            Ok(notifier) => Some(notifier),
                            Err(_) => None,
                        };
                        // only the leader executes the command in speculative pool
                        cmd.execute(self.cmd_executor.as_ref())
                            .map(|er| {
                                if let Some(n) = notifier {
                                    n.notify(1);
                                } else {
                                    return ProposeResponse::new_error(
                                        is_leader,
                                        self.state.read().term,
                                        &ProposeError::SyncedError(
                                            "Sync cmd channel closed".to_owned(),
                                        ),
                                    );
                                }
                                er.map_or_else(
                                    |err| {
                                        ProposeResponse::new_error(
                                            is_leader,
                                            self_term,
                                            &ProposeError::ExecutionError(err.to_string()),
                                        )
                                    },
                                    |rv| {
                                        ProposeResponse::new_result::<C>(is_leader, self_term, &rv)
                                    },
                                )
                            })
                            .right_future()
                            .left_future()
                    }
                    // not conflict and is not leader
                    (false, false) => {
                        async { ProposeResponse::new_empty(false, self.state.read().term) }
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
        /// Get the final result or the listener.
        /// We wait for the listener and repeat the steps until we get the
        /// finale result.
        enum FrOrListener {
            /// Internal state telling that we've got a final result
            MetFr,
            /// The real final result
            Fr(Box<Result<tonic::Response<WaitSyncedResponse>, tonic::Status>>),
            /// The event listener
            Listener(EventListener),
        }

        let ws = request.into_inner();
        let id = ws.id().map_err(|e| {
            tonic::Status::invalid_argument(format!("wait_synced id decode failed: {}", e))
        })?;

        loop {
            let fr_or_listener = self.cmd_board.map_lock(|mut board| {
                let fr_or_listener = match board.get(&id) {
                    Some(value) => match *value {
                        CmdBoardValue::Wait4Sync(ref event_state) => match event_state.1 {
                            CmdBoardState::FinalResult(_) => FrOrListener::MetFr,
                            CmdBoardState::NeedExecute | CmdBoardState::NoExecute => {
                                FrOrListener::Listener(event_state.0.listen())
                            }
                        },
                        CmdBoardValue::EarlyArrive(ref event) => {
                            FrOrListener::Listener(event.listen())
                        }
                    },
                    None => {
                        let event = Event::new();
                        let listener = event.listen();
                        let _ignore =
                            board.insert(id.clone(), CmdBoardValue::new_early_arrive(event));
                        FrOrListener::Listener(listener)
                    }
                };

                if let FrOrListener::MetFr = fr_or_listener {
                    if let Some(CmdBoardValue::Wait4Sync(event_state)) = board.remove(&id) {
                        if let CmdBoardState::FinalResult(fr) = event_state.1 {
                            return FrOrListener::Fr(Box::new(
                                fr.map_err(|e| {
                                    tonic::Status::internal(format!(
                                        "encode or decode error, {}",
                                        e
                                    ))
                                })
                                .map(tonic::Response::new),
                            ));
                        }
                    }
                    unreachable!("cmd board for id {:?} should have had final result", id);
                }

                fr_or_listener
            });

            match fr_or_listener {
                FrOrListener::Fr(fr) => break *fr,
                FrOrListener::Listener(l) => l.await,
                FrOrListener::MetFr => {
                    unreachable!("EmptyFr is the internal state, should not appear here")
                }
            }
        }
    }

    /// Handle `AppendEntries` requests
    fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!("append_entries received: term({}), commit({}), prev_log_index({}), prev_log_term({}), {} entries", 
            req.term, req.leader_commit, req.prev_log_index, req.prev_log_term, req.entries.len());

        let state = self.state.upgradable_read();

        // calibrate term
        if req.term < state.term {
            return Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                state.term,
            )));
        }

        let mut state = RwLockUpgradableReadGuard::upgrade(state);
        if req.term > state.term {
            state.update_to_term(req.term);
        }

        *self.last_rpc_time.write() = Instant::now();

        // remove inconsistencies
        #[allow(clippy::integer_arithmetic)] // TODO: overflow of log index should be prevented
        state.log.truncate((req.prev_log_index + 1).numeric_cast());

        // check if previous log index match leader's one
        if state
            .log
            .get(req.prev_log_index.numeric_cast::<usize>())
            .map_or(false, |entry| entry.term() != req.prev_log_term)
        {
            return Ok(tonic::Response::new(AppendEntriesResponse::new_reject(
                state.term,
            )));
        }

        // append new logs
        let entries = req
            .entries()
            .map_err(|e| tonic::Status::internal(format!("encode or decode error, {}", e)))?;
        state.log.extend(entries.into_iter());

        // update commit index
        let prev_commit_index = state.commit_index;
        state.commit_index = min(req.leader_commit.numeric_cast(), state.last_log_index());
        if prev_commit_index != state.commit_index {
            debug!("commit_index updated to {}", state.commit_index);
            state.commit_trigger.notify(1);
        }

        Ok(tonic::Response::new(AppendEntriesResponse::new_accept(
            state.term,
        )))
    }

    /// Handle `Vote` requests
    #[allow(clippy::pedantic)] // need not return result, but to keep it consistent with rpc handler functions, we keep it this way
    fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(
            "vote received: term({}), last_log_index({}), last_log_term({}), id({})",
            req.term, req.last_log_index, req.last_log_term, req.candidate_id
        );

        let state = self.state.upgradable_read();
        if req.term < state.term || state.voted_for.is_some() {
            return Ok(tonic::Response::new(VoteResponse::new_reject(state.term)));
        }

        // If a follower receives votes from a candidate, it should update last_rpc_time to prevent itself from starting election
        *self.last_rpc_time.write() = Instant::now();

        let mut state = RwLockUpgradableReadGuard::upgrade(state);
        // calibrate term
        if req.term > state.term {
            state.update_to_term(req.term);
        }

        if req.last_log_term > state.last_log_term()
            || (req.last_log_term == state.last_log_term()
                && req.last_log_index.numeric_cast::<usize>() >= state.last_log_index())
        {
            debug!("vote for server {}", req.candidate_id);
            state.voted_for = Some(req.candidate_id);
            Ok(tonic::Response::new(VoteResponse::new_accept(state.term)))
        } else {
            Ok(tonic::Response::new(VoteResponse::new_reject(state.term)))
        }
    }
}

impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Drop for Protocol<C, CE> {
    #[inline]
    fn drop(&mut self) {
        self.bg_handle.abort();
        for h in &self.after_sync_handle {
            h.abort();
        }
    }
}
