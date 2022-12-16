use std::{
    cmp::{min, Ordering},
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::Arc,
    vec,
};

use clippy_utilities::NumericCast;
use event_listener::Event;
use parking_lot::{lock_api::RwLockUpgradableReadGuard, Mutex, RwLock};
use tokio::{net::TcpListener, sync::broadcast, time::Instant};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, error, info, instrument};

use crate::{
    bg_tasks::{run_bg_tasks, SyncMessage},
    channel::key_mpsc::{self, MpscKeyBasedSender},
    cmd::{Command, CommandExecutor, ProposeId},
    cmd_board::{CmdState, CommandBoard},
    cmd_execute_worker::{cmd_execute_channel, CmdExecuteSender},
    error::{ProposeError, ServerError},
    gc::run_gc_tasks,
    log::LogEntry,
    message::TermNum,
    rpc::{
        AppendEntriesRequest, AppendEntriesResponse, ProposeRequest, ProposeResponse,
        ProtocolServer, VoteRequest, VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
    shutdown::Shutdown,
    util::{Extract, RwLockMap},
};

/// Default server serving port
pub(crate) static DEFAULT_SERVER_PORT: u16 = 12345;

/// The Rpc Server to handle rpc requests
/// This Wrapper is introduced due to the `MadSim` rpc lib
#[derive(Clone, Debug)]
pub struct Rpc<C: Command + 'static> {
    /// The inner server is wrapped in an Arc so that its state can be shared while cloning the rpc wrapper
    inner: Arc<Protocol<C>>,
}

#[tonic::async_trait]
impl<C: 'static + Command> crate::rpc::Protocol for Rpc<C> {
    #[instrument(skip(self), name = "server propose")]
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        request.metadata().extract_span();
        self.inner.propose(request).await
    }

    #[instrument(skip(self), name = "server wait_synced")]
    async fn wait_synced(
        &self,
        request: tonic::Request<WaitSyncedRequest>,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status> {
        request.metadata().extract_span();
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

impl<C: Command + 'static> Rpc<C> {
    /// New `Rpc`
    #[inline]
    pub fn new<CE: CommandExecutor<C> + 'static>(
        id: &str,
        is_leader: bool,
        term: u64,
        others: Vec<String>,
        executor: CE,
    ) -> Self {
        Self {
            inner: Arc::new(Protocol::new(id, is_leader, term, others, executor)),
        }
    }

    /// Run a new rpc server
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[inline]
    pub async fn run<CE: CommandExecutor<C> + 'static>(
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
    pub async fn run_from_listener<CE: CommandExecutor<C> + 'static>(
        id: &str,
        is_leader: bool,
        term: u64,
        others: Vec<String>,
        listener: TcpListener,
        executor: CE,
    ) -> Result<(), ServerError> {
        let server = Self {
            inner: Arc::new(Protocol::new(id, is_leader, term, others, executor)),
        };
        tonic::transport::Server::builder()
            .add_service(ProtocolServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await?;
        Ok(())
    }
}

/// The speculative pool that stores commands that might be executed speculatively
#[derive(Debug)]
pub(crate) struct SpeculativePool<C> {
    /// Store
    pub(crate) pool: VecDeque<C>,
    /// Store the ids of commands that have completed backend syncing, but not in the local speculative pool. It'll prevent the late arrived commands.
    pub(crate) ready: HashMap<ProposeId, Instant>,
}

impl<C: Command + 'static> SpeculativePool<C> {
    /// Create a new speculative pool
    pub(crate) fn new() -> Self {
        Self {
            pool: VecDeque::new(),
            ready: HashMap::new(),
        }
    }

    /// Push a new command into spec pool if it has not been marked ready
    pub(crate) fn push(&mut self, cmd: C) {
        if self.ready.remove(cmd.id()).is_none() {
            debug!("insert cmd {:?} to spec pool", cmd.id());
            self.pool.push_back(cmd);
        }
    }

    /// Check whether the command pool has conflict with the new command
    pub(crate) fn has_conflict_with(&self, cmd: &C) -> bool {
        self.pool.iter().any(|spec_cmd| spec_cmd.is_conflict(cmd))
    }

    /// Try to remove the command from spec pool and mark it ready.
    /// There could be no such command in the following situations:
    /// * When the proposal arrived, the command conflicted with speculative pool and was not stored in it.
    /// * The command has committed. But the fast round proposal has not arrived at the client or failed to arrive.
    /// To prevent the server from returning error in the second situation when the fast proposal finally arrives, we mark the command ready.
    pub(crate) fn mark_ready(&mut self, cmd_id: &ProposeId) {
        debug!("remove cmd {:?} from spec pool", cmd_id);
        if self
            .pool
            .iter()
            .position(|s| s.id() == cmd_id)
            .and_then(|index| self.pool.swap_remove_back(index))
            .is_none()
        {
            debug!("Cmd {:?} is marked ready", cmd_id);
            assert!(
                self.ready.insert(cmd_id.clone(), Instant::now()).is_none(),
                "Cmd {:?} is already in ready pool",
                cmd_id
            );
        };
    }
}

/// The server that handles client request and server consensus protocol
pub struct Protocol<C: Command + 'static> {
    /// Current state
    // TODO: apply fine-grain locking
    state: Arc<RwLock<State<C>>>,
    /// Last time a rpc is received
    last_rpc_time: Arc<RwLock<Instant>>,
    /// The speculative cmd pool, shared with executor
    spec: Arc<Mutex<SpeculativePool<C>>>,
    /// The channel to send synced command to background sync task
    sync_chan: MpscKeyBasedSender<C::K, SyncMessage<C>>,
    // TODO: clean up the board when the size is too large
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: Arc<Mutex<CommandBoard>>,
    /// Stop channel sender
    stop_ch_tx: broadcast::Sender<()>,
    /// The channel to send cmds to background exe tasks
    cmd_exe_tx: CmdExecuteSender<C>,
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
    /// Trigger when a new leader needs to calibrate its followers
    pub(crate) calibrate_trigger: Arc<Event>,
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
            calibrate_trigger: Arc::new(Event::new()),
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

impl<C: Command> Debug for Protocol<C> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.map_read(|state| {
            f.debug_struct("Server")
                .field("role", &state.role)
                .field("term", &state.term)
                .field("spec", &self.spec)
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

impl<C: 'static + Command> Protocol<C> {
    /// Create a new server instance
    #[must_use]
    #[inline]
    pub fn new<CE: CommandExecutor<C> + 'static>(
        id: &str,
        is_leader: bool,
        term: u64,
        others: Vec<String>,
        cmd_executor: CE,
    ) -> Self {
        let (sync_tx, sync_rx) = key_mpsc::channel();
        let cmd_board = Arc::new(Mutex::new(CommandBoard::new()));
        let spec = Arc::new(Mutex::new(SpeculativePool::new()));
        let last_rpc_time = Arc::new(RwLock::new(Instant::now()));
        let (stop_ch_tx, stop_ch_rx) = broadcast::channel(1);
        let (exe_tx, exe_rx) = cmd_execute_channel();

        let state = Arc::new(RwLock::new(State::new(
            id,
            if is_leader {
                ServerRole::Leader
            } else {
                ServerRole::Follower
            },
            term,
            others,
        )));

        // run background tasks
        let _bg_handle = tokio::spawn(run_bg_tasks(
            Arc::clone(&state),
            Arc::clone(&last_rpc_time),
            sync_rx,
            cmd_executor,
            Arc::clone(&spec),
            exe_tx.clone(),
            exe_rx,
            Arc::clone(&cmd_board),
            Shutdown::new(stop_ch_rx.resubscribe()),
        ));

        run_gc_tasks(Arc::clone(&spec));

        Self {
            state,
            last_rpc_time,
            spec,
            sync_chan: sync_tx,
            cmd_board,
            stop_ch_tx,
            cmd_exe_tx: exe_tx,
        }
    }

    /// Send sync event to the background sync task, it's not a blocking function
    #[instrument(skip(self))]
    fn sync_to_others(&self, term: TermNum, cmd: &C, need_execute: bool) {
        let mut cmd_board = self.cmd_board.lock();
        let _ignore = cmd_board.cmd_states.insert(
            cmd.id().clone(),
            if need_execute {
                CmdState::Execute
            } else {
                CmdState::AfterSync
            },
        );
        let ready_notify = self
            .sync_chan
            .send(cmd.keys(), SyncMessage::new(term, Arc::new(cmd.clone())));
        match ready_notify {
            Err(e) => error!("sync channel has closed, {}", e),
            Ok(notify) => notify.notify(1), // TODO: shall we remove this mechanism to hold msgs since it's no longer needed?
        }
    }

    /// Handle "propose" requests
    // TODO: dedup proposed commands
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        let p = request.into_inner();

        let cmd: C = p.cmd().map_err(|e| {
            tonic::Status::invalid_argument(format!("propose cmd decode failed: {}", e))
        })?;

        (|| async {
            let (is_leader, term) = self.state.map_read(|state| (state.is_leader(), state.term));
            let er_rx = {
                let mut spec = self.spec.lock();

                // check if the command is ready
                if spec.ready.contains_key(cmd.id()) {
                    return ProposeResponse::new_empty(false, term);
                }

                let has_conflict = spec.has_conflict_with(&cmd);
                if !has_conflict {
                    spec.push(cmd.clone());
                }

                // non-leader should return immediately
                if !is_leader {
                    return if has_conflict {
                        ProposeResponse::new_error(is_leader, term, &ProposeError::KeyConflict)
                    } else {
                        ProposeResponse::new_empty(false, term)
                    };
                }

                // leader should sync the cmd to others
                let cmd = Arc::new(cmd);

                if has_conflict {
                    // no spec execute, just sync
                    self.sync_to_others(term, cmd.as_ref(), true);
                    return ProposeResponse::new_error(is_leader, term, &ProposeError::KeyConflict);
                }

                // spec execute and sync
                // execute the command before sync so that the order of cmd is preserved
                let er_rx = self.cmd_exe_tx.send_exe(Arc::clone(&cmd));
                self.sync_to_others(term, cmd.as_ref(), false);

                // now we can release the lock and wait for the execution result
                er_rx
            };

            // wait for the speculative execution
            let er = er_rx.await;
            match er {
                Ok(Ok(er)) => ProposeResponse::new_result::<C>(is_leader, term, &er),
                Ok(Err(err)) => ProposeResponse::new_error(
                    is_leader,
                    term,
                    &ProposeError::ExecutionError(err.to_string()),
                ),
                Err(err) => ProposeResponse::new_error(
                    is_leader,
                    term,
                    &ProposeError::ProtocolError(err.to_string()),
                ),
            }
        })()
        .await
        .map_or_else(
            |err| {
                Err(tonic::Status::internal(format!(
                    "encode or decode error, {}",
                    err
                )))
            },
            |resp| Ok(tonic::Response::new(resp)),
        )
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

        loop {
            let listener = {
                let mut cmd_board = self.cmd_board.lock();
                let entry = cmd_board
                    .cmd_states
                    .entry(id.clone())
                    .or_insert(CmdState::EarlyArrive);
                if let CmdState::FinalResponse(ref resp) = *entry {
                    #[allow(clippy::shadow_unrelated)] // clippy false positive
                    return resp.as_ref().map_or_else(
                        |err| {
                            Err(tonic::Status::internal(format!(
                                "encode or decode error, {}",
                                err
                            )))
                        },
                        |resp| Ok(tonic::Response::new(resp.clone())),
                    );
                }
                cmd_board
                    .notifiers
                    .entry(id.clone())
                    .or_insert_with(Event::new)
                    .listen()
            };
            listener.await;
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
                state.commit_index,
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
                state.commit_index,
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

        // just grab a write lock because it's highly likely that term is updated and a vote is granted
        let mut state = self.state.write();

        // calibrate term
        match req.term.cmp(&state.term) {
            Ordering::Less => {
                return Ok(tonic::Response::new(VoteResponse::new_reject(state.term)));
            }
            Ordering::Equal => {}
            Ordering::Greater => {
                state.update_to_term(req.term);
            }
        }

        if let Some(id) = state.voted_for.as_ref() {
            if id != &req.candidate_id {
                return Ok(tonic::Response::new(VoteResponse::new_reject(state.term)));
            }
        }

        // If a follower receives votes from a candidate, it should update last_rpc_time to prevent itself from starting election
        if state.role() == ServerRole::Follower {
            *self.last_rpc_time.write() = Instant::now();
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

impl<C: 'static + Command> Drop for Protocol<C> {
    #[inline]
    fn drop(&mut self) {
        // TODO: async drop is still not supported by Rust(should wait for bg tasks to be stopped?), or we should create an async `stop` function for Protocol
        let _ = self.stop_ch_tx.send(()).ok();
    }
}
