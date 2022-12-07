use std::{
    cmp::{min, Ordering},
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
};

use clippy_utilities::NumericCast;
use event_listener::Event;
use lock_utils::parking_lot_lock::RwLockMap;
use opentelemetry::global;
use parking_lot::{lock_api::RwLockUpgradableReadGuard, Mutex, RwLock};
use tokio::{net::TcpListener, sync::broadcast, time::Instant};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, error, info, instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use self::{
    cmd_board::{CmdState, CommandBoard},
    gc::run_gc_tasks,
    spec_pool::SpeculativePool,
    state::State,
};
use crate::{
    cmd::{Command, CommandExecutor},
    error::{ProposeError, ServerError},
    message::{ServerId, TermNum},
    rpc::{
        AppendEntriesRequest, AppendEntriesResponse, FetchLeaderRequest, FetchLeaderResponse,
        ProposeRequest, ProposeResponse, ProtocolServer, SyncError, VoteRequest, VoteResponse,
        WaitSyncedRequest, WaitSyncedResponse,
    },
    server::cmd_execute_worker::{cmd_exe_channel, CmdExeSender},
    shutdown::Shutdown,
    util::ExtractMap,
};

/// Background tasks of Curp protocol
mod bg_tasks;

/// Command execute worker
mod cmd_execute_worker;

/// Server state
mod state;

/// Command board is the buffer to store command execution result
mod cmd_board;

/// Speculative pool
mod spec_pool;

/// Background garbage collection for Curp server
mod gc;

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

    async fn fetch_leader(
        &self,
        request: tonic::Request<FetchLeaderRequest>,
    ) -> Result<tonic::Response<FetchLeaderResponse>, tonic::Status> {
        self.inner.fetch_leader(request)
    }
}

impl<C: Command + 'static> Rpc<C> {
    /// New `Rpc`
    #[inline]
    pub fn new<CE: CommandExecutor<C> + 'static>(
        id: ServerId,
        is_leader: bool,
        others: HashMap<ServerId, String>,
        executor: CE,
    ) -> Self {
        Self {
            inner: Arc::new(Protocol::new(id, is_leader, others, executor)),
        }
    }

    /// Run a new rpc server
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[inline]
    pub async fn run<CE: CommandExecutor<C> + 'static>(
        id: ServerId,
        is_leader: bool, // TODO: remove this option
        others: HashMap<ServerId, String>,
        server_port: Option<u16>,
        executor: CE,
    ) -> Result<(), ServerError> {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        info!("RPC server {id} started, listening on port {port}");
        let server = Self::new(id, is_leader, others, executor);

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
        id: ServerId,
        is_leader: bool,
        others: HashMap<ServerId, String>,
        listener: TcpListener,
        executor: CE,
    ) -> Result<(), ServerError> {
        let server = Self {
            inner: Arc::new(Protocol::new(id, is_leader, others, executor)),
        };
        tonic::transport::Server::builder()
            .add_service(ProtocolServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await?;
        Ok(())
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
    sync_chan: flume::Sender<SyncMessage<C>>,
    // TODO: clean up the board when the size is too large
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: Arc<Mutex<CommandBoard>>,
    /// Stop channel sender
    stop_ch_tx: broadcast::Sender<()>,
    /// The channel to send cmds to background exe tasks
    cmd_exe_tx: CmdExeSender<C>,
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
    fn inner(&self) -> (TermNum, Arc<C>) {
        (self.term, Arc::clone(&self.cmd))
    }
}

impl<C: Command> Debug for Protocol<C> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.map_read(|state| {
            f.debug_struct("Server")
                .field("role", &state.role())
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
        id: ServerId,
        is_leader: bool,
        others: HashMap<ServerId, String>,
        cmd_executor: CE,
    ) -> Self {
        let (sync_tx, sync_rx) = flume::unbounded();
        let cmd_board = Arc::new(Mutex::new(CommandBoard::new()));
        let spec = Arc::new(Mutex::new(SpeculativePool::new()));
        let last_rpc_time = Arc::new(RwLock::new(Instant::now()));
        let (stop_ch_tx, stop_ch_rx) = broadcast::channel(1);
        let (exe_tx, exe_rx) = cmd_exe_channel();

        let state = Arc::new(RwLock::new(State::new(
            id,
            if is_leader {
                ServerRole::Leader
            } else {
                ServerRole::Follower
            },
            others,
            Arc::clone(&cmd_board),
            Arc::clone(&last_rpc_time),
        )));

        // run background tasks
        let _bg_handle = tokio::spawn(bg_tasks::run_bg_tasks(
            Arc::clone(&state),
            sync_rx,
            cmd_executor,
            Arc::clone(&spec),
            exe_tx.clone(),
            exe_rx,
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
    fn sync_to_others(&self, term: TermNum, cmd: Arc<C>, need_execute: bool) {
        let mut cmd_board = self.cmd_board.lock();
        let _ignore = cmd_board.cmd_states.insert(
            cmd.id().clone(),
            if need_execute {
                CmdState::Execute
            } else {
                CmdState::AfterSync
            },
        );
        if let Err(e) = self.sync_chan.send(SyncMessage::new(term, cmd)) {
            error!("send channel error, {e}");
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
            let cmd = Arc::new(cmd);
            let er_rx = {
                let mut spec = self.spec.lock();

                // check if the command is ready
                if spec.ready.contains_key(cmd.id()) {
                    return ProposeResponse::new_empty(false, term);
                }

                let has_conflict = spec.has_conflict_with(&cmd);
                if !has_conflict {
                    spec.push(cmd.as_ref().clone());
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
                if has_conflict {
                    // no spec execute, just sync
                    self.sync_to_others(term, Arc::clone(&cmd), true);
                    return ProposeResponse::new_error(is_leader, term, &ProposeError::KeyConflict);
                }

                // spec execute and sync
                // execute the command before sync so that the order of cmd is preserved
                let er_rx = self.cmd_exe_tx.send_exe(Arc::clone(&cmd));
                self.sync_to_others(term, Arc::clone(&cmd), false);

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
                // check if the server is still leader
                let state = self.state.read();
                if !state.is_leader() {
                    return WaitSyncedResponse::new_error(&SyncError::Redirect(
                        state.leader_id.clone(),
                        state.term,
                    ))
                    .map_or_else(
                        |err| {
                            Err(tonic::Status::internal(format!(
                                "encode or decode error, {}",
                                err
                            )))
                        },
                        |resp| Ok(tonic::Response::new(resp)),
                    );
                }

                // check if the cmd board already has response
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

                // generate wait_synced event listener
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
        if state.leader_id.is_none() {
            state.leader_id = Some(req.leader_id.clone());
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

    /// Handle fetch leader requests
    #[allow(clippy::unnecessary_wraps, clippy::needless_pass_by_value)] // To keep type consistent with other request handlers
    fn fetch_leader(
        &self,
        _request: tonic::Request<FetchLeaderRequest>,
    ) -> Result<tonic::Response<FetchLeaderResponse>, tonic::Status> {
        let state = self.state.read();
        let leader_id = state.leader_id.clone();
        let term = state.term;
        Ok(tonic::Response::new(FetchLeaderResponse::new(
            leader_id, term,
        )))
    }
}

impl<C: 'static + Command> Drop for Protocol<C> {
    #[inline]
    fn drop(&mut self) {
        // TODO: async drop is still not supported by Rust(should wait for bg tasks to be stopped?), or we should create an async `stop` function for Protocol
        let _ = self.stop_ch_tx.send(()).ok();
    }
}
