use std::{collections::HashMap, fmt::Debug, sync::Arc};

use clippy_utilities::NumericCast;
use event_listener::Event;
use parking_lot::{lock_api::RwLockUpgradableReadGuard, Mutex, RwLock};
use tokio::{net::TcpListener, sync::broadcast};
use tokio_stream::wrappers::TcpListenerStream;
use tower::filter::FilterLayer;
use tracing::{debug, error, info, instrument, warn};
use utils::{
    config::ServerTimeout,
    parking_lot_lock::{MutexMap, RwLockMap},
    tracing::Extract,
};

use self::{
    cmd_board::CommandBoard, cmd_worker::CmdExeSenderInterface, gc::run_gc_tasks,
    raw_curp::RawCurp, spec_pool::SpeculativePool,
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
    server::{
        cmd_board::CmdBoardRef,
        cmd_worker::{start_cmd_workers, CmdExeSender},
        spec_pool::SpecPoolRef,
    },
    TxFilter,
};

/// Background tasks of Curp protocol
mod bg_tasks;

/// Command worker to do execution and after sync
mod cmd_worker;

/// Raw Curp
mod raw_curp;

/// Command board is the buffer to store command execution result
mod cmd_board;

/// Speculative pool
mod spec_pool;

/// Background garbage collection for Curp server
mod gc;

/// Default server serving port
static DEFAULT_SERVER_PORT: u16 = 12345;

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
        timeout: Arc<ServerTimeout>,
        tx_filter: Option<Box<dyn TxFilter>>,
    ) -> Self {
        Self {
            inner: Arc::new(Protocol::new(
                id, is_leader, others, executor, timeout, tx_filter,
            )),
        }
    }

    /// Run a new rpc server
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub async fn run<CE, U, UE>(
        id: ServerId,
        is_leader: bool,
        others: HashMap<ServerId, String>,
        server_port: Option<u16>,
        executor: CE,
        timeout: Arc<ServerTimeout>,
        tx_filter: Option<Box<dyn TxFilter>>,
        rx_filter: Option<FilterLayer<U>>,
    ) -> Result<(), ServerError>
    where
        CE: 'static + CommandExecutor<C>,
        U: 'static
            + Send
            + Clone
            + FnMut(
                tonic::codegen::http::Request<tonic::transport::Body>,
            ) -> Result<tonic::codegen::http::Request<tonic::transport::Body>, UE>,
        UE: 'static + Send + Sync + std::error::Error,
    {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        info!("RPC server {id} started, listening on port {port}");
        let server = Self::new(id, is_leader, others, executor, timeout, tx_filter);

        if let Some(f) = rx_filter {
            tonic::transport::Server::builder()
                .layer(f)
                .add_service(ProtocolServer::new(server))
                .serve(
                    format!("0.0.0.0:{port}")
                        .parse()
                        .map_err(|e| ServerError::ParsingError(format!("{e}")))?,
                )
                .await?;
        } else {
            tonic::transport::Server::builder()
                .add_service(ProtocolServer::new(server))
                .serve(
                    format!("0.0.0.0:{port}")
                        .parse()
                        .map_err(|e| ServerError::ParsingError(format!("{e}")))?,
                )
                .await?;
        }
        Ok(())
    }

    /// Run a new rpc server from a listener, designed to be used in the tests
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub async fn run_from_listener<CE, U, UE>(
        id: ServerId,
        is_leader: bool,
        others: HashMap<ServerId, String>,
        listener: TcpListener,
        executor: CE,
        timeout: Arc<ServerTimeout>,
        tx_filter: Option<Box<dyn TxFilter>>,
        rx_filter: Option<FilterLayer<U>>,
    ) -> Result<(), ServerError>
    where
        CE: 'static + CommandExecutor<C>,
        U: 'static
            + Send
            + Clone
            + FnMut(
                tonic::codegen::http::Request<tonic::transport::Body>,
            ) -> Result<tonic::codegen::http::Request<tonic::transport::Body>, UE>,
        UE: 'static + Send + Sync + std::error::Error,
    {
        let server = Self {
            inner: Arc::new(Protocol::new(
                id, is_leader, others, executor, timeout, tx_filter,
            )),
        };
        if let Some(f) = rx_filter {
            tonic::transport::Server::builder()
                .layer(f)
                .add_service(ProtocolServer::new(server))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await?;
        } else {
            tonic::transport::Server::builder()
                .add_service(ProtocolServer::new(server))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await?;
        }
        Ok(())
    }

    /// Get a subscriber for leader changes
    #[inline]
    #[must_use]
    pub fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.inner.curp.leader_rx()
    }
}

/// The server that handles client request and server consensus protocol
#[derive(Debug)]
pub struct Protocol<C: Command + 'static> {
    /// Curp state machine
    curp: Arc<RawCurp<C>>,
    /// The speculative cmd pool, shared with executor
    spec_pool: SpecPoolRef<C>,
    /// The channel to send synced command to background sync task
    sync_tx: flume::Sender<SyncMessage<C>>,
    // TODO: clean up the board when the size is too large
    /// Cmd watch board for tracking the cmd sync results
    cmd_board: CmdBoardRef<C>,
    /// Stop channel sender
    shutdown_trigger: Arc<Event>,
    /// The channel to send cmds to background exe tasks
    cmd_exe_tx: CmdExeSender<C>,
    /// The curp server timeout
    timeout: Arc<ServerTimeout>,
}

/// The message sent to the background sync task
struct SyncMessage<C>
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
    fn new(term: TermNum, cmd: Arc<C>) -> Self {
        Self { term, cmd }
    }

    /// Get all values from the message
    fn inner(&self) -> (TermNum, Arc<C>) {
        (self.term, Arc::clone(&self.cmd))
    }
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
        timeout: Arc<ServerTimeout>,
        tx_filter: Option<Box<dyn TxFilter>>,
    ) -> Self {
        let (sync_tx, sync_rx) = flume::unbounded();
        let shutdown_trigger = Arc::new(Event::new());
        let cmd_board = Arc::new(RwLock::new(CommandBoard::new()));
        let spec_pool = Arc::new(Mutex::new(SpeculativePool::new()));

        // start cmd workers
        let exe_tx = start_cmd_workers(
            cmd_executor,
            Arc::clone(&spec_pool),
            Arc::clone(&cmd_board),
            Arc::clone(&shutdown_trigger),
        );

        // start curp state machine
        let curp = Arc::new(RawCurp::new(
            id,
            others.keys().cloned().collect(),
            is_leader,
            Arc::clone(&cmd_board),
            Arc::clone(&spec_pool),
            Arc::clone(&timeout),
            Box::new(exe_tx.clone()),
        ));

        // run background tasks
        let _bg_handle = tokio::spawn(bg_tasks::run_bg_tasks(
            Arc::clone(&curp),
            others,
            sync_rx,
            Arc::clone(&shutdown_trigger),
            tx_filter,
        ));

        run_gc_tasks(Arc::clone(&cmd_board), Arc::clone(&spec_pool));

        Self {
            curp,
            spec_pool,
            sync_tx,
            cmd_board,
            shutdown_trigger,
            cmd_exe_tx: exe_tx,
            timeout,
        }
    }

    /// Send sync event to the background sync task, it's not a blocking function
    #[instrument(skip(self))]
    fn sync_to_others(&self, term: TermNum, cmd: Arc<C>, needs_exe: bool) {
        if let Err(e) = self.sync_tx.send(SyncMessage::new(term, cmd)) {
            error!("send channel error, {e}");
        }
    }

    /// Handle "propose" requests
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        let p = request.into_inner();

        let cmd: C = p.cmd().map_err(|e| {
            tonic::Status::invalid_argument(format!("propose cmd decode failed: {e}"))
        })?;

        async {
            let (leader_id, term) = self.curp.leader();
            let is_leader = leader_id.as_ref() == Some(self.curp.id());
            let cmd = Arc::new(cmd);
            let er_rx = {
                let has_conflict = self
                    .spec_pool
                    .map_lock(|mut spec_l| spec_l.insert(Arc::clone(&cmd)).is_some());

                // non-leader should return immediately
                if !is_leader {
                    return if has_conflict {
                        ProposeResponse::new_error(leader_id, term, &ProposeError::KeyConflict)
                    } else {
                        ProposeResponse::new_empty(leader_id, term)
                    };
                }
                // leader should sync the cmd to others

                // leader needs dedup first, since a proposal might be sent to the leader twice
                let duplicated = self
                    .cmd_board
                    .map_write(|mut board_w| !board_w.sync.insert(cmd.id().clone()));
                if duplicated {
                    warn!("{:?} find duplicated cmd {:?}", self.curp.id(), cmd.id());
                    return ProposeResponse::new_error(leader_id, term, &ProposeError::Duplicated);
                }

                if has_conflict {
                    // no spec execute, just sync
                    self.sync_to_others(term, Arc::clone(&cmd), true);
                    return ProposeResponse::new_error(leader_id, term, &ProposeError::KeyConflict);
                }

                // spec execute and sync
                // execute the command before sync so that the order of cmd is preserved
                let er_rx = self.cmd_exe_tx.send_exe(Arc::clone(&cmd));
                self.cmd_board
                    .map_write(|mut cb_w| assert!(cb_w.spec_executed.insert(cmd.id().clone())));
                self.sync_to_others(term, Arc::clone(&cmd), false);

                // now we can release the lock and wait for the execution result
                er_rx
            };

            // wait for the speculative execution
            let er = er_rx.await;
            match er {
                Ok(Ok(er)) => ProposeResponse::new_result::<C>(leader_id, term, &er),
                Ok(Err(err)) => {
                    ProposeResponse::new_error(leader_id, term, &ProposeError::ExecutionError(err))
                }
                Err(err) => ProposeResponse::new_error(
                    leader_id,
                    term,
                    &ProposeError::ProtocolError(err.to_string()),
                ),
            }
        }
        .await
        .map_or_else(
            |err| {
                Err(tonic::Status::internal(format!(
                    "encode or decode error, {err}",
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
            tonic::Status::invalid_argument(format!("wait_synced id decode failed: {e}"))
        })?;

        debug!("{} get wait synced request for {id:?}", self.curp.id());
        let resp = loop {
            let listener = {
                // check if the server is still leader
                let (leader, term) = self.curp.leader();
                let is_leader = leader.as_ref() == Some(self.curp.id());
                if !is_leader {
                    warn!("non-leader server {} receives wait_synced", self.curp.id());
                    break WaitSyncedResponse::new_error(&SyncError::Redirect(leader, term));
                }

                // check if the cmd board already has response
                let board_r = self.cmd_board.upgradable_read();
                #[allow(clippy::pattern_type_mismatch)] // can't get away with this
                match (board_r.er_buffer.get(&id), board_r.asr_buffer.get(&id)) {
                    (Some(Err(err)), _) => {
                        break WaitSyncedResponse::new_error(&SyncError::ExecuteError(
                            err.to_string(),
                        ));
                    }
                    (Some(er), Some(asr)) => {
                        break WaitSyncedResponse::new_from_result::<C>(
                            Some(er.clone()),
                            Some(asr.clone()),
                        );
                    }
                    _ => {}
                }

                let mut board_w = RwLockUpgradableReadGuard::upgrade(board_r);
                // generate wait_synced event listener
                board_w
                    .notifiers
                    .entry(id.clone())
                    .or_insert_with(Event::new)
                    .listen()
            };
            let wait_synced_timeout = *self.timeout.wait_synced_timeout();
            if tokio::time::timeout(wait_synced_timeout, listener)
                .await
                .is_err()
            {
                let _ignored = self.cmd_board.write().notifiers.remove(&id);
                warn!("wait synced timeout for {id:?}");
                break WaitSyncedResponse::new_error(&SyncError::Timeout);
            }
        };

        debug!("{} wait synced for {id:?} finishes", self.curp.id());
        resp.map(tonic::Response::new)
            .map_err(|err| tonic::Status::internal(format!("encode or decode error, {err}")))
    }

    /// Handle `AppendEntries` requests
    fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();

        let entries = req
            .entries()
            .map_err(|e| tonic::Status::internal(format!("encode or decode error, {e}")))?;

        let result = self.curp.handle_append_entries(
            req.term,
            req.leader_id,
            req.prev_log_index.numeric_cast(),
            req.prev_log_term,
            entries,
            req.leader_commit.numeric_cast(),
        );
        let resp = match result {
            Ok(term) => AppendEntriesResponse::new_accept(term),
            Err((term, hint)) => AppendEntriesResponse::new_reject(term, hint),
        };

        Ok(tonic::Response::new(resp))
    }

    /// Handle `Vote` requests
    #[allow(clippy::pedantic)] // need not return result, but to keep it consistent with rpc handler functions, we keep it this way
    fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let req = request.into_inner();

        let result = self.curp.handle_vote(
            req.term,
            req.candidate_id,
            req.last_log_index.numeric_cast(),
            req.last_log_term,
        );
        let resp = match result {
            Ok((term, sp)) => VoteResponse::new_accept(term, sp)
                .map_err(|e| tonic::Status::internal(format!("can't serialize cmds, {e}")))?,
            Err(term) => VoteResponse::new_reject(term),
        };

        Ok(tonic::Response::new(resp))
    }

    /// Handle fetch leader requests
    #[allow(clippy::unnecessary_wraps, clippy::needless_pass_by_value)] // To keep type consistent with other request handlers
    fn fetch_leader(
        &self,
        _request: tonic::Request<FetchLeaderRequest>,
    ) -> Result<tonic::Response<FetchLeaderResponse>, tonic::Status> {
        let (leader_id, term) = self.curp.leader();
        Ok(tonic::Response::new(FetchLeaderResponse::new(
            leader_id, term,
        )))
    }
}

impl<C: 'static + Command> Drop for Protocol<C> {
    #[inline]
    fn drop(&mut self) {
        // TODO: async drop is still not supported by Rust(should wait for bg tasks to be stopped?), or we should create an async `stop` function for Protocol
        self.shutdown_trigger.notify(usize::MAX);
    }
}
