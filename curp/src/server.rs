use std::{collections::VecDeque, fmt::Debug, net::SocketAddr, ops::Not, sync::Arc};

use madsim::net::Endpoint;
use parking_lot::{Mutex, RwLock};

use crate::{
    cmd::{Command, CommandExecutor, ProposeId},
    error::ServerError,
    log::LogEntry,
    message::{Propose, ProposeError, ProposeResponse},
    util::MutexMap,
};

/// Default server serving port
pub(crate) static DEFAULT_SERVER_PORT: u16 = 12345;

/// The Rpc Server to handle rpc requests
#[derive(Clone, Debug)]
pub struct RpcServerWrap<C: Command + 'static, CE: CommandExecutor<C> + 'static> {
    /// The inner server wrappped in an Arc so that we share state while clone the rpc wrapper
    inner: Arc<Server<C, CE>>,
}

#[allow(missing_docs)]
#[madsim::service]
impl<C: Command + 'static, CE: CommandExecutor<C> + 'static> RpcServerWrap<C, CE> {
    /// handle Propose
    #[rpc]
    async fn propose(&self, p: Propose<C>) -> ProposeResponse<C> {
        self.inner.propose(p).await
    }

    /// Run a new rpc server
    #[allow(dead_code)]
    async fn run(
        is_leader: bool,
        term: u64,
        others: &[SocketAddr],
        server_port: Option<u16>,
    ) -> Result<(), ServerError> {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        let rx_ep = Endpoint::bind(format!("0.0.0.0:{port}")).await?;
        let tx_ep = Endpoint::bind("127.0.0.1:0").await?;
        let server = Self {
            inner: Arc::new(Server::new(is_leader, term, tx_ep, others)),
        };
        Self::serve_on(server, rx_ep)
            .await
            .map_err(|e| ServerError::RpcServiceError(format!("{e}")))
    }
}

/// The server that handles client request and server consensus protocol
pub struct Server<C: Command + 'static, CE: CommandExecutor<C> + 'static> {
    /// the server role
    #[allow(dead_code)]
    role: RwLock<ServerRole>,
    /// Current term
    term: RwLock<u64>,
    /// The speculative cmd pool, shared with executor
    spec: Arc<Mutex<VecDeque<C>>>,
    /// The pending cmd pool, shared with executor
    pending: Arc<Mutex<VecDeque<C>>>,
    /// Command executor
    cmd_executor: CE,
    /// Consensus log
    #[allow(dead_code)]
    log: RwLock<Vec<LogEntry<C>>>,
    /// Other server address
    others: RwLock<Vec<SocketAddr>>,
    /// Self endpoint
    #[allow(dead_code)]
    ep: Endpoint,
}

impl<C, CE> Debug for Server<C, CE>
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
            .field("pending", &self.pending)
            .field("cmd_executor", &self.cmd_executor)
            .field("log", &self.log)
            .field("others", &self.others)
            // .field("ep", &self.ep) `Endpoint` missing debug impl
            .finish()
    }
}

// fn block_on<F: Future>(fut: F) -> F::Output {
//     match tokio::runtime::Handle::try_current() {
//         Err(_) => tokio::runtime::Runtime::new().block_on(fut),
//         Ok(handle) => handle.block_on(fut),
//     }
// }

/// The server role same as Raft
#[derive(Debug, Clone, Copy)]
enum ServerRole {
    /// The follower
    Follower,
    /// The candidater
    #[allow(dead_code)]
    Candidater,
    /// The leader
    Leader,
}

impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Server<C, CE> {
    /// Create a new server instance
    #[must_use]
    #[inline]
    pub fn new(is_leader: bool, term: u64, self_ep: Endpoint, others: &[SocketAddr]) -> Self {
        Self {
            role: RwLock::new(if is_leader {
                ServerRole::Leader
            } else {
                ServerRole::Follower
            }),
            term: RwLock::new(term),
            spec: Arc::new(Mutex::new(VecDeque::new())),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            cmd_executor: CE::new(),
            log: RwLock::new(Vec::new()),
            others: RwLock::new(others.to_vec()),
            ep: self_ep,
        }
    }

    /// Check if the `cmd` conflict with any conflicts in the speculative cmd pool
    fn add_spec(&self, cmd: &C) -> bool {
        self.spec.map(|mut spec| {
            let can_insert = spec
                .iter()
                .map(|spec_cmd| spec_cmd.is_conflict(cmd))
                .any(|has| has)
                .not();

            if can_insert {
                spec.push_back(cmd.clone());
            }

            can_insert
        })
    }

    /// Remove command from the speculative cmd pool
    #[allow(dead_code)]
    fn spec_remove_cmd(&self, cmd_id: &ProposeId) -> Option<C> {
        self.spec
            .map(|mut spec| {
                spec.iter()
                    .position(|s| s.id() == cmd_id)
                    .map(|index| spec.swap_remove_back(index))
            })
            .flatten()
    }

    /// Propose request handler
    async fn propose(&self, p: Propose<C>) -> ProposeResponse<C> {
        if self.add_spec(p.cmd()) {
            let role = *self.role.read();
            if let ServerRole::Leader = role {
                // only the leader executes the command in speculative pool
                p.cmd().execute(&self.cmd_executor).await.map_or_else(
                    |err| {
                        ProposeResponse::new_error(
                            true,
                            *self.term.read(),
                            ProposeError::ExecutionError(err.to_string()),
                        )
                    },
                    |er| ProposeResponse::new_ok(true, *self.term.read(), er),
                )
            } else {
                ProposeResponse::new_empty(false, *self.term.read())
            }
        } else {
            // only the leader puts the command in the pending list
            let is_leader = if let ServerRole::Leader = *self.role.read() {
                self.pending.map(|mut pending| {
                    pending.push_back(p.cmd().clone());
                });
                true
            } else {
                false
            };
            ProposeResponse::new_error(is_leader, *self.term.read(), ProposeError::KeyConflict)
        }
    }
}
