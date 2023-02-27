use std::{collections::HashMap, fmt::Debug, path::Path, sync::Arc};

use tokio::{net::TcpListener, sync::broadcast};
use tokio_stream::wrappers::TcpListenerStream;
use tower::filter::FilterLayer;
use tracing::{info, instrument};
use utils::{config::ServerTimeout, tracing::Extract};

use self::curp_node::{CurpError, CurpNode};
use crate::{
    cmd::{Command, CommandExecutor},
    error::ServerError,
    message::ServerId,
    rpc::{
        AppendEntriesRequest, AppendEntriesResponse, FetchLeaderRequest, FetchLeaderResponse,
        ProposeRequest, ProposeResponse, ProtocolServer, VoteRequest, VoteResponse,
        WaitSyncedRequest, WaitSyncedResponse,
    },
    TxFilter,
};

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

/// Curp Node
mod curp_node;

/// Storage
mod storage;

/// Default server serving port
static DEFAULT_SERVER_PORT: u16 = 12345;

/// The Rpc Server to handle rpc requests
/// This Wrapper is introduced due to the `MadSim` rpc lib
#[derive(Clone, Debug)]
pub struct Rpc<C: Command + 'static> {
    /// The inner server is wrapped in an Arc so that its state can be shared while cloning the rpc wrapper
    inner: Arc<CurpNode<C>>,
}

#[tonic::async_trait]
impl<C: 'static + Command> crate::rpc::Protocol for Rpc<C> {
    #[instrument(skip(self), name = "server propose")]
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.propose(request.into_inner()).await?,
        ))
    }

    #[instrument(skip(self), name = "server wait_synced")]
    async fn wait_synced(
        &self,
        request: tonic::Request<WaitSyncedRequest>,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status> {
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.wait_synced(request.into_inner()).await?,
        ))
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.append_entries(request.into_inner())?,
        ))
    }

    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.vote(request.into_inner()).await?,
        ))
    }

    async fn fetch_leader(
        &self,
        request: tonic::Request<FetchLeaderRequest>,
    ) -> Result<tonic::Response<FetchLeaderResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.fetch_leader(request.into_inner())?,
        ))
    }
}

impl<C: Command + 'static> Rpc<C> {
    /// New `Rpc`
    ///
    /// # Panics
    /// Panic if storage creation failed
    #[inline]
    pub async fn new<CE: CommandExecutor<C> + 'static>(
        id: ServerId,
        is_leader: bool,
        others: HashMap<ServerId, String>,
        executor: CE,
        timeout: Arc<ServerTimeout>,
        tx_filter: Option<Box<dyn TxFilter>>,
        storage_path: impl AsRef<Path>,
    ) -> Self {
        #[allow(clippy::panic)]
        let curp_node = match CurpNode::new(
            id,
            is_leader,
            others,
            executor,
            timeout,
            tx_filter,
            storage_path,
        )
        .await
        {
            Ok(n) => n,
            Err(err) => {
                panic!("failed to create curp service {err}");
            }
        };

        Self {
            inner: Arc::new(curp_node),
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
        storage_path: impl AsRef<Path>,
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
        let server = Self::new(
            id,
            is_leader,
            others,
            executor,
            timeout,
            tx_filter,
            storage_path,
        )
        .await;

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
        storage_path: impl AsRef<Path>,
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
        let server = Self::new(
            id,
            is_leader,
            others,
            executor,
            timeout,
            tx_filter,
            storage_path,
        )
        .await;

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
        self.inner.leader_rx()
    }
}

impl From<CurpError> for tonic::Status {
    #[inline]
    fn from(err: CurpError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
