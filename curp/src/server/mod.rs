use std::{fmt::Debug, sync::Arc};

use engine::SnapshotAllocator;
#[cfg(not(madsim))]
use tokio::net::TcpListener;
use tokio::sync::broadcast;
#[cfg(not(madsim))]
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(not(madsim))]
use tracing::info;
use tracing::instrument;
use utils::{config::CurpConfig, shutdown, tracing::Extract};

use self::curp_node::CurpNode;
use crate::{
    cmd::{Command, CommandExecutor},
    error::ServerError,
    members::{ClusterInfo, ServerId},
    role_change::RoleChange,
    rpc::{
        AppendEntriesRequest, AppendEntriesResponse, FetchClusterRequest, FetchClusterResponse,
        FetchReadStateRequest, FetchReadStateResponse, InnerProtocolServer, InstallSnapshotRequest,
        InstallSnapshotResponse, ProposeConfChangeRequest, ProposeConfChangeResponse,
        ProposeRequest, ProposeResponse, ProtocolServer, PublishRequest, PublishResponse,
        ShutdownRequest, ShutdownResponse, TriggerShutdownRequest, TriggerShutdownResponse,
        VoteRequest, VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
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
#[cfg(not(madsim))]
static DEFAULT_SERVER_PORT: u16 = 12345;

/// The Rpc Server to handle rpc requests
/// This Wrapper is introduced due to the `MadSim` rpc lib
#[derive(Debug)]
pub struct Rpc<C: Command, RC: RoleChange> {
    /// The inner server is wrapped in an Arc so that its state can be shared while cloning the rpc wrapper
    inner: Arc<CurpNode<C, RC>>,
}

impl<C: Command, RC: RoleChange> Clone for Rpc<C, RC> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[tonic::async_trait]
impl<C: Command, RC: RoleChange> crate::rpc::Protocol for Rpc<C, RC> {
    #[instrument(skip_all, name = "curp_propose")]
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.propose(request.into_inner()).await?,
        ))
    }

    #[instrument(skip_all, name = "curp_shutdown")]
    async fn shutdown(
        &self,
        request: tonic::Request<ShutdownRequest>,
    ) -> Result<tonic::Response<ShutdownResponse>, tonic::Status> {
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.shutdown(request.into_inner()).await?,
        ))
    }

    #[instrument(skip_all, name = "curp_propose_conf_change")]
    async fn propose_conf_change(
        &self,
        request: tonic::Request<ProposeConfChangeRequest>,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, tonic::Status> {
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.propose_conf_change(request.into_inner()).await?,
        ))
    }

    #[instrument(skip_all, name = "curp_publish")]
    async fn publish(
        &self,
        request: tonic::Request<PublishRequest>,
    ) -> Result<tonic::Response<PublishResponse>, tonic::Status> {
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.publish(request.into_inner())?,
        ))
    }

    #[instrument(skip_all, name = "curp_wait_synced")]
    async fn wait_synced(
        &self,
        request: tonic::Request<WaitSyncedRequest>,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status> {
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.wait_synced(request.into_inner()).await?,
        ))
    }

    #[instrument(skip_all, name = "curp_fetch_cluster")]
    async fn fetch_cluster(
        &self,
        request: tonic::Request<FetchClusterRequest>,
    ) -> Result<tonic::Response<FetchClusterResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.fetch_cluster(request.into_inner())?,
        ))
    }

    #[instrument(skip_all, name = "curp_fetch_read_state")]
    async fn fetch_read_state(
        &self,
        request: tonic::Request<FetchReadStateRequest>,
    ) -> Result<tonic::Response<FetchReadStateResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.fetch_read_state(request.into_inner())?,
        ))
    }
}

#[tonic::async_trait]
impl<C: Command, RC: RoleChange> crate::rpc::InnerProtocol for Rpc<C, RC> {
    #[instrument(skip_all, name = "curp_append_entries")]
    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.append_entries(request.get_ref())?,
        ))
    }

    #[instrument(skip_all, name = "curp_vote")]
    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.vote(request.into_inner()).await?,
        ))
    }

    #[instrument(skip_all, name = "curp_trigger_shutdown")]
    async fn trigger_shutdown(
        &self,
        request: tonic::Request<TriggerShutdownRequest>,
    ) -> Result<tonic::Response<TriggerShutdownResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.trigger_shutdown(request.get_ref()),
        ))
    }

    #[instrument(skip_all, name = "curp_install_snapshot")]
    async fn install_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<InstallSnapshotRequest>>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        let req_stream = request.into_inner();
        Ok(tonic::Response::new(
            self.inner.install_snapshot(req_stream).await?,
        ))
    }
}

impl<C: Command, RC: RoleChange> Rpc<C, RC> {
    /// New `Rpc`
    ///
    /// # Panics
    /// Panic if storage creation failed
    #[inline]
    pub async fn new<CE: CommandExecutor<C>>(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        executor: CE,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        shutdown_trigger: shutdown::Trigger,
    ) -> Self {
        #[allow(clippy::panic)]
        let curp_node = match CurpNode::new(
            cluster_info,
            is_leader,
            Arc::new(executor),
            snapshot_allocator,
            role_change,
            curp_cfg,
            shutdown_trigger,
        )
        .await
        {
            Ok(n) => n,
            Err(err) => {
                panic!("failed to create curp service, {err:?}");
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
    #[cfg(not(madsim))]
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub async fn run<CE>(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        server_port: Option<u16>,
        executor: CE,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        shutdown_trigger: shutdown::Trigger,
    ) -> Result<(), ServerError>
    where
        CE: CommandExecutor<C>,
    {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        let id = cluster_info.self_id();
        info!("RPC server {id} started, listening on port {port}");
        let server = Self::new(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            shutdown_trigger,
        )
        .await;

        tonic::transport::Server::builder()
            .add_service(ProtocolServer::new(server.clone()))
            .add_service(InnerProtocolServer::new(server))
            .serve(
                format!("0.0.0.0:{port}")
                    .parse()
                    .map_err(|e| ServerError::ParsingError(format!("{e}")))?,
            )
            .await?;

        Ok(())
    }

    /// Run a new rpc server from a listener, designed to be used in the tests
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[cfg(not(madsim))]
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub async fn run_from_listener<CE>(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        listener: TcpListener,
        executor: CE,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        shutdown_trigger: shutdown::Trigger,
    ) -> Result<(), ServerError>
    where
        CE: CommandExecutor<C>,
    {
        use tonic::transport::{Identity, ServerTlsConfig};
        use utils::certs;

        let mut shutdown_listener = shutdown_trigger.subscribe();
        let server = Self::new(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            shutdown_trigger,
        )
        .await;
        let tls_config = ServerTlsConfig::new().identity(Identity::from_pem(
            certs::server_cert(),
            certs::server_key(),
        ));
        tonic::transport::Server::builder()
            .tls_config(tls_config)?
            .add_service(ProtocolServer::new(server.clone()))
            .add_service(InnerProtocolServer::new(server))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                shutdown_listener.wait_self_shutdown().await;
            })
            .await?;

        Ok(())
    }

    /// Run a new rpc server on a specific addr, designed to be used in the tests
    ///
    /// # Errors
    ///   `ServerError::ParsingError` if parsing failed for the local server address
    ///   `ServerError::RpcError` if any rpc related error met
    #[cfg(madsim)]
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub async fn run_from_addr<CE>(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        addr: std::net::SocketAddr,
        executor: CE,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        shutdown_trigger: shutdown::Trigger,
    ) -> Result<(), ServerError>
    where
        CE: CommandExecutor<C>,
    {
        let mut shutdown_listener = shutdown_trigger.subscribe();
        let server = Self::new(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            shutdown_trigger,
        )
        .await;

        tonic::transport::Server::builder()
            .add_service(ProtocolServer::new(server.clone()))
            .add_service(InnerProtocolServer::new(server))
            .serve_with_shutdown(addr, async move {
                shutdown_listener.wait_self_shutdown().await;
            })
            .await?;
        Ok(())
    }

    /// Get a subscriber for leader changes
    #[inline]
    #[must_use]
    pub fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.inner.leader_rx()
    }

    /// Get a shutdown listener
    #[must_use]
    #[inline]
    pub fn shutdown_listener(&self) -> shutdown::Listener {
        self.inner.shutdown_listener()
    }
}
