use std::{fmt::Debug, sync::Arc};

use curp_external_api::cmd::{ConflictCheck, ProposeId};
use engine::SnapshotAllocator;
use serde::{Deserialize, Serialize};
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
        ProposeRequest, ProposeResponse, ProtocolServer, ShutdownRequest, ShutdownResponse,
        VoteRequest, VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
    ConfChangeEntry, PublishRequest, PublishResponse,
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
#[derive(Clone, Debug)]
pub struct Rpc<C: Command + 'static, RC: RoleChange + 'static> {
    /// The inner server is wrapped in an Arc so that its state can be shared while cloning the rpc wrapper
    inner: Arc<CurpNode<C, RC>>,
}

#[tonic::async_trait]
impl<C: 'static + Command, RC: RoleChange + 'static> crate::rpc::Protocol for Rpc<C, RC> {
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
impl<C: 'static + Command, RC: RoleChange + 'static> crate::rpc::InnerProtocol for Rpc<C, RC> {
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

impl<C: Command + 'static, RC: RoleChange + 'static> Rpc<C, RC> {
    /// New `Rpc`
    ///
    /// # Panics
    /// Panic if storage creation failed
    #[inline]
    pub async fn new<CE: CommandExecutor<C> + 'static>(
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
        CE: 'static + CommandExecutor<C>,
    {
        let port = server_port.unwrap_or(DEFAULT_SERVER_PORT);
        let id = cluster_info.self_id();
        info!("RPC server {id} started, listening on port {port}");
        let server = Arc::new(
            Self::new(
                cluster_info,
                is_leader,
                executor,
                snapshot_allocator,
                role_change,
                curp_cfg,
                shutdown_trigger,
            )
            .await,
        );

        tonic::transport::Server::builder()
            .add_service(ProtocolServer::from_arc(Arc::clone(&server)))
            .add_service(InnerProtocolServer::from_arc(server))
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
        CE: 'static + CommandExecutor<C>,
    {
        let mut shutdown_listener = shutdown_trigger.subscribe();
        let server = Arc::new(
            Self::new(
                cluster_info,
                is_leader,
                executor,
                snapshot_allocator,
                role_change,
                curp_cfg,
                shutdown_trigger,
            )
            .await,
        );

        tonic::transport::Server::builder()
            .add_service(ProtocolServer::from_arc(Arc::clone(&server)))
            .add_service(InnerProtocolServer::from_arc(server))
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
        CE: 'static + CommandExecutor<C>,
    {
        let mut shutdown_listener = shutdown_trigger.subscribe();
        let server = Arc::new(
            Self::new(
                cluster_info,
                is_leader,
                executor,
                snapshot_allocator,
                role_change,
                curp_cfg,
                shutdown_trigger,
            )
            .await,
        );

        tonic::transport::Server::builder()
            .add_service(ProtocolServer::from_arc(Arc::clone(&server)))
            .add_service(InnerProtocolServer::from_arc(server))
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

/// Entry of speculative pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PoolEntry<C> {
    /// Command entry
    Command(Arc<C>),
    /// ConfChange entry
    ConfChange(ConfChangeEntry),
}

impl<C> PoolEntry<C>
where
    C: Command,
{
    /// Propose id
    pub(crate) fn id(&self) -> ProposeId {
        match *self {
            Self::Command(ref cmd) => cmd.id(),
            Self::ConfChange(ref conf_change) => conf_change.id(),
        }
    }

    /// Check if the entry is conflict with the command
    pub(crate) fn is_conflict_with_cmd(&self, c: &C) -> bool {
        match *self {
            Self::Command(ref cmd) => cmd.is_conflict(c),
            Self::ConfChange(ref _conf_change) => true,
        }
    }
}

impl<C> ConflictCheck for PoolEntry<C>
where
    C: ConflictCheck,
{
    fn is_conflict(&self, other: &Self) -> bool {
        let Self::Command(ref cmd1) = *self else {
            return true;
        };
        let Self::Command(ref cmd2) = *other else {
            return true;
        };
        cmd1.is_conflict(cmd2)
    }
}

impl<C> From<Arc<C>> for PoolEntry<C> {
    fn from(value: Arc<C>) -> Self {
        Self::Command(value)
    }
}

impl<C> From<ConfChangeEntry> for PoolEntry<C> {
    fn from(value: ConfChangeEntry) -> Self {
        Self::ConfChange(value)
    }
}
