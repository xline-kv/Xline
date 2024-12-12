use std::{fmt::Debug, sync::Arc};

use engine::SnapshotAllocator;
use flume::r#async::RecvStream;
use tokio::sync::broadcast;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::instrument;
#[cfg(madsim)]
use utils::ClientTlsConfig;
use utils::{config::prelude::CurpConfig, task_manager::TaskManager, tracing::Extract};

use self::curp_node::CurpNode;
pub use self::{
    conflict::{spec_pool_new::SpObject, uncommitted_pool::UcpObject},
    raw_curp::RawCurp,
};
use crate::rpc::{OpResponse, RecordRequest, RecordResponse};
use crate::{
    cmd::{Command, CommandExecutor},
    members::{ClusterInfo, ServerId},
    role_change::RoleChange,
    rpc::{
        connect::Bypass, AppendEntriesRequest, AppendEntriesResponse, FetchClusterRequest,
        FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, LeaseKeepAliveMsg, MoveLeaderRequest,
        MoveLeaderResponse, ProposeConfChangeRequest, ProposeConfChangeResponse, ProposeRequest,
        PublishRequest, PublishResponse, ShutdownRequest, ShutdownResponse, TriggerShutdownRequest,
        TriggerShutdownResponse, TryBecomeLeaderNowRequest, TryBecomeLeaderNowResponse,
        VoteRequest, VoteResponse,
    },
};
use crate::{
    response::ResponseSender,
    rpc::{ReadIndexRequest, ReadIndexResponse},
};

/// Command worker to do execution and after sync
mod cmd_worker;

/// Raw Curp
mod raw_curp;

/// Command board is the buffer to store command execution result
mod cmd_board;

/// Conflict pools
pub mod conflict;

/// Background garbage collection for Curp server
mod gc;

/// Curp Node
mod curp_node;

/// Storage
mod storage;

/// Lease Manager
mod lease_manager;

/// Curp metrics
mod metrics;

pub use storage::{db::DB, StorageApi, StorageError};

/// The Rpc Server to handle rpc requests
///
/// This Wrapper is introduced due to the `MadSim` rpc lib
#[derive(Debug)]
pub struct Rpc<C: Command, CE: CommandExecutor<C>, RC: RoleChange> {
    /// The inner server is wrapped in an Arc so that its state can be shared while cloning the rpc wrapper
    inner: Arc<CurpNode<C, CE, RC>>,
}

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> Clone for Rpc<C, CE, RC> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[tonic::async_trait]
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> crate::rpc::Protocol for Rpc<C, CE, RC> {
    type ProposeStreamStream = RecvStream<'static, Result<OpResponse, tonic::Status>>;

    #[instrument(skip_all, name = "propose_stream")]
    async fn propose_stream(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<Self::ProposeStreamStream>, tonic::Status> {
        let bypassed = request.metadata().is_bypassed();
        let (tx, rx) = flume::bounded(2);
        let resp_tx = Arc::new(ResponseSender::new(tx));
        self.inner
            .propose_stream(&request.into_inner(), resp_tx, bypassed)
            .await?;

        Ok(tonic::Response::new(rx.into_stream()))
    }

    #[instrument(skip_all, name = "record")]
    async fn record(
        &self,
        request: tonic::Request<RecordRequest>,
    ) -> Result<tonic::Response<RecordResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.record(&request.into_inner())?,
        ))
    }

    #[instrument(skip_all, name = "read_index")]
    async fn read_index(
        &self,
        _request: tonic::Request<ReadIndexRequest>,
    ) -> Result<tonic::Response<ReadIndexResponse>, tonic::Status> {
        Ok(tonic::Response::new(self.inner.read_index()?))
    }

    #[instrument(skip_all, name = "curp_shutdown")]
    async fn shutdown(
        &self,
        request: tonic::Request<ShutdownRequest>,
    ) -> Result<tonic::Response<ShutdownResponse>, tonic::Status> {
        let bypassed = request.metadata().is_bypassed();
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.shutdown(request.into_inner(), bypassed).await?,
        ))
    }

    #[instrument(skip_all, name = "curp_propose_conf_change")]
    async fn propose_conf_change(
        &self,
        request: tonic::Request<ProposeConfChangeRequest>,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, tonic::Status> {
        let bypassed = request.metadata().is_bypassed();
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner
                .propose_conf_change(request.into_inner(), bypassed)
                .await?,
        ))
    }

    #[instrument(skip_all, name = "curp_publish")]
    async fn publish(
        &self,
        request: tonic::Request<PublishRequest>,
    ) -> Result<tonic::Response<PublishResponse>, tonic::Status> {
        let bypassed = request.metadata().is_bypassed();
        request.metadata().extract_span();
        Ok(tonic::Response::new(
            self.inner.publish(request.into_inner(), bypassed)?,
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

    #[instrument(skip_all, name = "curp_move_leader")]
    async fn move_leader(
        &self,
        request: tonic::Request<MoveLeaderRequest>,
    ) -> Result<tonic::Response<MoveLeaderResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.move_leader(request.into_inner()).await?,
        ))
    }

    #[instrument(skip_all, name = "lease_keep_alive")]
    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveMsg>>,
    ) -> Result<tonic::Response<LeaseKeepAliveMsg>, tonic::Status> {
        let req_stream = request.into_inner();
        Ok(tonic::Response::new(
            self.inner.lease_keep_alive(req_stream).await?,
        ))
    }
}

#[tonic::async_trait]
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> crate::rpc::InnerProtocol
    for Rpc<C, CE, RC>
{
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
            self.inner.vote(&request.into_inner())?,
        ))
    }

    #[instrument(skip_all, name = "curp_trigger_shutdown")]
    async fn trigger_shutdown(
        &self,
        request: tonic::Request<TriggerShutdownRequest>,
    ) -> Result<tonic::Response<TriggerShutdownResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.trigger_shutdown(*request.get_ref()),
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

    #[instrument(skip_all, name = "curp_try_become_leader_now")]
    async fn try_become_leader_now(
        &self,
        request: tonic::Request<TryBecomeLeaderNowRequest>,
    ) -> Result<tonic::Response<TryBecomeLeaderNowResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.inner.try_become_leader_now(request.get_ref()).await?,
        ))
    }
}

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> Rpc<C, CE, RC> {
    /// New `Rpc`
    ///
    /// # Panics
    ///
    /// Panic if storage creation failed
    #[inline]
    #[allow(clippy::too_many_arguments)] // TODO: refactor this use builder pattern
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        storage: Arc<DB<C>>,
        task_manager: Arc<TaskManager>,
        client_tls_config: Option<ClientTlsConfig>,
        sps: Vec<SpObject<C>>,
        ucps: Vec<UcpObject<C>>,
    ) -> Self {
        #[allow(clippy::panic)]
        let curp_node = match CurpNode::new(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            storage,
            task_manager,
            client_tls_config,
            sps,
            ucps,
        ) {
            Ok(n) => n,
            Err(err) => {
                panic!("failed to create curp service, {err:?}");
            }
        };

        Self {
            inner: Arc::new(curp_node),
        }
    }

    /// Run a new rpc server on a specific addr, designed to be used in the tests
    ///
    /// # Errors
    ///
    /// - `ServerError::ParsingError` if parsing failed for the local server address
    /// - `ServerError::RpcError` if any rpc related error met
    #[cfg(madsim)]
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub async fn run_from_addr(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        addr: std::net::SocketAddr,
        executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        storage: Arc<DB<C>>,
        task_manager: Arc<TaskManager>,
        client_tls_config: Option<ClientTlsConfig>,
        sps: Vec<SpObject<C>>,
        ucps: Vec<UcpObject<C>>,
    ) -> Result<(), crate::error::ServerError> {
        use utils::task_manager::tasks::TaskName;

        use crate::rpc::{InnerProtocolServer, ProtocolServer};

        let n = task_manager
            .get_shutdown_listener(TaskName::TonicServer)
            .unwrap_or_else(|| unreachable!("cluster should never shutdown before start"));
        let server = Self::new(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            storage,
            task_manager,
            client_tls_config,
            sps,
            ucps,
        );

        tonic::transport::Server::builder()
            .add_service(ProtocolServer::new(server.clone()))
            .add_service(InnerProtocolServer::new(server))
            .serve_with_shutdown(addr, n.wait())
            .await?;
        Ok(())
    }

    /// Get a subscriber for leader changes
    #[inline]
    #[must_use]
    pub fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.inner.leader_rx()
    }

    /// Get raw curp
    #[inline]
    #[must_use]
    pub fn raw_curp(&self) -> Arc<RawCurp<C, RC>> {
        self.inner.raw_curp()
    }
}
