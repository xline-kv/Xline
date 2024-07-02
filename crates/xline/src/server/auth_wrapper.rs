use std::sync::Arc;

use curp::{
    cmd::PbCodec,
    rpc::{
        FetchClusterRequest, FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
        LeaseKeepAliveMsg, MoveLeaderRequest, MoveLeaderResponse, OpResponse,
        ProposeConfChangeRequest, ProposeConfChangeResponse, ProposeRequest, Protocol,
        PublishRequest, PublishResponse, ReadIndexRequest, ReadIndexResponse, RecordRequest,
        RecordResponse, ShutdownRequest, ShutdownResponse,
    },
};
use flume::r#async::RecvStream;
use tracing::debug;
use xlineapi::command::Command;

use super::xline_server::CurpServer;
use crate::storage::AuthStore;

/// Auth wrapper
pub(crate) struct AuthWrapper {
    /// Curp server
    curp_server: CurpServer,
    /// Auth store
    auth_store: Arc<AuthStore>,
}

impl AuthWrapper {
    /// Create a new auth wrapper
    pub(crate) fn new(curp_server: CurpServer, auth_store: Arc<AuthStore>) -> Self {
        Self {
            curp_server,
            auth_store,
        }
    }
}

#[tonic::async_trait]
impl Protocol for AuthWrapper {
    type ProposeStreamStream = RecvStream<'static, Result<OpResponse, tonic::Status>>;

    async fn propose_stream(
        &self,
        mut request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<Self::ProposeStreamStream>, tonic::Status> {
        debug!(
            "AuthWrapper received propose request: {}",
            request.get_ref().propose_id()
        );
        if let Some(auth_info) = self.auth_store.try_get_auth_info_from_request(&request)? {
            let mut command: Command = request
                .get_ref()
                .cmd()
                .map_err(|e| tonic::Status::internal(e.to_string()))?;
            command.set_auth_info(auth_info);
            request.get_mut().command = command.encode();
        };
        self.curp_server.propose_stream(request).await
    }

    async fn record(
        &self,
        request: tonic::Request<RecordRequest>,
    ) -> Result<tonic::Response<RecordResponse>, tonic::Status> {
        self.curp_server.record(request).await
    }

    async fn read_index(
        &self,
        request: tonic::Request<ReadIndexRequest>,
    ) -> Result<tonic::Response<ReadIndexResponse>, tonic::Status> {
        self.curp_server.read_index(request).await
    }

    async fn shutdown(
        &self,
        request: tonic::Request<ShutdownRequest>,
    ) -> Result<tonic::Response<ShutdownResponse>, tonic::Status> {
        self.curp_server.shutdown(request).await
    }

    async fn propose_conf_change(
        &self,
        request: tonic::Request<ProposeConfChangeRequest>,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, tonic::Status> {
        self.curp_server.propose_conf_change(request).await
    }

    async fn publish(
        &self,
        request: tonic::Request<PublishRequest>,
    ) -> Result<tonic::Response<PublishResponse>, tonic::Status> {
        self.curp_server.publish(request).await
    }

    async fn fetch_cluster(
        &self,
        request: tonic::Request<FetchClusterRequest>,
    ) -> Result<tonic::Response<FetchClusterResponse>, tonic::Status> {
        self.curp_server.fetch_cluster(request).await
    }

    async fn fetch_read_state(
        &self,
        request: tonic::Request<FetchReadStateRequest>,
    ) -> Result<tonic::Response<FetchReadStateResponse>, tonic::Status> {
        self.curp_server.fetch_read_state(request).await
    }

    async fn move_leader(
        &self,
        request: tonic::Request<MoveLeaderRequest>,
    ) -> Result<tonic::Response<MoveLeaderResponse>, tonic::Status> {
        self.curp_server.move_leader(request).await
    }

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveMsg>>,
    ) -> Result<tonic::Response<LeaseKeepAliveMsg>, tonic::Status> {
        self.curp_server.lease_keep_alive(request).await
    }
}
