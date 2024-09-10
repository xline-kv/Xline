use std::time::Duration;

use async_trait::async_trait;
use event_listener::Event;
use futures::Stream;

use crate::{
    members::ServerId,
    rpc::{
        connect::ConnectApi, AddLearnerRequest, AddLearnerResponse, AddMemberRequest,
        AddMemberResponse, CurpError, FetchClusterRequest, FetchClusterResponse,
        FetchMembershipRequest, FetchMembershipResponse, FetchReadStateRequest,
        FetchReadStateResponse, MoveLeaderRequest, MoveLeaderResponse, OpResponse, ProposeRequest,
        PublishRequest, PublishResponse, ReadIndexResponse, RecordRequest, RecordResponse,
        RemoveLearnerRequest, RemoveLearnerResponse, RemoveMemberRequest, RemoveMemberResponse,
        ShutdownRequest, ShutdownResponse,
    },
};

/// Auto reconnect of a connection
pub(super) struct Reconnect<C> {
    /// Connect id
    id: ServerId,
    /// The connection
    connect: tokio::sync::RwLock<Option<C>>,
    /// The connect builder
    builder: Box<dyn Fn() -> C + Send + Sync + 'static>,
    /// Signal to abort heartbeat
    event: Event,
}

impl<C: ConnectApi> Reconnect<C> {
    /// Creates a new `Reconnect`
    pub(crate) fn new(builder: Box<dyn Fn() -> C + Send + Sync + 'static>) -> Self {
        let init_connect = builder();
        Self {
            id: init_connect.id(),
            connect: tokio::sync::RwLock::new(Some(init_connect)),
            builder,
            event: Event::new(),
        }
    }

    /// Creating a new connection to replace the current
    async fn reconnect(&self) {
        let new_connect = (self.builder)();
        // Cancel the leader keep alive loop task because it hold a read lock
        let _cancel = self.event.notify(1);
        let _ignore = self.connect.write().await.replace(new_connect);
    }

    /// Try to reconnect if the result is `Err`
    async fn try_reconnect<R>(&self, result: Result<R, CurpError>) -> Result<R, CurpError> {
        // TODO: use `tonic::Status` instead of `CurpError`, we can't tell
        // if a reconnect is required from `CurpError`.
        if matches!(
            result,
            Err(CurpError::RpcTransport(()) | CurpError::Internal(_))
        ) {
            tracing::info!("client reconnecting");
            self.reconnect().await;
        }
        result
    }
}

/// Execute with reconnect
macro_rules! execute_with_reconnect {
    ($self:expr, $trait_method:path, $($arg:expr),*) => {{
        let result = {
            let connect = $self.connect.read().await;
            let connect_ref = connect.as_ref().unwrap();
            ($trait_method)(connect_ref, $($arg),*).await
        };
        $self.try_reconnect(result).await
    }};
}

#[allow(clippy::unwrap_used, clippy::unwrap_in_result)]
#[async_trait]
impl<C: ConnectApi> ConnectApi for Reconnect<C> {
    /// Get server id
    fn id(&self) -> ServerId {
        self.id
    }

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
        let connect = self.connect.read().await;
        connect.as_ref().unwrap().update_addrs(addrs).await
    }

    /// Send `ProposeRequest`
    async fn propose_stream(
        &self,
        request: ProposeRequest,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<
        tonic::Response<Box<dyn Stream<Item = Result<OpResponse, tonic::Status>> + Send>>,
        CurpError,
    > {
        execute_with_reconnect!(self, ConnectApi::propose_stream, request, token, timeout)
    }

    /// Send `RecordRequest`
    async fn record(
        &self,
        request: RecordRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RecordResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::record, request, timeout)
    }

    /// Send `ReadIndexRequest`
    async fn read_index(
        &self,
        timeout: Duration,
    ) -> Result<tonic::Response<ReadIndexResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::read_index, timeout)
    }

    /// Send `PublishRequest`
    async fn publish(
        &self,
        request: PublishRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<PublishResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::publish, request, timeout)
    }

    /// Send `ShutdownRequest`
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::shutdown, request, timeout)
    }

    /// Send `FetchClusterRequest`
    async fn fetch_cluster(
        &self,
        request: FetchClusterRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchClusterResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::fetch_cluster, request, timeout)
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::fetch_read_state, request, timeout)
    }

    /// Send `MoveLeaderRequest`
    async fn move_leader(
        &self,
        request: MoveLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<MoveLeaderResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::move_leader, request, timeout)
    }

    /// Keep send lease keep alive to server and mutate the client id
    async fn lease_keep_alive(&self, client_id: u64, interval: Duration) -> Result<u64, CurpError> {
        let connect = self.connect.read().await;
        let connect_ref = connect.as_ref().unwrap();
        tokio::select! {
            result = connect_ref.lease_keep_alive(client_id, interval) => result,
            _empty = self.event.listen() => Err(CurpError::RpcTransport(())),
        }
    }

    /// Fetches the membership
    async fn fetch_membership(
        &self,
        request: FetchMembershipRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchMembershipResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::fetch_membership, request, timeout)
    }

    /// Add a learner to the cluster.
    async fn add_learner(
        &self,
        request: AddLearnerRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AddLearnerResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::add_learner, request, timeout)
    }

    /// Remove a learner from the cluster.
    async fn remove_learner(
        &self,
        request: RemoveLearnerRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RemoveLearnerResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::remove_learner, request, timeout)
    }

    /// Add a learner to the cluster.
    async fn add_member(
        &self,
        request: AddMemberRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AddMemberResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::add_member, request, timeout)
    }

    /// Remove a learner from the cluster.
    async fn remove_member(
        &self,
        request: RemoveMemberRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RemoveMemberResponse>, CurpError> {
        execute_with_reconnect!(self, ConnectApi::remove_member, request, timeout)
    }
}
