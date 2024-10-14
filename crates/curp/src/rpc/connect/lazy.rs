use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use tonic::transport::Channel;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
#[cfg(madsim)]
use utils::ClientTlsConfig;

use crate::{
    rpc::{
        proto::{
            commandpb::protocol_client::ProtocolClient,
            inner_messagepb::inner_protocol_client::InnerProtocolClient,
        },
        AppendEntriesRequest, AppendEntriesResponse, ChangeMembershipRequest, CurpError,
        FetchMembershipRequest, FetchReadStateRequest, FetchReadStateResponse,
        InstallSnapshotResponse, MembershipResponse, MoveLeaderRequest, MoveLeaderResponse,
        OpResponse, ProposeRequest, ReadIndexResponse, RecordRequest, RecordResponse,
        ShutdownRequest, ShutdownResponse, VoteRequest, VoteResponse, WaitLearnerRequest,
        WaitLearnerResponse,
    },
    snapshot::Snapshot,
};

use super::{connect_to, Connect, ConnectApi, FromTonicChannel, InnerConnectApi};

/// A structure that lazily establishes a connection to a server.
pub(super) struct ConnectLazy<C> {
    // Configs
    /// The node id
    id: u64,
    /// Node addrs
    addrs: Vec<String>,
    /// The TLS config
    tls_config: Option<ClientTlsConfig>,

    /// The connection
    inner: tokio::sync::Mutex<Option<Connect<C>>>,
}

impl<C> ConnectLazy<C> {
    /// Lazily establishes a connection to the specified server.
    pub(super) fn connect(
        id: u64,
        addrs: Vec<String>,
        tls_config: Option<ClientTlsConfig>,
    ) -> Self {
        Self {
            id,
            addrs,
            tls_config,
            inner: tokio::sync::Mutex::new(None),
        }
    }
}

impl<C> ConnectLazy<C>
where
    C: FromTonicChannel,
{
    /// Establishes a connection if it does not already exist.
    fn connect_inner(&self, inner: &mut Option<Connect<C>>) {
        if inner.is_none() {
            let connect = connect_to(self.id, self.addrs.clone(), self.tls_config.clone());
            *inner = Some(connect);
        }
    }
}

#[allow(clippy::unwrap_used)]
#[async_trait]
impl InnerConnectApi for ConnectLazy<InnerProtocolClient<Channel>> {
    fn id(&self) -> u64 {
        self.id
    }

    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().update_addrs(addrs).await
    }

    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner
            .as_ref()
            .unwrap()
            .append_entries(request, timeout)
            .await
    }

    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().vote(request, timeout).await
    }

    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: u64,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner
            .as_ref()
            .unwrap()
            .install_snapshot(term, leader_id, snapshot)
            .await
    }

    async fn trigger_shutdown(&self) -> Result<(), tonic::Status> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().trigger_shutdown().await
    }

    async fn try_become_leader_now(&self, timeout: Duration) -> Result<(), tonic::Status> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().try_become_leader_now(timeout).await
    }
}

#[allow(clippy::unwrap_used)]
#[async_trait]
impl ConnectApi for ConnectLazy<ProtocolClient<Channel>> {
    fn id(&self) -> u64 {
        self.id
    }

    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().update_addrs(addrs).await
    }

    async fn propose_stream(
        &self,
        request: ProposeRequest,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<
        tonic::Response<Box<dyn Stream<Item = Result<OpResponse, tonic::Status>> + Send>>,
        CurpError,
    > {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner
            .as_ref()
            .unwrap()
            .propose_stream(request, token, timeout)
            .await
    }

    async fn record(
        &self,
        request: RecordRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RecordResponse>, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().record(request, timeout).await
    }

    async fn read_index(
        &self,
        timeout: Duration,
    ) -> Result<tonic::Response<ReadIndexResponse>, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().read_index(timeout).await
    }

    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().shutdown(request, timeout).await
    }

    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner
            .as_ref()
            .unwrap()
            .fetch_read_state(request, timeout)
            .await
    }

    async fn move_leader(
        &self,
        request: MoveLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<MoveLeaderResponse>, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().move_leader(request, timeout).await
    }

    async fn lease_keep_alive(&self, client_id: u64, interval: Duration) -> Result<u64, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner
            .as_ref()
            .unwrap()
            .lease_keep_alive(client_id, interval)
            .await
    }

    async fn fetch_membership(
        &self,
        request: FetchMembershipRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<MembershipResponse>, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner
            .as_ref()
            .unwrap()
            .fetch_membership(request, timeout)
            .await
    }

    async fn change_membership(
        &self,
        request: ChangeMembershipRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<MembershipResponse>, CurpError> {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner
            .as_ref()
            .unwrap()
            .change_membership(request, timeout)
            .await
    }

    async fn wait_learner(
        &self,
        request: WaitLearnerRequest,
        timeout: Duration,
    ) -> Result<
        tonic::Response<Box<dyn Stream<Item = Result<WaitLearnerResponse, tonic::Status>> + Send>>,
        CurpError,
    > {
        let mut inner = self.inner.lock().await;
        self.connect_inner(&mut inner);
        inner.as_ref().unwrap().wait_learner(request, timeout).await
    }
}
