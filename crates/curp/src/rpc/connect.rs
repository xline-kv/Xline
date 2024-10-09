use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    ops::Deref,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use async_stream::stream;
use async_trait::async_trait;
use bytes::BytesMut;
use clippy_utilities::NumericCast;
use engine::SnapshotApi;
use futures::Stream;
#[cfg(test)]
use mockall::automock;
use tokio::sync::Mutex;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, instrument};
#[cfg(madsim)]
use utils::ClientTlsConfig;
use utils::{build_endpoint, tracing::Inject};

use crate::{
    members::ServerId,
    rpc::{
        proto::{
            commandpb::protocol_client::ProtocolClient,
            inner_messagepb::inner_protocol_client::InnerProtocolClient,
        },
        AppendEntriesRequest, AppendEntriesResponse, CurpError, FetchClusterRequest,
        FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, LeaseKeepAliveMsg, MoveLeaderRequest,
        MoveLeaderResponse, ProposeConfChangeRequest, ProposeConfChangeResponse, ProposeRequest,
        Protocol, PublishRequest, PublishResponse, ShutdownRequest, ShutdownResponse,
        TriggerShutdownRequest, TryBecomeLeaderNowRequest, VoteRequest, VoteResponse,
    },
    snapshot::Snapshot,
};

use super::{
    proto::commandpb::{ReadIndexRequest, ReadIndexResponse},
    reconnect::Reconnect,
    OpResponse, RecordRequest, RecordResponse,
};

/// Install snapshot chunk size: 64KB
const SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

/// The default buffer size for rpc connection
const DEFAULT_BUFFER_SIZE: usize = 1024;

/// For protocol client
trait FromTonicChannel {
    /// New from channel
    fn from_channel(channel: Channel) -> Self;
}

impl FromTonicChannel for ProtocolClient<Channel> {
    fn from_channel(channel: Channel) -> Self {
        ProtocolClient::new(channel)
    }
}

impl FromTonicChannel for InnerProtocolClient<Channel> {
    fn from_channel(channel: Channel) -> Self {
        InnerProtocolClient::new(channel)
    }
}

/// Creates a new connection
fn connect_to<Client: FromTonicChannel>(
    id: ServerId,
    addrs: Vec<String>,
    tls_config: Option<ClientTlsConfig>,
) -> Connect<Client> {
    let (channel, change_tx) = Channel::balance_channel(DEFAULT_BUFFER_SIZE.max(addrs.len()));
    for addr in &addrs {
        let endpoint = build_endpoint(addr, tls_config.as_ref())
            .unwrap_or_else(|_| unreachable!("address is ill-formatted"));
        change_tx
            .try_send(tower::discover::Change::Insert(addr.clone(), endpoint))
            .unwrap_or_else(|_| unreachable!("unknown channel tx send error"));
    }
    let client = Client::from_channel(channel);
    Connect {
        id,
        rpc_connect: client,
        change_tx,
        addrs: Mutex::new(addrs),

        tls_config,
    }
}

/// Creates a new connection with auto reconnect
fn new_reconnect(
    id: ServerId,
    addrs: Vec<String>,
    tls_config: Option<ClientTlsConfig>,
) -> Reconnect<Connect<ProtocolClient<Channel>>> {
    Reconnect::new(Box::new(move || {
        connect_to(id, addrs.clone(), tls_config.clone())
    }))
}

/// A wrapper of [`connect_to`], hide the detailed [`Connect<ProtocolClient>`]
pub(crate) fn connect(
    id: ServerId,
    addrs: Vec<String>,
    tls_config: Option<ClientTlsConfig>,
) -> Arc<dyn ConnectApi> {
    let conn = new_reconnect(id, addrs, tls_config);
    Arc::new(conn)
}

/// Wrapper of [`connect_all`], hide the details of [`Connect<ProtocolClient>`]
pub(crate) fn connects(
    members: HashMap<ServerId, Vec<String>>,
    tls_config: Option<&ClientTlsConfig>,
) -> impl Iterator<Item = (ServerId, Arc<dyn ConnectApi>)> {
    let tls_config = tls_config.cloned();
    members
        .into_iter()
        .map(move |(id, addrs)| (id, connect(id, addrs, tls_config.clone())))
}

/// Wrapper of [`connect_all`], hide the details of [`Connect<InnerProtocolClient>`]
pub(crate) fn inner_connects(
    members: HashMap<ServerId, Vec<String>>,
    tls_config: Option<&ClientTlsConfig>,
) -> impl Iterator<Item = (ServerId, InnerConnectApiWrapper)> {
    let tls_config = tls_config.cloned();
    members.into_iter().map(move |(id, addrs)| {
        (
            id,
            InnerConnectApiWrapper::new_from_arc(Arc::new(connect_to::<
                InnerProtocolClient<Channel>,
            >(
                id, addrs, tls_config.clone()
            ))),
        )
    })
}

/// Connect interface between server and clients
#[allow(clippy::module_name_repetitions)] // better for recognizing than just "Api"
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait ConnectApi: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> ServerId;

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error>;

    /// Send `ProposeRequest`
    async fn propose_stream(
        &self,
        request: ProposeRequest,
        token: Option<String>,
        timeout: Duration,
    ) -> Result<
        tonic::Response<Box<dyn Stream<Item = Result<OpResponse, tonic::Status>> + Send>>,
        CurpError,
    >;

    /// Send `RecordRequest`
    async fn record(
        &self,
        request: RecordRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RecordResponse>, CurpError>;

    /// Send `ReadIndexRequest`
    async fn read_index(
        &self,
        timeout: Duration,
    ) -> Result<tonic::Response<ReadIndexResponse>, CurpError>;

    /// Send `ProposeRequest`
    async fn propose_conf_change(
        &self,
        request: ProposeConfChangeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, CurpError>;

    /// Send `PublishRequest`
    async fn publish(
        &self,
        request: PublishRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<PublishResponse>, CurpError>;

    /// Send `ShutdownRequest`
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, CurpError>;

    /// Send `FetchClusterRequest`
    async fn fetch_cluster(
        &self,
        request: FetchClusterRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchClusterResponse>, CurpError>;

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, CurpError>;

    /// Send `MoveLeaderRequest`
    async fn move_leader(
        &self,
        request: MoveLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<MoveLeaderResponse>, CurpError>;

    /// Keep send lease keep alive to server and mutate the client id
    async fn lease_keep_alive(&self, client_id: Arc<AtomicU64>, interval: Duration) -> CurpError;
}

/// Inner Connect interface among different servers
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait InnerConnectApi: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> ServerId;

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error>;

    /// Send `AppendEntriesRequest`
    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status>;

    /// Send `VoteRequest`
    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status>;

    /// Send a snapshot
    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status>;

    /// Trigger follower shutdown
    async fn trigger_shutdown(&self) -> Result<(), tonic::Status>;

    /// Send `TryBecomeLeaderNowRequest`
    async fn try_become_leader_now(&self, timeout: Duration) -> Result<(), tonic::Status>;
}

/// Inner Connect Api Wrapper
/// The solution of [rustc bug](https://github.com/dtolnay/async-trait/issues/141#issuecomment-767978616)
#[derive(Clone)]
pub(crate) struct InnerConnectApiWrapper(Arc<dyn InnerConnectApi>);

impl InnerConnectApiWrapper {
    /// Create a new `InnerConnectApiWrapper` from `Arc<dyn ConnectApi>`
    pub(crate) fn new_from_arc(connect: Arc<dyn InnerConnectApi>) -> Self {
        Self(connect)
    }

    /// Create a new `InnerConnectApiWrapper` from id and addrs
    pub(crate) fn connect(
        id: ServerId,
        addrs: Vec<String>,
        tls_config: Option<ClientTlsConfig>,
    ) -> Self {
        let conn = connect_to::<InnerProtocolClient<Channel>>(id, addrs, tls_config);
        InnerConnectApiWrapper::new_from_arc(Arc::new(conn))
    }
}

impl Debug for InnerConnectApiWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerConnectApiWrapper").finish()
    }
}

impl Deref for InnerConnectApiWrapper {
    type Target = Arc<dyn InnerConnectApi>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The connection struct to hold the real rpc connections, it may failed to connect, but it also
/// retries the next time
#[derive(Debug)]
pub(crate) struct Connect<C> {
    /// Server id
    id: ServerId,
    /// The rpc connection
    rpc_connect: C,
    /// The rpc connection balance sender
    change_tx: tokio::sync::mpsc::Sender<tower::discover::Change<String, Endpoint>>,
    /// The current rpc connection address, when the address is updated,
    /// `addrs` will be used to remove previous connection
    addrs: Mutex<Vec<String>>,
    /// Client tls config
    tls_config: Option<ClientTlsConfig>,
}

impl<C> Connect<C> {
    /// Update server addresses, the new addresses will override the old ones
    async fn inner_update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
        let mut old = self.addrs.lock().await;
        let old_addrs: HashSet<String> = old.iter().cloned().collect();
        let new_addrs: HashSet<String> = addrs.iter().cloned().collect();
        let diffs = &old_addrs ^ &new_addrs;
        for diff in &diffs {
            let change = if new_addrs.contains(diff) {
                let endpoint = build_endpoint(diff, self.tls_config.as_ref())?;
                tower::discover::Change::Insert(diff.clone(), endpoint)
            } else {
                tower::discover::Change::Remove(diff.clone())
            };
            let _ig = self.change_tx.send(change).await;
        }
        *old = addrs;
        Ok(())
    }

    /// Before RPC
    #[cfg(feature = "client-metrics")]
    fn before_rpc<Req>(&self) -> std::time::Instant {
        super::metrics::get().peer_sent_bytes_total.add(
            std::mem::size_of::<Req>().max(1).numeric_cast(),
            &[opentelemetry::KeyValue::new("to", self.id.to_string())],
        );
        std::time::Instant::now()
    }

    /// Before RPC with size
    #[cfg(feature = "client-metrics")]
    fn before_rpc_with_size(&self, size: u64) -> std::time::Instant {
        super::metrics::get().peer_sent_bytes_total.add(
            size,
            &[opentelemetry::KeyValue::new("to", self.id.to_string())],
        );
        std::time::Instant::now()
    }

    /// After RPC
    #[cfg(feature = "client-metrics")]
    fn after_rpc<R>(&self, start_at: std::time::Instant, res: &Result<R, tonic::Status>) {
        super::metrics::get().peer_round_trip_time_seconds.record(
            start_at.elapsed().as_secs(),
            &[opentelemetry::KeyValue::new("to", self.id.to_string())],
        );
        if let Err(err) = res.as_ref() {
            super::metrics::get().peer_sent_failures_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new("to", self.id.to_string()),
                    opentelemetry::KeyValue::new("reason", err.message().to_owned()),
                ],
            );
        }
    }
}

/// Sets timeout for a client connection
macro_rules! with_timeout {
    ($timeout:expr, $client_op:expr) => {
        tokio::time::timeout($timeout, $client_op)
            .await
            .map_err(|_| tonic::Status::deadline_exceeded("timeout"))?
    };
}

#[async_trait]
impl ConnectApi for Connect<ProtocolClient<Channel>> {
    /// Get server id
    fn id(&self) -> ServerId {
        self.id
    }

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
        self.inner_update_addrs(addrs).await
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
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        if let Some(token) = token {
            _ = req.metadata_mut().insert("token", token.parse()?);
        }
        let resp = with_timeout!(timeout, client.propose_stream(req))?.into_inner();
        Ok(tonic::Response::new(Box::new(resp)))

        // let resp = client.propose_stream(req).await?.map(Box::new);
        // Ok(resp)
    }

    /// Send `RecordRequest`
    async fn record(
        &self,
        request: RecordRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RecordResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(request);
        with_timeout!(timeout, client.record(req)).map_err(Into::into)
    }

    /// Send `ReadIndexRequest`
    async fn read_index(
        &self,
        timeout: Duration,
    ) -> Result<tonic::Response<ReadIndexResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(ReadIndexRequest {});
        with_timeout!(timeout, client.read_index(req)).map_err(Into::into)
    }

    /// Send `ShutdownRequest`
    #[instrument(skip(self), name = "client shutdown")]
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_current();
        with_timeout!(timeout, client.shutdown(req)).map_err(Into::into)
    }

    /// Send `ProposeRequest`
    #[instrument(skip(self), name = "client propose conf change")]
    async fn propose_conf_change(
        &self,
        request: ProposeConfChangeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_current();
        with_timeout!(timeout, client.propose_conf_change(req)).map_err(Into::into)
    }

    /// Send `PublishRequest`
    #[instrument(skip(self), name = "client publish")]
    async fn publish(
        &self,
        request: PublishRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<PublishResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_current();
        with_timeout!(timeout, client.publish(req)).map_err(Into::into)
    }

    /// Send `FetchClusterRequest`
    async fn fetch_cluster(
        &self,
        request: FetchClusterRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchClusterResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(request);
        with_timeout!(timeout, client.fetch_cluster(req)).map_err(Into::into)
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(request);
        with_timeout!(timeout, client.fetch_read_state(req)).map_err(Into::into)
    }

    /// Send `MoveLeaderRequest`
    async fn move_leader(
        &self,
        request: MoveLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<MoveLeaderResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(request);
        with_timeout!(timeout, client.move_leader(req)).map_err(Into::into)
    }

    /// Keep send lease keep alive to server and mutate the client id
    async fn lease_keep_alive(&self, client_id: Arc<AtomicU64>, interval: Duration) -> CurpError {
        let mut client = self.rpc_connect.clone();
        loop {
            let stream = heartbeat_stream(
                client_id.load(std::sync::atomic::Ordering::Relaxed),
                interval,
            );
            let new_id = match client.lease_keep_alive(stream).await {
                Err(err) => return err.into(),
                Ok(res) => res.into_inner().client_id,
            };
            // The only place to update the client id for follower
            info!("client_id update to {new_id}");
            client_id.store(new_id, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[allow(clippy::let_and_return)] // for metrics
#[async_trait]
impl InnerConnectApi for Connect<InnerProtocolClient<Channel>> {
    /// Get server id
    fn id(&self) -> ServerId {
        self.id
    }

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
        self.inner_update_addrs(addrs).await
    }

    /// Send `AppendEntriesRequest`
    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        #[cfg(feature = "client-metrics")]
        let start_at = self.before_rpc::<AppendEntriesRequest>();

        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(request);
        let result = with_timeout!(timeout, client.append_entries(req));

        #[cfg(feature = "client-metrics")]
        self.after_rpc(start_at, &result);

        result
    }

    /// Send `VoteRequest`
    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        #[cfg(feature = "client-metrics")]
        let start_at = self.before_rpc::<VoteRequest>();

        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(request);
        let result = with_timeout!(timeout, client.vote(req));

        #[cfg(feature = "client-metrics")]
        self.after_rpc(start_at, &result);

        result
    }

    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        #[cfg(feature = "client-metrics")]
        let start_at = self.before_rpc_with_size(snapshot.inner().size());

        let stream = install_snapshot_stream(term, leader_id, snapshot);
        let mut client = self.rpc_connect.clone();
        let result = client.install_snapshot(stream).await;

        #[cfg(feature = "client-metrics")]
        self.after_rpc(start_at, &result);

        result
    }

    async fn trigger_shutdown(&self) -> Result<(), tonic::Status> {
        #[cfg(feature = "client-metrics")]
        let start_at = self.before_rpc::<TriggerShutdownRequest>();

        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(TriggerShutdownRequest::default());
        let result = client.trigger_shutdown(req).await;

        #[cfg(feature = "client-metrics")]
        self.after_rpc(start_at, &result);

        result.map(|_| ())
    }

    async fn try_become_leader_now(&self, timeout: Duration) -> Result<(), tonic::Status> {
        #[cfg(feature = "client-metrics")]
        let start_at = self.before_rpc::<TryBecomeLeaderNowRequest>();

        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(TryBecomeLeaderNowRequest::default());
        let result = with_timeout!(timeout, client.try_become_leader_now(req));

        #[cfg(feature = "client-metrics")]
        self.after_rpc(start_at, &result);

        result.map(|_| ())
    }
}

/// A connect api implementation which bypass kernel to dispatch method directly.
pub(crate) struct BypassedConnect<T: Protocol> {
    /// inner server
    server: T,
    /// server id
    id: ServerId,
}

impl<T: Protocol> BypassedConnect<T> {
    /// Create a bypassed connect
    pub(crate) fn new(id: ServerId, server: T) -> Self {
        Self { server, id }
    }
}

/// Metadata key of a bypassed request
const BYPASS_KEY: &str = "bypass";

/// Inject bypassed message into a request's metadata and check if it is a bypassed request.
///
/// A bypass request can skip the check for lease expiration (there will never be a disconnection from oneself).
pub(crate) trait Bypass {
    /// Inject into metadata
    fn inject_bypassed(&mut self);

    /// Check
    fn is_bypassed(&self) -> bool;
}

impl Bypass for tonic::metadata::MetadataMap {
    /// Inject into metadata
    fn inject_bypassed(&mut self) {
        let _ig = self.insert(
            BYPASS_KEY,
            "true"
                .parse()
                .unwrap_or_else(|_e| unreachable!("it must be parsed")),
        );
    }

    /// Check
    fn is_bypassed(&self) -> bool {
        self.contains_key(BYPASS_KEY)
    }
}

#[async_trait]
impl<T> ConnectApi for BypassedConnect<T>
where
    T: Protocol,
{
    /// Get server id
    fn id(&self) -> ServerId {
        self.id
    }

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, _addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
        // bypassed connect never updates its addresses
        Ok(())
    }

    /// Send `ProposeRequest`
    #[instrument(skip(self), name = "client propose stream")]
    async fn propose_stream(
        &self,
        request: ProposeRequest,
        token: Option<String>,
        _timeout: Duration,
    ) -> Result<
        tonic::Response<Box<dyn Stream<Item = Result<OpResponse, tonic::Status>> + Send>>,
        CurpError,
    > {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        if let Some(token) = token {
            _ = req.metadata_mut().insert("token", token.parse()?);
        }
        let resp = self.server.propose_stream(req).await?.into_inner();
        Ok(tonic::Response::new(Box::new(resp)))
    }

    /// Send `RecordRequest`
    #[instrument(skip(self), name = "client record")]
    async fn record(
        &self,
        request: RecordRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<RecordResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        self.server.record(req).await.map_err(Into::into)
    }

    async fn read_index(
        &self,
        _timeout: Duration,
    ) -> Result<tonic::Response<ReadIndexResponse>, CurpError> {
        let mut req = tonic::Request::new(ReadIndexRequest {});
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        self.server.read_index(req).await.map_err(Into::into)
    }

    /// Send `PublishRequest`
    async fn publish(
        &self,
        request: PublishRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<PublishResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        self.server.publish(req).await.map_err(Into::into)
    }

    /// Send `ProposeRequest`
    async fn propose_conf_change(
        &self,
        request: ProposeConfChangeRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        self.server
            .propose_conf_change(req)
            .await
            .map_err(Into::into)
    }

    /// Send `ShutdownRequest`
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        self.server.shutdown(req).await.map_err(Into::into)
    }

    /// Send `FetchClusterRequest`
    async fn fetch_cluster(
        &self,
        request: FetchClusterRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<FetchClusterResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        self.server.fetch_cluster(req).await.map_err(Into::into)
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_bypassed();
        req.metadata_mut().inject_current();
        self.server.fetch_read_state(req).await.map_err(Into::into)
    }

    /// Send `MoveLeaderRequest`
    async fn move_leader(
        &self,
        request: MoveLeaderRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<MoveLeaderResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_current();
        self.server.move_leader(req).await.map_err(Into::into)
    }

    /// Keep send lease keep alive to server and mutate the client id
    async fn lease_keep_alive(&self, _client_id: Arc<AtomicU64>, _interval: Duration) -> CurpError {
        unreachable!("cannot invoke lease_keep_alive in bypassed connect")
    }
}

/// Generate heartbeat stream
fn heartbeat_stream(client_id: u64, interval: Duration) -> impl Stream<Item = LeaseKeepAliveMsg> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    stream! {
        loop {
            _ = ticker.tick().await;
            if client_id == 0 {
                debug!("client request a client id");
            } else {
                debug!("client keep alive the client id({client_id})");
            }
            yield LeaseKeepAliveMsg { client_id };
        }
    }
}

/// Generate install snapshot stream
fn install_snapshot_stream(
    term: u64,
    leader_id: ServerId,
    snapshot: Snapshot,
) -> impl Stream<Item = InstallSnapshotRequest> {
    stream! {
        let meta = snapshot.meta;
        let mut snapshot = snapshot.into_inner();
        let mut offset = 0;
        if let Err(e) = snapshot.rewind() {
            error!("snapshot seek failed, {e}");
            return;
        }
        #[allow(clippy::arithmetic_side_effects)] // can't overflow
        while offset < snapshot.size() {
            let len: u64 =
                std::cmp::min(snapshot.size() - offset, SNAPSHOT_CHUNK_SIZE).numeric_cast();
            let mut data = BytesMut::with_capacity(len.numeric_cast());
            if let Err(e) = snapshot.read_buf_exact(&mut data).await {
                error!("read snapshot error, {e}");
                break;
            }
            yield InstallSnapshotRequest {
                term,
                leader_id,
                last_included_index: meta.last_included_index,
                last_included_term: meta.last_included_term,
                offset,
                data: data.freeze(),
                done: (offset + len) == snapshot.size(),
            };

            offset += len;
        }
        // TODO: Shall we clean snapshot after stream generation complete
        if let Err(e) = snapshot.clean().await {
            error!("snapshot clean error, {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use engine::{EngineType, Snapshot as EngineSnapshot};
    use futures::{pin_mut, StreamExt};
    use test_macros::abort_on_panic;
    use tracing_test::traced_test;

    use super::*;
    use crate::snapshot::SnapshotMeta;

    #[traced_test]
    #[tokio::test]
    #[abort_on_panic]
    async fn test_install_snapshot_stream() {
        const SNAPSHOT_SIZE: u64 = 200 * 1024;
        let mut snapshot = EngineSnapshot::new_for_receiving(EngineType::Memory).unwrap();
        snapshot
            .write_all(Bytes::from(vec![1; SNAPSHOT_SIZE.numeric_cast()]))
            .await
            .unwrap();
        let stream = install_snapshot_stream(
            0,
            123,
            Snapshot::new(
                SnapshotMeta {
                    last_included_index: 1,
                    last_included_term: 1,
                },
                snapshot,
            ),
        );
        pin_mut!(stream);
        let mut sum = 0;
        while let Some(req) = stream.next().await {
            assert_eq!(req.term, 0);
            assert_eq!(req.leader_id, 123);
            assert_eq!(req.last_included_index, 1);
            assert_eq!(req.last_included_term, 1);
            sum += req.data.len() as u64;
            assert_eq!(sum == SNAPSHOT_SIZE, req.done);
        }
        assert_eq!(sum, SNAPSHOT_SIZE);
    }
}
