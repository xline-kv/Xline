use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use async_stream::stream;
use async_trait::async_trait;
use bytes::BytesMut;
use clippy_utilities::NumericCast;
use engine::SnapshotApi;
use futures::{stream::FuturesUnordered, Stream};
#[cfg(test)]
use mockall::automock;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tracing::{error, instrument};
use utils::tracing::Inject;

use super::{ShutdownRequest, ShutdownResponse};
use crate::{
    members::ServerId,
    rpc::{
        proto::{
            commandpb::protocol_client::ProtocolClient,
            inner_messagepb::inner_protocol_client::InnerProtocolClient,
        },
        AppendEntriesRequest, AppendEntriesResponse, CurpError, FetchClusterRequest,
        FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, ProposeConfChangeRequest,
        ProposeConfChangeResponse, ProposeRequest, ProposeResponse, PublishRequest,
        PublishResponse, TriggerShutdownRequest, VoteRequest, VoteResponse, WaitSyncedRequest,
        WaitSyncedResponse,
    },
    snapshot::Snapshot,
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

/// Connect to a server
async fn connect_to<Client: FromTonicChannel>(
    id: ServerId,
    mut addrs: Vec<String>,
) -> Result<Arc<Connect<Client>>, tonic::transport::Error> {
    let (channel, change_tx) = Channel::balance_channel(DEFAULT_BUFFER_SIZE);
    for addr in &mut addrs {
        // TODO: support TLS
        if !addr.starts_with("http://") {
            addr.insert_str(0, "http://");
        }
        let endpoint = Endpoint::from_shared(addr.clone())?;
        let _ig = change_tx
            .send(tower::discover::Change::Insert(addr.clone(), endpoint))
            .await;
    }
    let client = Client::from_channel(channel);
    let connect = Arc::new(Connect {
        id,
        rpc_connect: client,
        change_tx,
        addrs: Mutex::new(addrs),
    });
    Ok(connect)
}

/// Connect to a map of members
async fn connect_all<Client: FromTonicChannel>(
    members: HashMap<ServerId, Vec<String>>,
) -> Result<Vec<(u64, Arc<Connect<Client>>)>, tonic::transport::Error> {
    let conns_to: FuturesUnordered<_> =
        members
            .into_iter()
            .map(|(id, addrs)| async move {
                connect_to::<Client>(id, addrs).await.map(|conn| (id, conn))
            })
            .collect();
    futures::StreamExt::collect::<Vec<_>>(conns_to)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
}

/// A wrapper of [`connect_to`], hide the detailed [`Connect<ProtocolClient>`]
pub(crate) async fn connect(
    id: ServerId,
    addrs: Vec<String>,
) -> Result<Arc<dyn ConnectApi>, tonic::transport::Error> {
    let conn = connect_to::<ProtocolClient<Channel>>(id, addrs).await?;
    Ok(conn)
}

/// Wrapper of [`connect_all`], hide the details of [`Connect<ProtocolClient>`]
pub(crate) async fn connects(
    members: HashMap<ServerId, Vec<String>>,
) -> Result<impl Iterator<Item = (ServerId, Arc<dyn ConnectApi>)>, tonic::transport::Error> {
    // It seems that casting high-rank types cannot be inferred, so we allow trivial_casts to cast manually
    #[allow(trivial_casts)]
    #[allow(clippy::as_conversions)]
    let conns = connect_all(members)
        .await?
        .into_iter()
        .map(|(id, conn)| (id, conn as Arc<dyn ConnectApi>));
    Ok(conns)
}

/// Wrapper of [`connect_all`], hide the details of [`Connect<InnerProtocolClient>`]
pub(crate) async fn inner_connects(
    members: HashMap<ServerId, Vec<String>>,
) -> Result<impl Iterator<Item = (ServerId, InnerConnectApiWrapper)>, tonic::transport::Error> {
    let conns = connect_all(members)
        .await?
        .into_iter()
        .map(|(id, conn)| (id, InnerConnectApiWrapper::new_from_arc(conn)));
    Ok(conns)
}

/// Connect interface between server and clients
#[cfg_attr(test, automock)]
#[allow(clippy::module_name_repetitions)] // better for recognizing than just "Api"
#[async_trait]
pub trait ConnectApi: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> ServerId;

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error>;

    /// Send `ProposeRequest`
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, CurpError>;

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

    /// Send `WaitSyncedRequest`
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, CurpError>;

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
    pub(crate) async fn connect(
        id: ServerId,
        addrs: Vec<String>,
    ) -> Result<Self, tonic::transport::Error> {
        let conn = connect_to::<InnerProtocolClient<Channel>>(id, addrs).await?;
        Ok(InnerConnectApiWrapper::new_from_arc(conn))
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
                let endpoint = Endpoint::from_shared(diff.clone())?;
                tower::discover::Change::Insert(diff.clone(), endpoint)
            } else {
                tower::discover::Change::Remove(diff.clone())
            };
            let _ig = self.change_tx.send(change).await;
        }
        *old = addrs;
        Ok(())
    }
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
    #[instrument(skip(self), name = "client propose")]
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.propose(req).await.map_err(Into::into)
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
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.shutdown(req).await.map_err(Into::into)
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
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.propose_conf_change(req).await.map_err(Into::into)
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
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.publish(req).await.map_err(Into::into)
    }

    /// Send `WaitSyncedRequest`
    #[instrument(skip(self), name = "client propose")]
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.wait_synced(req).await.map_err(Into::into)
    }

    /// Send `FetchClusterRequest`
    async fn fetch_cluster(
        &self,
        request: FetchClusterRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchClusterResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_cluster(req).await.map_err(Into::into)
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, CurpError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_read_state(req).await.map_err(Into::into)
    }
}

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
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.append_entries(req).await
    }

    /// Send `VoteRequest`
    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.vote(req).await
    }

    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        let stream = install_snapshot_stream(term, leader_id, snapshot);
        let mut client = self.rpc_connect.clone();
        client.install_snapshot(stream).await
    }

    async fn trigger_shutdown(&self) -> Result<(), tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let req = tonic::Request::new(TriggerShutdownRequest::default());
        _ = client.trigger_shutdown(req).await?;
        Ok(())
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
    #[allow(unused)] // TODO: remove
    pub(crate) fn new(id: ServerId, server: T) -> Self {
        Self { server, id }
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
    async fn propose(
        &self,
        request: ProposeRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_current();
        self.server.propose(req).await.map_err(Into::into)
    }

    /// Send `PublishRequest`
    async fn publish(
        &self,
        request: PublishRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<PublishResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
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
        req.metadata_mut().inject_current();
        self.server
            .propose_conf_change(req)
            .await
            .map_err(Into::into)
    }

    /// Send `WaitSyncedRequest`
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
        req.metadata_mut().inject_current();
        self.server.wait_synced(req).await.map_err(Into::into)
    }

    /// Send `ShutdownRequest`
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, CurpError> {
        let mut req = tonic::Request::new(request);
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
        req.metadata_mut().inject_current();
        self.server.fetch_read_state(req).await.map_err(Into::into)
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
        #[allow(clippy::integer_arithmetic)] // can't overflow
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
