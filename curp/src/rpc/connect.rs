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
use event_listener::Event;
use futures::Stream;
#[cfg(test)]
use mockall::automock;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, instrument};
use utils::tracing::Inject;

use super::{ShutdownRequest, ShutdownResponse};
use crate::{
    members::ServerId,
    rpc::{
        proto::{
            commandpb::protocol_client::ProtocolClient,
            inner_messagepb::inner_protocol_client::InnerProtocolClient,
        },
        AppendEntriesRequest, AppendEntriesResponse, FetchClusterRequest, FetchClusterResponse,
        FetchReadStateRequest, FetchReadStateResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, ProposeConfChangeRequest, ProposeConfChangeResponse,
        ProposeRequest, ProposeResponse, VoteRequest, VoteResponse, WaitSyncedRequest,
        WaitSyncedResponse,
    },
    snapshot::Snapshot,
    ClientLeaseKeepAliveRequest,
};

/// Install snapshot chunk size: 64KB
const SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

/// The default buffer size for rpc connection
const DEFAULT_BUFFER_SIZE: usize = 1024;

/// Connect implementation
macro_rules! connect_impl {
    ($client:ty, $api:path, $members:ident) => {
        futures::future::join_all(
            $members
                .into_iter()
                .map(|(id, mut addrs)| single_connect_impl!($client, $api, id, addrs)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, tonic::transport::Error>>()
        .map(IntoIterator::into_iter)
    };
}

/// Single Connect implementation
macro_rules! single_connect_impl {
    ($client:ty, $api:path, $id:ident, $addrs:ident) => {
        async move {
            let (channel, change_tx) = Channel::balance_channel(DEFAULT_BUFFER_SIZE);
            // Addrs must start with "http" to communicate with the server
            for addr in &mut $addrs {
                if !addr.starts_with("http://") {
                    addr.insert_str(0, "http://");
                }
                let endpoint = Endpoint::from_shared(addr.clone())?;
                let _ig = change_tx
                    .send(tower::discover::Change::Insert(addr.clone(), endpoint))
                    .await;
            }
            let client = <$client>::new(channel);
            let connect: Arc<dyn $api> = Arc::new(Connect {
                $id,
                rpc_connect: client,
                change_tx,
                addrs: Mutex::new($addrs),
            });
            Ok(($id, connect))
        }
    };
}

/// Convert a vec of addr string to a vec of `Connect`
/// # Errors
/// Return error if any of the address format is invalid
pub(crate) async fn connect(
    members: HashMap<ServerId, Vec<String>>,
) -> Result<impl Iterator<Item = (ServerId, Arc<dyn ConnectApi>)>, tonic::transport::Error> {
    connect_impl!(ProtocolClient<Channel>, ConnectApi, members)
}

/// Convert a vec of addr string to a vec of `InnerConnect`
pub(crate) async fn inner_connect(
    members: HashMap<ServerId, Vec<String>>,
) -> Result<impl Iterator<Item = (ServerId, InnerConnectApiWrapper)>, tonic::transport::Error> {
    connect_impl!(InnerProtocolClient<Channel>, InnerConnectApi, members)
        .map(|iter| iter.map(|(id, connect)| (id, InnerConnectApiWrapper::new_from_arc(connect))))
}

/// Connect interface between server and clients
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait ConnectApi: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> ServerId;

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), tonic::transport::Error>;

    /// Send `ProposeRequest`
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status>;

    /// Send `ProposeRequest`
    async fn propose_conf_change(
        &self,
        request: ProposeConfChangeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, tonic::Status>;

    /// Send `WaitSyncedRequest`
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status>;

    /// Send `ShutdownRequest`
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, tonic::Status>;

    /// Send `FetchClusterRequest`
    async fn fetch_cluster(
        &self,
        request: FetchClusterRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchClusterResponse>, tonic::Status>;

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, tonic::Status>;

    /// Keep send lease keep alive to server and mutate the client id
    /// Return RpcError if failed to get response
    /// Return leader_id and term if leadership changed
    async fn lease_keep_alive(
        &self,
        client_id: Arc<RwLock<u64>>,
        client_id_notifier: Arc<Event>,
    ) -> Result<(ServerId, u64, bool), tonic::Status>;
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
        mut addrs: Vec<String>,
    ) -> Result<Self, tonic::transport::Error> {
        let (_id, connect) =
            single_connect_impl!(InnerProtocolClient<Channel>, InnerConnectApi, id, addrs).await?;
        Ok(InnerConnectApiWrapper::new_from_arc(connect))
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
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.propose(req).await
    }

    /// Send `ShutdownRequest`
    #[instrument(skip(self), name = "client shutdown")]
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.shutdown(req).await
    }

    /// Send `ProposeRequest`
    #[instrument(skip(self), name = "client propose conf change")]
    async fn propose_conf_change(
        &self,
        request: ProposeConfChangeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.propose_conf_change(req).await
    }

    /// Send `WaitSyncedRequest`
    #[instrument(skip(self), name = "client propose")]
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.wait_synced(req).await
    }

    /// Send `FetchClusterRequest`
    async fn fetch_cluster(
        &self,
        request: FetchClusterRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchClusterResponse>, tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_cluster(req).await
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, tonic::Status> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_read_state(req).await
    }

    /// Keep send lease keep alive to server and mutate the client id
    /// Return RpcError if failed to get response
    /// Return leader_id and term if leadership changed
    async fn lease_keep_alive(
        &self,
        client_id: Arc<RwLock<u64>>,
        client_id_notifier: Arc<Event>,
    ) -> Result<(ServerId, u64, bool), tonic::Status> {
        let mut client = self.rpc_connect.clone();
        loop {
            let stream = heartbeat_stream(Arc::clone(&client_id));
            let resp = client.client_lease_keep_alive(stream).await?.into_inner();
            if resp.cluster_shutdown {
                return Ok((0, 0, true));
            }
            if let Some(leader_id) = resp.leader_id {
                return Ok((leader_id, resp.term, false));
            }
            let mut client_id = client_id.write().await;
            *client_id = resp.client_id;
            client_id_notifier.notify(usize::MAX);
        }
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

/// Generate heartbeat stream
fn heartbeat_stream(
    client_id: Arc<RwLock<u64>>,
) -> impl Stream<Item = ClientLeaseKeepAliveRequest> {
    /// Keep alive interval
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

    let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    stream! {
        loop {
            _ = ticker.tick().await;
            let id = *client_id.read().await;
            if id == 0 {
                debug!("grant a client id");
                yield ClientLeaseKeepAliveRequest::grant();
            } else {
                debug!("keep alive the client id({id})");
                yield ClientLeaseKeepAliveRequest::keep_alive(id);
            }
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
