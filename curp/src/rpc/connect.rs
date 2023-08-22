use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use async_stream::stream;
use async_trait::async_trait;
use bytes::BytesMut;
use clippy_utilities::NumericCast;
use engine::SnapshotApi;
use futures::Stream;
#[cfg(test)]
use mockall::automock;
use tokio::sync::Mutex;
use tonic::transport::Endpoint;
use tracing::{error, instrument};
use utils::tracing::Inject;

use super::{ShutdownRequest, ShutdownResponse};
use crate::{
    error::RpcError,
    members::ServerId,
    rpc::{
        proto::{
            inner_messagepb::inner_protocol_client::InnerProtocolClient,
            messagepb::protocol_client::ProtocolClient,
        },
        AppendEntriesRequest, AppendEntriesResponse, FetchLeaderRequest, FetchLeaderResponse,
        FetchReadStateRequest, FetchReadStateResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, ProposeRequest, ProposeResponse, VoteRequest, VoteResponse,
        WaitSyncedRequest, WaitSyncedResponse,
    },
    snapshot::Snapshot,
};

/// Install snapshot chunk size: 64KB
const SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

/// Convert a vec of addr string to a vec of `Connect`
/// # Errors
/// Return error if any of the address format is invalid
pub(crate) async fn connect(
    members: HashMap<ServerId, Vec<String>>,
) -> Result<impl Iterator<Item = (ServerId, Arc<dyn ConnectApi>)>, tonic::transport::Error> {
    /// The default buffer size for rpc connection
    const DEFAULT_BUFFER_SIZE: usize = 1024;

    futures::future::join_all(members.into_iter().map(|(id, mut addrs)| async move {
        let (channel, change_tx) = tonic::transport::Channel::balance_channel(DEFAULT_BUFFER_SIZE);
        // Addrs must start with "http" to communicate with the server
        for addr in &mut addrs {
            if !addr.starts_with("http://") {
                addr.insert_str(0, "http://");
            }
            let endpoint = addr.parse()?;
            let res = change_tx
                .send(tower::discover::Change::Insert(addr.clone(), endpoint))
                .await;
            assert!(res.is_ok(), "balance rx must not be closed");
        }
        let client = ProtocolClient::new(channel);
        let connect: Arc<dyn ConnectApi> = Arc::new(Connect {
            id,
            rpc_connect: client,
            change_tx,
            addrs: Mutex::new(addrs),
        });
        Ok((id, connect))
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>, tonic::transport::Error>>()
    .map(IntoIterator::into_iter)
}

/// Convert a vec of addr string to a vec of `InnerConnect`
pub(crate) async fn inner_connect(
    addrs: HashMap<ServerId, String>,
) -> impl Iterator<Item = (ServerId, Arc<dyn InnerConnectApi>)> {
    futures::future::join_all(addrs.into_iter().map(|(id, mut addr)| async move {
        // Addrs must start with "http" to communicate with the server
        if !addr.starts_with("http://") {
            addr.insert_str(0, "http://");
        }
        (
            id,
            addr.clone(),
            InnerProtocolClient::connect(addr.clone()).await,
        )
    }))
    .await
    .into_iter()
    .map(|(id, addr, conn)| {
        debug!("successfully establish connection with {addr}");
        let connect: Arc<dyn InnerConnectApi> = Arc::new(InnerConnect {
            id,
            rpc_connect: RwLock::new(conn),
            addr,
        });
        (id, connect)
    })
}

/// Connect interface between server and clients
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait ConnectApi: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> ServerId;

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), RpcError>;

    /// Send `ProposeRequest`
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, RpcError>;

    /// Send `WaitSyncedRequest`
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, RpcError>;

    /// Send `ShutdownRequest`
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, RpcError>;

    /// Send `FetchLeaderRequest`
    async fn fetch_leader(
        &self,
        request: FetchLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchLeaderResponse>, RpcError>;

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, RpcError>;
}

/// Inner Connect interface among different servers
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait InnerConnectApi: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> ServerId;

    /// Get the internal rpc connection/client
    async fn get(
        &self,
    ) -> Result<InnerProtocolClient<tonic::transport::Channel>, tonic::transport::Error>;

    /// Send `AppendEntriesRequest`
    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, RpcError>;

    /// Send `VoteRequest`
    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, RpcError>;

    /// Send a snapshot
    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, RpcError>;
}

/// The connection struct to hold the real rpc connections, it may failed to connect, but it also
/// retries the next time
#[derive(Debug)]
pub(crate) struct Connect {
    /// Server id
    id: ServerId,
    /// The rpc connection
    rpc_connect: ProtocolClient<tonic::transport::Channel>,
    /// The rpc connection balance sender
    change_tx: tokio::sync::mpsc::Sender<tower::discover::Change<String, Endpoint>>,
    /// The current rpc connection address, when the address is updated,
    /// `addrs` will be used to remove previous connection
    addrs: Mutex<Vec<String>>,
}

#[async_trait]
impl ConnectApi for Connect {
    /// Get server id
    fn id(&self) -> ServerId {
        self.id
    }

    /// Update server addresses, the new addresses will override the old ones
    async fn update_addrs(&self, addrs: Vec<String>) -> Result<(), RpcError> {
        let mut old_addrs = self.addrs.lock().await;
        for addr in &*old_addrs {
            let res = self
                .change_tx
                .send(tower::discover::Change::Remove(addr.clone()))
                .await;
            assert!(res.is_ok(), "balance rx must not be closed");
        }
        for addr in &addrs {
            let res = self
                .change_tx
                .send(tower::discover::Change::Insert(addr.clone(), addr.parse()?))
                .await;
            assert!(res.is_ok(), "balance rx must not be closed");
        }
        *old_addrs = addrs;
        Ok(())
    }

    /// Send `ProposeRequest`
    #[instrument(skip(self), name = "client propose")]
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, RpcError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.propose(req).await.map_err(Into::into)
    }

    /// Send `ShutdownRequest`
    async fn shutdown(
        &self,
        request: ShutdownRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ShutdownResponse>, RpcError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.shutdown(req).await.map_err(Into::into)
    }

    /// Send `WaitSyncedRequest`
    #[instrument(skip(self), name = "client propose")]
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, RpcError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.wait_synced(req).await.map_err(Into::into)
    }

    /// Send `FetchLeaderRequest`
    async fn fetch_leader(
        &self,
        request: FetchLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchLeaderResponse>, RpcError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_leader(req).await.map_err(Into::into)
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, RpcError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_read_state(req).await.map_err(Into::into)
    }
}

// The inner connection struct to hold the real rpc connections, it may failed to connect, but it also
/// retries the next time
#[derive(Debug)]
pub(crate) struct InnerConnect {
    /// Server id
    id: ServerId,
    /// The rpc connection, if it fails it contains a error, otherwise the rpc client is there
    rpc_connect:
        RwLock<Result<InnerProtocolClient<tonic::transport::Channel>, tonic::transport::Error>>,
    /// The addr used to connect if failing met
    addr: String,
}

#[async_trait]
impl InnerConnectApi for InnerConnect {
    /// Get server id
    fn id(&self) -> ServerId {
        self.id
    }

    /// Get the internal rpc connection/client
    async fn get(
        &self,
    ) -> Result<InnerProtocolClient<tonic::transport::Channel>, tonic::transport::Error> {
        if let Ok(ref client) = *self.rpc_connect.read().await {
            return Ok(client.clone());
        }
        let mut connect_write = self.rpc_connect.write().await;
        if let Ok(ref client) = *connect_write {
            return Ok(client.clone());
        }
        let client = InnerProtocolClient::<_>::connect(self.addr.clone())
            .await
            .map(|client| {
                *connect_write = Ok(client.clone());
                client
            })?;
        *connect_write = Ok(client.clone());
        Ok(client)
    }

    /// Send `AppendEntriesRequest`
    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, RpcError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.append_entries(req).await.map_err(Into::into)
    }

    /// Send `VoteRequest`
    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, RpcError> {
        let mut client = self.rpc_connect.clone();
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.vote(req).await.map_err(Into::into)
    }

    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, RpcError> {
        let stream = install_snapshot_stream(term, leader_id, snapshot);
        let mut client = self.rpc_connect.clone();
        client.install_snapshot(stream).await.map_err(Into::into)
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
