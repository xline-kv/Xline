use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use async_stream::stream;
use async_trait::async_trait;
use bytes::BytesMut;
use clippy_utilities::NumericCast;
use engine::SnapshotApi;
use futures::Stream;
#[cfg(test)]
use mockall::automock;
use tokio::sync::RwLock;
use tracing::{debug, error, instrument};
use utils::tracing::Inject;

use crate::{
    error::ProposeError,
    rpc::{
        proto::protocol_client::ProtocolClient, AppendEntriesRequest, AppendEntriesResponse,
        FetchLeaderRequest, FetchLeaderResponse, FetchReadStateRequest, FetchReadStateResponse,
        InstallSnapshotRequest, InstallSnapshotResponse, ProposeRequest, ProposeResponse,
        VoteRequest, VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
    snapshot::Snapshot,
    ServerId,
};

/// Install snapshot chunk size: 64KB
const SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

/// Convert a vec of addr string to a vec of `Connect`
pub(crate) async fn connect(
    addrs: HashMap<ServerId, String>,
) -> HashMap<ServerId, Arc<dyn ConnectApi>> {
    futures::future::join_all(addrs.into_iter().map(|(id, mut addr)| async move {
        // Addrs must start with "http" to communicate with the server
        if !addr.starts_with("http://") {
            addr.insert_str(0, "http://");
        }
        (
            id,
            addr.clone(),
            ProtocolClient::connect(addr.clone()).await,
        )
    }))
    .await
    .into_iter()
    .map(|(id, addr, conn)| {
        debug!("successfully establish connection with {addr}");
        let connect: Arc<dyn ConnectApi> = Arc::new(Connect {
            id: id.clone(),
            rpc_connect: RwLock::new(conn),
            addr,
        });
        (id, connect)
    })
    .collect()
}

/// Connect interface
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait ConnectApi: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> &ServerId;

    /// Get the internal rpc connection/client
    async fn get(
        &self,
    ) -> Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error>;

    /// Send `ProposeRequest`
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, ProposeError>;

    /// Send `WaitSyncedRequest`
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, ProposeError>;

    /// Send `AppendEntriesRequest`
    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, ProposeError>;

    /// Send `VoteRequest`
    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, ProposeError>;

    /// Send `FetchLeaderRequest`
    async fn fetch_leader(
        &self,
        request: FetchLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchLeaderResponse>, ProposeError>;

    /// Send a snapshot
    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, ProposeError>;

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, ProposeError>;
}

/// The connection struct to hold the real rpc connections, it may failed to connect, but it also
/// retries the next time
#[derive(Debug)]
pub(crate) struct Connect {
    /// Server id
    id: ServerId,
    /// The rpc connection, if it fails it contains a error, otherwise the rpc client is there
    rpc_connect: RwLock<Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error>>,
    /// The addr used to connect if failing met
    addr: String,
}

#[async_trait]
impl ConnectApi for Connect {
    /// Get server id
    fn id(&self) -> &ServerId {
        &self.id
    }

    /// Get the internal rpc connection/client
    async fn get(
        &self,
    ) -> Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error> {
        if let Ok(ref client) = *self.rpc_connect.read().await {
            return Ok(client.clone());
        }
        let mut connect_write = self.rpc_connect.write().await;
        if let Ok(ref client) = *connect_write {
            return Ok(client.clone());
        }
        let client = ProtocolClient::<_>::connect(self.addr.clone())
            .await
            .map(|client| {
                *connect_write = Ok(client.clone());
                client
            })?;
        *connect_write = Ok(client.clone());
        Ok(client)
    }

    /// Send `ProposeRequest`
    #[instrument(skip(self), name = "client propose")]
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, ProposeError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.propose(req).await.map_err(Into::into)
    }

    /// Send `WaitSyncedRequest`
    #[instrument(skip(self), name = "client propose")]
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, ProposeError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        req.metadata_mut().inject_current();
        client.wait_synced(req).await.map_err(Into::into)
    }

    /// Send `AppendEntriesRequest`
    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, ProposeError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.append_entries(req).await.map_err(Into::into)
    }

    /// Send `VoteRequest`
    async fn vote(
        &self,
        request: VoteRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<VoteResponse>, ProposeError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.vote(req).await.map_err(Into::into)
    }

    /// Send `FetchLeaderRequest`
    async fn fetch_leader(
        &self,
        request: FetchLeaderRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchLeaderResponse>, ProposeError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_leader(req).await.map_err(Into::into)
    }

    async fn install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        snapshot: Snapshot,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, ProposeError> {
        let stream = install_snapshot_stream(term, leader_id, snapshot);
        let mut client = self.get().await?;
        client.install_snapshot(stream).await.map_err(Into::into)
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, ProposeError> {
        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_read_state(req).await.map_err(Into::into)
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
                leader_id: leader_id.clone(),
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
    use tracing_test::traced_test;

    use super::*;
    use crate::snapshot::SnapshotMeta;

    #[traced_test]
    #[tokio::test]
    async fn test_install_snapshot_stream() {
        const SNAPSHOT_SIZE: u64 = 200 * 1024;
        let mut snapshot = EngineSnapshot::new_for_receiving(EngineType::Memory).unwrap();
        snapshot
            .write_all(Bytes::from(vec![1; SNAPSHOT_SIZE.numeric_cast()]))
            .await
            .unwrap();
        let stream = install_snapshot_stream(
            0,
            "test".to_owned(),
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
            assert_eq!(req.leader_id, "test".to_owned());
            assert_eq!(req.last_included_index, 1);
            assert_eq!(req.last_included_term, 1);
            sum += req.data.len() as u64;
            assert_eq!(sum == SNAPSHOT_SIZE, req.done);
        }
        assert_eq!(sum, SNAPSHOT_SIZE);
    }
}
