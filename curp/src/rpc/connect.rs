use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
use engine::snapshot_api::SnapshotApi;
use futures::Stream;
#[cfg(test)]
use mockall::automock;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
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

/// Connect will call filter(request) before it sends out a request
pub trait TxFilter: Send + Sync + Debug {
    /// Filter request
    // TODO: add request as a parameter
    fn filter(&self) -> Option<()>;
    /// Clone self
    fn boxed_clone(&self) -> Box<dyn TxFilter>;
}

/// Convert a vec of addr string to a vec of `Connect`
pub(crate) async fn connect(
    addrs: HashMap<ServerId, String>,
    tx_filter: Option<Box<dyn TxFilter>>,
) -> HashMap<ServerId, Arc<Connect>> {
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
        let connect = Arc::new(Connect {
            id: id.clone(),
            rpc_connect: RwLock::new(conn),
            addr,
            tx_filter: tx_filter.as_ref().map(|f| f.boxed_clone()),
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
        mut snapshot: Snapshot,
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
    /// The injected filter
    tx_filter: Option<Box<dyn TxFilter>>,
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
        self.filter()?;

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
        self.filter()?;

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
        self.filter()?;

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
        self.filter()?;

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
        self.filter()?;

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
        self.filter()?;

        let mut client = self.get().await?;
        client
            .install_snapshot(Request::new(install_snapshot_stream(
                term, leader_id, snapshot,
            )))
            .await
            .map_err(Into::into)
    }

    /// Send `FetchReadStateRequest`
    async fn fetch_read_state(
        &self,
        request: FetchReadStateRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<FetchReadStateResponse>, ProposeError> {
        self.filter()?;

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
    // FIXME: The following code is better. But it will result in an unknown compiling error that might origin from a compiler bug(https://github.com/rust-lang/rust/issues/102211).
    // let req_stream = futures::stream::unfold(
    //     (0, snapshot, term, leader_id, meta),
    //     |(offset, mut snapshot, term, leader_id, meta)| async move {
    //         if offset >= snapshot.size() {
    //             return None;
    //         }
    //         let mut data = Vec::with_capacity(
    //             std::cmp::min(snapshot.size() - offset, SNAPSHOT_CHUNK_SIZE).numeric_cast(),
    //         );
    //         let n: u64 = match snapshot.read(&mut data) {
    //             Ok(n) => n.numeric_cast(),
    //             Err(e) => {
    //                 error!("read snapshot error, {e}");
    //                 return None;
    //             }
    //         };
    //         let done = (offset + n) == snapshot.size();
    //         let req = InstallSnapshotRequest {
    //             term,
    //             leader_id: leader_id.clone(),
    //             last_included_index: meta.last_included_index,
    //             last_included_term: meta.last_included_term,
    //             offset,
    //             data,
    //             done,
    //         };
    //         Some((req, (offset + n, snapshot, term, leader_id, meta)))
    //     },
    // );

    let (tx, rx) = mpsc::channel(1);
    let _ig = tokio::spawn(async move {
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
            let mut data = vec![0; len.numeric_cast()];
            if let Err(e) = snapshot.read_exact(&mut data).await {
                error!("read snapshot error, {e}");
                break;
            }
            let req = InstallSnapshotRequest {
                term,
                leader_id: leader_id.clone(),
                last_included_index: meta.last_included_index,
                last_included_term: meta.last_included_term,
                offset,
                data,
                done: (offset + len) == snapshot.size(),
            };
            if let Err(e) = tx.send(req).await {
                error!("snapshot tx error, {e}");
                break;
            }
            offset += len;
        }
        if let Err(e) = snapshot.clean().await {
            error!("snapshot clean error, {e}");
        };
    });

    ReceiverStream::new(rx)
}

impl Connect {
    /// Filter requests
    // TODO: add request as input
    fn filter(&self) -> Result<(), ProposeError> {
        self.tx_filter.as_ref().map_or(Ok(()), |f| {
            f.filter().map_or_else(
                || Err(ProposeError::RpcError("unreachable".to_owned())),
                |_| Ok(()),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use engine::snapshot_api::{MemorySnapshot, SnapshotApi, SnapshotProxy};
    use futures::StreamExt;
    use tracing_test::traced_test;

    use super::*;
    use crate::snapshot::SnapshotMeta;

    #[traced_test]
    #[tokio::test]
    async fn test_install_snapshot_stream() {
        const SNAPSHOT_SIZE: u64 = 200 * 1024;
        let mut snapshot = MemorySnapshot::default();
        snapshot
            .write_all(&mut vec![1; SNAPSHOT_SIZE.numeric_cast()])
            .await
            .unwrap();
        let mut stream = install_snapshot_stream(
            0,
            "test".to_owned(),
            Snapshot::new(
                SnapshotMeta {
                    last_included_index: 1,
                    last_included_term: 1,
                },
                SnapshotProxy::Memory(snapshot),
            ),
        );
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
