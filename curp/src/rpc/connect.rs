use std::{collections::HashMap, fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use tokio::sync::RwLock;
use tracing::{debug, instrument};
use utils::tracing::Inject;

use crate::{
    error::ProposeError,
    message::ServerId,
    rpc::{
        proto::protocol_client::ProtocolClient, AppendEntriesRequest, AppendEntriesResponse,
        FetchLeaderRequest, FetchLeaderResponse, ProposeRequest, ProposeResponse, VoteRequest,
        VoteResponse, WaitSyncedRequest, WaitSyncedResponse,
    },
};

/// Connect will call filter(request) before it sends out a request
pub trait TxFilter: Send + Sync + Debug {
    /// Filter request
    // TODO: add request as a parameter
    fn filter(&self) -> Option<()>;
    /// Clone self
    fn boxed_clone(&self) -> Box<dyn TxFilter>;
}

/// Convert a vec of addr string to a vec of `Connect`
// FIXME: move it back to `Connect` when `mockall` supports mock `Fn`
pub(crate) async fn connect(
    addrs: HashMap<ServerId, String>,
    tx_filter: Option<Box<dyn TxFilter>>,
) -> Vec<Arc<Connect>> {
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
        Arc::new(Connect {
            id,
            rpc_connect: RwLock::new(conn),
            addr,
            tx_filter: tx_filter.as_ref().map(|f| f.boxed_clone()),
        })
    })
    .collect()
}

/// Connect interface
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait ConnectInterface: Send + Sync + 'static {
    /// Get server id
    fn id(&self) -> &ServerId;

    /// Get the internal rpc connection/client
    async fn get(
        &self,
    ) -> Result<ProtocolClient<tonic::transport::Channel>, tonic::transport::Error>;

    /// send "propose" request
    async fn propose(
        &self,
        request: ProposeRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, ProposeError>;

    /// send "wait synced" request
    async fn wait_synced(
        &self,
        request: WaitSyncedRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, ProposeError>;

    /// Send `AppendEntries` request
    async fn append_entries(
        &self,
        request: AppendEntriesRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AppendEntriesResponse>, ProposeError>;

    /// Send `Vote` request
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
impl ConnectInterface for Connect {
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

    /// send "propose" request
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

    /// send "wait synced" request
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

    /// Send `AppendEntries` request
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

    /// Send `Vote` request
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
