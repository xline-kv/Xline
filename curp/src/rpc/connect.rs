#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashMap, sync::Arc, time::Duration};

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

    /// Convert a vec of addr string to a vec of `Connect`
    async fn try_connect(addrs: HashMap<ServerId, String>) -> Vec<Arc<Self>>;

    /// Convert a vec of addr string to a vec of `Connect`
    #[cfg(test)]
    async fn try_connect_test(
        addrs: HashMap<ServerId, String>,
        reachable: Arc<AtomicBool>,
    ) -> Vec<Arc<Self>>;
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
    /// Whether the client can reach others, useful for testings
    #[cfg(test)]
    reachable: Arc<AtomicBool>,
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
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

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
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

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
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

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
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

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
        #[cfg(test)]
        if !self.reachable.load(Ordering::Relaxed) {
            return Err(ProposeError::RpcError("unreachable".to_owned()));
        }

        let mut client = self.get().await?;
        let mut req = tonic::Request::new(request);
        req.set_timeout(timeout);
        client.fetch_leader(req).await.map_err(Into::into)
    }

    /// Convert a vec of addr string to a vec of `Connect`
    async fn try_connect(addrs: HashMap<ServerId, String>) -> Vec<Arc<Self>> {
        futures::future::join_all(addrs.into_iter().map(|(id, mut addr)| async move {
            // Addrs must start with "http" to communicate with the server
            if !addr.starts_with("http") {
                addr.insert_str(0, "http://");
            }
            (
                id,
                addr.clone(),
                ProtocolClient::<_>::connect(addr.clone()).await,
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
                #[cfg(test)]
                reachable: Arc::new(AtomicBool::new(true)),
            })
        })
        .collect()
    }

    /// Convert a vec of addr string to a vec of `Connect`
    #[cfg(test)]
    async fn try_connect_test(
        addrs: HashMap<ServerId, String>,
        reachable: Arc<AtomicBool>,
    ) -> Vec<Arc<Self>> {
        futures::future::join_all(addrs.into_iter().map(|(id, mut addr)| async move {
            // Addrs must start with "http" to communicate with the server
            if !addr.starts_with("http") {
                addr.insert_str(0, "http://");
            }
            (
                id,
                addr.clone(),
                ProtocolClient::<_>::connect(addr.clone()).await,
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
                reachable: Arc::clone(&reachable),
            })
        })
        .collect()
    }
}
