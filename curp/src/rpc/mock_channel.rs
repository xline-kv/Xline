use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tower::discover::Change;
use tracing::debug;

use crate::rpc::proto::protocol_client::ProtocolClient;

/// Mocked Channel
#[derive(Clone, Debug)]
pub(crate) struct MockedChannel {
    /// Endpoints
    endpoints: Arc<RwLock<HashMap<String, Endpoint>>>,
}

impl MockedChannel {
    /// Mock `balance_channel` method
    pub(crate) fn balance_channel(capacity: usize) -> (Self, Sender<Change<String, Endpoint>>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Change<String, Endpoint>>(capacity);
        let endpoints = Arc::new(RwLock::new(HashMap::new()));
        let endpoints_clone = Arc::clone(&endpoints);
        let _ig = madsim::task::spawn(async move {
            while let Some(change) = rx.recv().await {
                match change {
                    Change::Insert(key, endpoint) => {
                        debug!("insert endpoint: {key}");
                        let _ig = endpoints_clone.write().await.insert(key, endpoint);
                    }
                    Change::Remove(key) => {
                        let _ig = endpoints_clone.write().await.remove(&key);
                    }
                }
            }
        });
        (Self { endpoints }, tx)
    }
}

/// Mocked Protocol Client
#[derive(Debug, Clone)]
pub(crate) struct MockedProtocolClient<T> {
    /// Mocked channel
    channel: MockedChannel,
    /// Cached client
    cached_client: Option<ProtocolClient<Channel>>,
    /// PhantomData
    _fake: PhantomData<T>,
}

impl<T> MockedProtocolClient<T> {
    /// Mock `new` method
    pub(crate) fn new(channel: MockedChannel) -> Self {
        Self {
            channel,
            cached_client: None,
            _fake: PhantomData,
        }
    }
}

/// Mock some methods of `ProtocolClient`
impl<T> MockedProtocolClient<T> {
    /// Get a client base on round-robin mechanism
    async fn get_client(&mut self) -> Result<ProtocolClient<Channel>, tonic::Status> {
        if let Some(client) = self.cached_client.as_ref() {
            return Ok(client.clone());
        }
        let endpoints = self.channel.endpoints.read().await;
        // In the testing phase of madsim, each node has only one address.
        // Additionally, these mock stuff will be removed in the future,
        // thus round-robin and similar strategies have not been implemented here.
        if let Some(endpoint) = endpoints.values().next() {
            let chan = endpoint.connect().await.map_err(|_e| {
                tonic::Status::new(tonic::Code::Unavailable, "Failed to connect to endpoint")
            })?;
            return Ok(self.cached_client.insert(ProtocolClient::new(chan)).clone());
        }
        Err(tonic::Status::new(
            tonic::Code::Unavailable,
            "No endpoint available",
        ))
    }

    /// Mock `propose` method
    pub(crate) async fn propose(
        &mut self,
        request: impl tonic::IntoRequest<super::ProposeRequest>,
    ) -> Result<tonic::Response<super::ProposeResponse>, tonic::Status> {
        self.get_client().await?.propose(request).await
    }

    /// Mock `wait_synced` method
    pub(crate) async fn wait_synced(
        &mut self,
        request: impl tonic::IntoRequest<super::WaitSyncedRequest>,
    ) -> Result<tonic::Response<super::WaitSyncedResponse>, tonic::Status> {
        self.get_client().await?.wait_synced(request).await
    }

    /// Mock `append_entries` method
    pub(crate) async fn append_entries(
        &mut self,
        request: impl tonic::IntoRequest<super::AppendEntriesRequest>,
    ) -> Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status> {
        self.get_client().await?.append_entries(request).await
    }

    /// Mock `vote` method
    pub(crate) async fn vote(
        &mut self,
        request: impl tonic::IntoRequest<super::VoteRequest>,
    ) -> Result<tonic::Response<super::VoteResponse>, tonic::Status> {
        self.get_client().await?.vote(request).await
    }

    /// Mock `install_snapshot` method
    pub(crate) async fn install_snapshot(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = super::InstallSnapshotRequest>,
    ) -> Result<tonic::Response<super::InstallSnapshotResponse>, tonic::Status> {
        self.get_client().await?.install_snapshot(request).await
    }

    /// Mock `fetch_leader` method
    pub(crate) async fn fetch_leader(
        &mut self,
        request: impl tonic::IntoRequest<super::FetchLeaderRequest>,
    ) -> Result<tonic::Response<super::FetchLeaderResponse>, tonic::Status> {
        self.get_client().await?.fetch_leader(request).await
    }

    /// Mock `fetch_read_state` method
    pub(crate) async fn fetch_read_state(
        &mut self,
        request: impl tonic::IntoRequest<super::FetchReadStateRequest>,
    ) -> Result<tonic::Response<super::FetchReadStateResponse>, tonic::Status> {
        self.get_client().await?.fetch_read_state(request).await
    }
}
