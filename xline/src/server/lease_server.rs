use std::{sync::Arc, time::Duration};

use clippy_utilities::Cast;
use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use tokio::{sync::mpsc, time};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::{
    auth_server::get_token,
    command::{Command, CommandResponse, KeyRange, SyncResponse},
};
use crate::{
    id_gen::IdGenerator,
    rpc::{
        Lease, LeaseClient, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
        LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest,
        LeaseRevokeResponse, LeaseStatus, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
        RequestWithToken, RequestWrapper,
    },
    state::State,
    storage::{storage_api::StorageApi, AuthStore, LeaseStore},
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;
/// Default Lease Request Time
const DEFAULT_LEASE_REQUEST_TIME: Duration = Duration::from_millis(500);

/// Lease Server
#[derive(Debug)]
pub(crate) struct LeaseServer<S>
where
    S: StorageApi,
{
    /// Lease storage
    lease_storage: Arc<LeaseStore>,
    /// Auth storage
    auth_storage: Arc<AuthStore<S>>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
    /// State of current node
    state: Arc<State>,
    /// Id generator
    id_gen: Arc<IdGenerator>,
}

impl<S> LeaseServer<S>
where
    S: StorageApi,
{
    /// New `LeaseServer`
    pub(crate) fn new(
        lease_storage: Arc<LeaseStore>,
        auth_storage: Arc<AuthStore<S>>,
        client: Arc<Client<Command>>,
        name: String,
        state: Arc<State>,
        id_gen: Arc<IdGenerator>,
    ) -> Arc<Self> {
        let lease_server = Arc::new(Self {
            lease_storage,
            auth_storage,
            client,
            name,
            state,
            id_gen,
        });
        let _h = tokio::spawn(Self::revoke_expired_leases_task(Arc::clone(&lease_server)));
        lease_server
    }

    /// Task of revoke expired leases
    async fn revoke_expired_leases_task(lease_server: Arc<LeaseServer<S>>) {
        loop {
            // only leader will check expired lease
            if lease_server.is_leader() {
                for id in lease_server.lease_storage.find_expired_leases() {
                    let _handle = tokio::spawn({
                        let s = Arc::clone(&lease_server);
                        let token_option = lease_server.auth_storage.root_token();
                        async move {
                            let mut request = tonic::Request::new(LeaseRevokeRequest { id });
                            if let Ok(token) = token_option {
                                let _ignore = request.metadata_mut().insert(
                                    "token",
                                    token.parse().unwrap_or_else(|e| {
                                        panic!("metadata value parse error: {}", e)
                                    }),
                                );
                            }
                            if let Err(e) = s.lease_revoke(request).await {
                                warn!("Failed to revoke expired leases: {}", e);
                            }
                        }
                    });
                }
            } else {
                let listener = lease_server.state.leader_listener();
                listener.await;
            }

            time::sleep(DEFAULT_LEASE_REQUEST_TIME).await;
        }
    }

    /// Generate propose id
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }

    /// Generate `Command` proposal from `Request`
    fn command_from_request_wrapper(
        &self,
        propose_id: ProposeId,
        wrapper: RequestWithToken,
    ) -> Command {
        let keys = if let RequestWrapper::LeaseRevokeRequest(ref req) = wrapper.request {
            self.lease_storage
                .get_keys(req.id)
                .into_iter()
                .map(|k| KeyRange::new(k, ""))
                .collect()
        } else {
            vec![]
        };
        Command::new(keys, wrapper, propose_id)
    }

    /// Check if the node is leader
    fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: tonic::Request<T>,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>), tonic::Status>
    where
        T: Into<RequestWrapper>,
    {
        let wrapper = match get_token(request.metadata()) {
            Some(token) => RequestWithToken::new_with_token(request.into_inner().into(), token),
            None => RequestWithToken::new(request.into_inner().into()),
        };
        let propose_id = self.generate_propose_id();
        let cmd = self.command_from_request_wrapper(propose_id, wrapper);
        if use_fast_path {
            let cmd_res = self.client.propose(cmd).await.map_err(|err| {
                if let ProposeError::ExecutionError(e) = err {
                    tonic::Status::invalid_argument(e)
                } else {
                    panic!("propose err {err:?}")
                }
            })?;
            Ok((cmd_res, None))
        } else {
            let (cmd_res, sync_res) = self.client.propose_indexed(cmd).await.map_err(|err| {
                if let ProposeError::ExecutionError(e) = err {
                    tonic::Status::invalid_argument(e)
                } else {
                    panic!("propose err {err:?}")
                }
            })?;
            Ok((cmd_res, Some(sync_res)))
        }
    }

    /// Handle keep alive at leader
    async fn leader_keep_alive(
        &self,
        mut request_stream: tonic::Streaming<LeaseKeepAliveRequest>,
    ) -> ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>> {
        let (response_tx, response_rx) = mpsc::channel(CHANNEL_SIZE);
        let _hd = tokio::spawn({
            let lease_storage = Arc::clone(&self.lease_storage);
            async move {
                while let Some(req_result) = request_stream.next().await {
                    match req_result {
                        Ok(keep_alive_req) => {
                            debug!("Receive LeaseKeepAliveRequest {:?}", keep_alive_req);
                            // TODO wait applied index
                            let res = lease_storage
                                .keep_alive(keep_alive_req.id)
                                .map(|ttl| LeaseKeepAliveResponse {
                                    id: keep_alive_req.id,
                                    ttl,
                                    ..LeaseKeepAliveResponse::default()
                                })
                                .map_err(|e| {
                                    tonic::Status::invalid_argument(format!(
                                        "Keep alive error: {}",
                                        e
                                    ))
                                });
                            assert!(
                                response_tx.send(res).await.is_ok(),
                                "Command receiver dropped"
                            );
                        }
                        Err(e) => {
                            warn!("Receive LeaseKeepAliveRequest error {:?}", e);
                            break;
                        }
                    }
                }
            }
        });
        ReceiverStream::new(response_rx)
    }

    /// Handle keep alive at follower
    async fn follower_keep_alive(
        &self,
        mut request_stream: tonic::Streaming<LeaseKeepAliveRequest>,
    ) -> Result<ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>, tonic::Status> {
        // TODO: refactor stream forward in a easy way
        let leader_addr = self.state.wait_leader().await?;
        let mut lease_client = LeaseClient::connect(format!("http://{leader_addr}"))
            .await
            .map_err(|_e| tonic::Status::internal("Connect to leader error: {e}"))?;

        let (request_tx, request_rx) = mpsc::channel(CHANNEL_SIZE);
        let (response_tx, response_rx) = mpsc::channel(CHANNEL_SIZE);

        let _req_handle = tokio::spawn(async move {
            while let Some(Ok(req)) = request_stream.next().await {
                assert!(request_tx.send(req).await.is_ok(), "receiver dropped");
            }
            info!("redirect stream closed");
        });

        let _client_handle = tokio::spawn(async move {
            let mut stream = lease_client
                .lease_keep_alive(ReceiverStream::new(request_rx))
                .await
                .unwrap_or_else(|e| panic!("Stream redirect to leader failed: {e:?}"))
                .into_inner();
            while let Some(res) = stream.next().await {
                assert!(response_tx.send(res).await.is_ok(), "receiver dropped");
            }
        });

        Ok(ReceiverStream::new(response_rx))
    }
}

#[tonic::async_trait]
impl<S> Lease for LeaseServer<S>
where
    S: StorageApi,
{
    /// LeaseGrant creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    async fn lease_grant(
        &self,
        mut request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        debug!("Receive LeaseGrantRequest {:?}", request);
        let lease_grant_req = request.get_mut();
        if lease_grant_req.id == 0 {
            lease_grant_req.id = self.id_gen.next().cast();
        }

        let is_fast_path = true;
        let (res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: LeaseGrantResponse = res.decode().into();
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for AuthStatusResponse", revision);
            if let Some(mut header) = res.header.as_mut() {
                header.revision = revision;
            }
        }
        Ok(tonic::Response::new(res))
    }

    /// LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
    async fn lease_revoke(
        &self,
        request: tonic::Request<LeaseRevokeRequest>,
    ) -> Result<tonic::Response<LeaseRevokeResponse>, tonic::Status> {
        debug!("Receive LeaseRevokeRequest {:?}", request);

        let is_fast_path = true;
        let (res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: LeaseRevokeResponse = res.decode().into();
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for AuthStatusResponse", revision);
            if let Some(mut header) = res.header.as_mut() {
                header.revision = revision;
            }
        }
        Ok(tonic::Response::new(res))
    }

    ///Server streaming response type for the LeaseKeepAlive method.
    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        debug!("Receive LeaseKeepAliveRequest {:?}", request);
        let request_stream = request.into_inner();
        let response_stream = if self.is_leader() {
            self.leader_keep_alive(request_stream).await
        } else {
            self.follower_keep_alive(request_stream).await?
        };
        Ok(tonic::Response::new(response_stream))
    }

    /// LeaseTimeToLive retrieves lease information.
    async fn lease_time_to_live(
        &self,
        request: tonic::Request<LeaseTimeToLiveRequest>,
    ) -> Result<tonic::Response<LeaseTimeToLiveResponse>, tonic::Status> {
        debug!("Receive LeaseTimeToLiveRequest {:?}", request);
        if self.is_leader() {
            // TODO wait applied index
            let time_to_live_req = request.into_inner();
            let lease = match self.lease_storage.look_up(time_to_live_req.id) {
                Some(lease) => lease,
                None => return Err(tonic::Status::not_found("Lease not found")),
            };

            let keys = time_to_live_req
                .keys
                .then(|| lease.keys())
                .unwrap_or_default();
            let res = LeaseTimeToLiveResponse {
                header: Some(self.lease_storage.gen_header()),
                id: time_to_live_req.id,
                ttl: lease.remaining().as_secs().cast(),
                granted_ttl: lease.ttl().as_secs().cast(),
                keys,
            };
            Ok(tonic::Response::new(res))
        } else {
            let leader_addr = self.state.wait_leader().await?;
            let mut lease_client = LeaseClient::connect(format!("http://{leader_addr}"))
                .await
                .map_err(|e| tonic::Status::internal(format!("Connect to leader error: {e}")))?;
            lease_client.lease_time_to_live(request).await
        }
    }

    /// LeaseLeases lists all existing leases.
    async fn lease_leases(
        &self,
        request: tonic::Request<LeaseLeasesRequest>,
    ) -> Result<tonic::Response<LeaseLeasesResponse>, tonic::Status> {
        debug!("Receive LeaseLeasesRequest {:?}", request);
        let leases = self
            .lease_storage
            .leases()
            .into_iter()
            .map(|lease| LeaseStatus { id: lease.id() })
            .collect();
        let res = LeaseLeasesResponse {
            header: Some(self.lease_storage.gen_header()),
            leases,
        };
        Ok(tonic::Response::new(res))
    }
}
