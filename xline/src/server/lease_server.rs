use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clippy_utilities::Cast;
use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use log::{debug, warn};
use parking_lot::Mutex;
use tokio::{sync::mpsc, time};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use uuid::Uuid;

use super::{
    auth_server::get_token,
    command::{Command, CommandResponse, KeyRange, SyncResponse},
};
use crate::{
    rpc::{
        LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
        LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
        LeaseStatus, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, LeaseTrait, RequestWithToken,
        RequestWrapper,
    },
    storage::LeaseStore,
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

/// Lease Server
#[derive(Debug)]
#[allow(dead_code)] // Remove this after feature is completed
pub(crate) struct LeaseServer {
    /// Lease storage
    storage: Arc<LeaseStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
    /// Current node is leader or not
    is_leader: Arc<Mutex<bool>>,
}

impl LeaseServer {
    /// New `LeaseServer`
    pub(crate) fn new(
        lease_storage: Arc<LeaseStore>,
        client: Arc<Client<Command>>,
        name: String,
        is_leader: Arc<Mutex<bool>>,
    ) -> Arc<Self> {
        let lease_server = Arc::new(Self {
            storage: lease_storage,
            client,
            name,
            is_leader,
        });

        let _h = tokio::spawn({
            let server = Arc::clone(&lease_server);
            async move {
                loop {
                    // only leader will check expired lease
                    if server.is_leader() {
                        for id in server.find_expired_leases() {
                            let _handle = tokio::spawn({
                                let s = Arc::clone(&server);
                                async move {
                                    let request = tonic::Request::new(LeaseRevokeRequest { id });
                                    if let Err(e) = s.lease_revoke(request).await {
                                        warn!("Failed to revoke expired leases: {}", e);
                                    }
                                }
                            });
                        }
                    }

                    time::sleep(Duration::from_millis(500)).await;
                }
            }
        });

        lease_server
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
            self.storage
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
        *self.is_leader.lock()
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

    /// Find expired leases
    fn find_expired_leases(&self) -> Vec<i64> {
        self.storage.find_expired_leases()
    }
}

#[tonic::async_trait]
impl LeaseTrait for LeaseServer {
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
            lease_grant_req.id = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {}", e))
                .as_secs()
                .cast(); // TODO: generate lease unique id
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
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        if self.is_leader() {
            let mut req_stream = request.into_inner();
            let _hd = tokio::spawn({
                let lease_storage = Arc::clone(&self.storage);
                async move {
                    while let Some(req_result) = req_stream.next().await {
                        match req_result {
                            Ok(keep_alive_req) => {
                                debug!("Receive LeaseKeepAliveRequest {:?}", keep_alive_req);
                                // TODO wait applied index
                                let res = lease_storage.keep_alive(keep_alive_req.id).map(|ttl| {
                                    LeaseKeepAliveResponse {
                                        id: keep_alive_req.id,
                                        ttl,
                                        ..LeaseKeepAliveResponse::default()
                                    }
                                });
                                assert!(tx.send(res).await.is_ok(), "Command receiver dropped");
                            }
                            Err(e) => {
                                warn!("Receive LeaseKeepAliveRequest error {:?}", e);
                                break;
                            }
                        }
                    }
                }
            });
        } else {
            //TODO: forward to leader
        }
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
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
            let lease = match self.storage.look_up(time_to_live_req.id) {
                Some(lease) => lease,
                None => return Err(tonic::Status::not_found("Lease not found")),
            };

            let keys = time_to_live_req
                .keys
                .then(|| lease.keys())
                .unwrap_or_default();
            let res = LeaseTimeToLiveResponse {
                header: Some(self.storage.gen_header()),
                id: time_to_live_req.id,
                ttl: lease.remaining().as_secs().cast(),
                granted_ttl: lease.ttl().as_secs().cast(),
                keys,
            };
            return Ok(tonic::Response::new(res));
        }
        // TODO: forward to leader
        Err(tonic::Status::unimplemented("not leader"))
    }

    /// LeaseLeases lists all existing leases.
    async fn lease_leases(
        &self,
        request: tonic::Request<LeaseLeasesRequest>,
    ) -> Result<tonic::Response<LeaseLeasesResponse>, tonic::Status> {
        debug!("Receive LeaseLeasesRequest {:?}", request);
        let leases = self
            .storage
            .leases()
            .into_iter()
            .map(|lease| LeaseStatus { id: lease.id() })
            .collect();
        let res = LeaseLeasesResponse {
            header: Some(self.storage.gen_header()),
            leases,
        };
        Ok(tonic::Response::new(res))
    }
}
