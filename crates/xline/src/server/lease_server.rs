use std::{pin::Pin, sync::Arc, time::Duration};

use async_stream::{stream, try_stream};
use clippy_utilities::NumericCast;
use curp::members::ClusterInfo;
use futures::stream::Stream;
use tokio::time;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tracing::{debug, warn};
#[cfg(madsim)]
use utils::ClientTlsConfig;
use utils::{
    build_endpoint,
    task_manager::{tasks::TaskName, Listener, TaskManager},
};
use xlineapi::{
    command::{Command, CommandResponse, CurpClient, KeyRange, SyncResponse},
    execute_error::ExecuteError,
};

use crate::{
    id_gen::IdGenerator,
    metrics,
    rpc::{
        Lease, LeaseClient, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
        LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest,
        LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, RequestWrapper,
    },
    storage::{storage_api::StorageApi, AuthStore, LeaseStore},
};

/// Default Lease Request Time
const DEFAULT_LEASE_REQUEST_TIME: Duration = Duration::from_millis(500);

/// Lease Server
pub(crate) struct LeaseServer<S>
where
    S: StorageApi,
{
    /// Lease storage
    lease_storage: Arc<LeaseStore<S>>,
    /// Auth storage
    auth_storage: Arc<AuthStore<S>>,
    /// Consensus client
    client: Arc<CurpClient>,
    /// Id generator
    id_gen: Arc<IdGenerator>,
    /// cluster information
    cluster_info: Arc<ClusterInfo>,
    /// Client tls config
    client_tls_config: Option<ClientTlsConfig>,
    /// Task manager
    task_manager: Arc<TaskManager>,
}

impl<S> LeaseServer<S>
where
    S: StorageApi,
{
    /// New `LeaseServer`
    pub(crate) fn new(
        lease_storage: Arc<LeaseStore<S>>,
        auth_storage: Arc<AuthStore<S>>,
        client: Arc<CurpClient>,
        id_gen: Arc<IdGenerator>,
        cluster_info: Arc<ClusterInfo>,
        client_tls_config: Option<ClientTlsConfig>,
        task_manager: &Arc<TaskManager>,
    ) -> Arc<Self> {
        let lease_server = Arc::new(Self {
            lease_storage,
            auth_storage,
            client,
            id_gen,
            cluster_info,
            client_tls_config,
            task_manager: Arc::clone(task_manager),
        });
        task_manager.spawn(TaskName::RevokeExpiredLeases, |n| {
            Self::revoke_expired_leases_task(Arc::clone(&lease_server), n)
        });
        lease_server
    }

    /// Task of revoke expired leases
    #[allow(clippy::arithmetic_side_effects)] // Introduced by tokio::select!
    async fn revoke_expired_leases_task(
        lease_server: Arc<LeaseServer<S>>,
        shutdown_listener: Listener,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_listener.wait() => return ,
                _ = time::sleep(DEFAULT_LEASE_REQUEST_TIME) => {}
            }
            // only leader will check expired lease
            if lease_server.lease_storage.is_primary() {
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
                                        panic!("metadata value parse error: {e}")
                                    }),
                                );
                            }
                            if let Err(e) = s.lease_revoke(request).await {
                                warn!("Failed to revoke expired leases: {}", e);
                            }
                        }
                    });
                }
            }
        }
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
        let auth_info = self.auth_storage.try_get_auth_info_from_request(&request)?;
        let request = request.into_inner().into();
        let keys = {
            if let RequestWrapper::LeaseRevokeRequest(ref req) = request {
                self.lease_storage
                    .get_keys(req.id)
                    .into_iter()
                    .map(|k| KeyRange::new(k, ""))
                    .collect()
            } else {
                vec![]
            }
        };
        let cmd = Command::new_with_auth_info(keys, request, auth_info);
        let res = self.client.propose(&cmd, None, use_fast_path).await??;
        Ok(res)
    }

    /// Handle keep alive at leader
    #[allow(clippy::arithmetic_side_effects)] // Introduced by tokio::select!
    async fn leader_keep_alive(
        &self,
        mut request_stream: tonic::Streaming<LeaseKeepAliveRequest>,
    ) -> Pin<Box<dyn Stream<Item = Result<LeaseKeepAliveResponse, tonic::Status>> + Send>> {
        let shutdown_listener = self
            .task_manager
            .get_shutdown_listener(TaskName::LeaseKeepAlive);
        let lease_storage = Arc::clone(&self.lease_storage);
        let stream = try_stream! {
           loop {
                let keep_alive_req: LeaseKeepAliveRequest = tokio::select! {
                    _ = shutdown_listener.wait() => {
                        debug!("Lease keep alive shutdown");
                        break;
                    }
                    res = request_stream.message() => {
                        if let Ok(Some(keep_alive_req)) = res {
                            keep_alive_req
                        } else {
                            break;
                        }
                    }
                };
                debug!("Receive LeaseKeepAliveRequest {:?}", keep_alive_req);
                let ttl = if lease_storage.is_primary() {
                    tokio::select! {
                        _ = shutdown_listener.wait() => {
                            debug!("Lease keep alive shutdown");
                            break;
                        }
                        _ = lease_storage.wait_synced(keep_alive_req.id) => {
                        }
                    };
                    lease_storage.keep_alive(keep_alive_req.id).map_err(Into::into)
                } else {
                    Err(tonic::Status::failed_precondition("current node is not a leader"))
                }?;
                yield LeaseKeepAliveResponse {
                    id: keep_alive_req.id,
                    ttl,
                    ..LeaseKeepAliveResponse::default()
                };
            }
        };
        Box::pin(stream)
    }

    /// Handle keep alive at follower
    #[allow(clippy::arithmetic_side_effects)] // Introduced by tokio::select!
    async fn follower_keep_alive(
        &self,
        mut request_stream: tonic::Streaming<LeaseKeepAliveRequest>,
        leader_addrs: &[String],
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<LeaseKeepAliveResponse, tonic::Status>> + Send>>,
        tonic::Status,
    > {
        let shutdown_listener = self
            .task_manager
            .get_shutdown_listener(TaskName::LeaseKeepAlive);
        let endpoints = build_endpoints(leader_addrs, self.client_tls_config.as_ref())?;
        let channel = tonic::transport::Channel::balance_list(endpoints.into_iter());
        let mut lease_client = LeaseClient::new(channel);

        let redirect_stream = stream! {
            loop {
                tokio::select! {
                    _ = shutdown_listener.wait() => {
                        debug!("Lease keep alive shutdown");
                        break;
                    }
                    res = request_stream.message() => {
                        if let Ok(Some(keep_alive_req)) = res {
                            yield keep_alive_req;
                        } else {
                            break;
                        }
                    }
                }
            }

        };

        let stream = lease_client
            .lease_keep_alive(redirect_stream)
            .await?
            .into_inner();

        Ok(Box::pin(stream))
    }
}

/// Build endpoints from addresses
fn build_endpoints(
    addrs: &[String],
    tls_config: Option<&ClientTlsConfig>,
) -> Result<Vec<Endpoint>, tonic::Status> {
    addrs
        .iter()
        .map(|addr| {
            let endpoint = build_endpoint(addr, tls_config)
                .map_err(|e| tonic::Status::internal(e.to_string()))?;
            Ok(endpoint)
        })
        .collect()
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
            lease_grant_req.id = self.id_gen.next();
        }

        let is_fast_path = true;
        let (res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: LeaseGrantResponse = res.into_inner().into();
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for LeaseGrantResponse", revision);
            if let Some(header) = res.header.as_mut() {
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

        let mut res: LeaseRevokeResponse = res.into_inner().into();
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for LeaseRevokeResponse", revision);
            if let Some(header) = res.header.as_mut() {
                header.revision = revision;
            }
            metrics::get().lease_expired_total.add(1, &[]);
        }
        Ok(tonic::Response::new(res))
    }

    ///Server streaming response type for the LeaseKeepAlive method.
    type LeaseKeepAliveStream =
        Pin<Box<dyn Stream<Item = Result<LeaseKeepAliveResponse, tonic::Status>> + Send>>;

    /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
    /// to the server and streaming keep alive responses from the server to the client.
    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        debug!("Receive LeaseKeepAliveRequest {:?}", request);
        let request_stream = request.into_inner();
        let stream = loop {
            if self.lease_storage.is_primary() {
                break self.leader_keep_alive(request_stream).await;
            }
            let leader_id = self.client.fetch_leader_id(false).await?;
            // Given that a candidate server may become a leader when it won the election or
            // a follower when it lost the election. Therefore we need to double check here.
            // We can directly invoke leader_keep_alive when a candidate becomes a leader.
            if !self.lease_storage.is_primary() {
                let leader_addrs = self.cluster_info.client_urls(leader_id).unwrap_or_else(|| {
                    unreachable!(
                        "The address of leader {} not found in all_members {:?}",
                        leader_id, self.cluster_info
                    )
                });
                break self
                    .follower_keep_alive(request_stream, &leader_addrs)
                    .await?;
            }
        };
        Ok(tonic::Response::new(stream))
    }

    /// LeaseTimeToLive retrieves lease information.
    async fn lease_time_to_live(
        &self,
        request: tonic::Request<LeaseTimeToLiveRequest>,
    ) -> Result<tonic::Response<LeaseTimeToLiveResponse>, tonic::Status> {
        debug!("Receive LeaseTimeToLiveRequest {:?}", request);
        loop {
            if self.lease_storage.is_primary() {
                let time_to_live_req = request.into_inner();

                self.lease_storage.wait_synced(time_to_live_req.id).await;

                let Some(lease) = self.lease_storage.look_up(time_to_live_req.id) else {
                    return Err(ExecuteError::LeaseNotFound(time_to_live_req.id).into());
                };

                let keys = time_to_live_req
                    .keys
                    .then(|| lease.keys())
                    .unwrap_or_default();
                let res = LeaseTimeToLiveResponse {
                    header: Some(self.lease_storage.gen_header()),
                    id: time_to_live_req.id,
                    ttl: lease.remaining().as_secs().numeric_cast(),
                    granted_ttl: lease.ttl().as_secs().numeric_cast(),
                    keys,
                };
                return Ok(tonic::Response::new(res));
            }
            let leader_id = self.client.fetch_leader_id(false).await?;
            let leader_addrs = self.cluster_info.client_urls(leader_id).unwrap_or_else(|| {
                unreachable!(
                    "The address of leader {} not found in all_members {:?}",
                    leader_id, self.cluster_info
                )
            });
            if !self.lease_storage.is_primary() {
                let endpoints = build_endpoints(&leader_addrs, self.client_tls_config.as_ref())?;
                let channel = tonic::transport::Channel::balance_list(endpoints.into_iter());
                let mut lease_client = LeaseClient::new(channel);
                return lease_client.lease_time_to_live(request).await;
            }
        }
    }

    /// LeaseLeases lists all existing leases.
    async fn lease_leases(
        &self,
        request: tonic::Request<LeaseLeasesRequest>,
    ) -> Result<tonic::Response<LeaseLeasesResponse>, tonic::Status> {
        debug!("Receive LeaseLeasesRequest {:?}", request);

        let is_fast_path = true;
        let (res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: LeaseLeasesResponse = res.into_inner().into();
        if let Some(sync_res) = sync_res {
            let revision = sync_res.revision();
            debug!("Get revision {:?} for LeaseLeasesResponse", revision);
            if let Some(header) = res.header.as_mut() {
                header.revision = revision;
            }
        }
        Ok(tonic::Response::new(res))
    }
}
