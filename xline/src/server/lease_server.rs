use std::sync::Arc;

use curp::client::Client;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{debug, warn};

use super::command::Command;
use crate::{
    rpc::{
        Lease, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
        LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest,
        LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
    },
    storage::KvStore,
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;

/// Lease Server
//#[derive(Debug)]
#[allow(dead_code)] // Remove this after feature is completed
pub(crate) struct LeaseServer {
    /// KV storage
    storage: Arc<KvStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
}

impl LeaseServer {
    /// New `LeaseServer`
    pub(crate) fn new(storage: Arc<KvStore>, client: Arc<Client<Command>>, name: String) -> Self {
        Self {
            storage,
            client,
            name,
        }
    }
}

#[tonic::async_trait]
impl Lease for LeaseServer {
    /// LeaseGrant creates a lease which expires if the server does not receive a keepAlive
    /// within a given time to live period. All keys attached to the lease will be expired and
    /// deleted if the lease expires. Each expired key generates a delete event in the event history.
    async fn lease_grant(
        &self,
        request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        debug!("Receive LeaseGrantRequest {:?}", request);
        // TODO add real implementation
        Ok(tonic::Response::new(LeaseGrantResponse::default()))
    }

    /// LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
    async fn lease_revoke(
        &self,
        request: tonic::Request<LeaseRevokeRequest>,
    ) -> Result<tonic::Response<LeaseRevokeResponse>, tonic::Status> {
        debug!("Receive LeaseRevokeRequest {:?}", request);
        // TODO add real implementation
        Ok(tonic::Response::new(LeaseRevokeResponse::default()))
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
        // TODO: Temporary solution
        let mut req_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        let _hd = tokio::spawn(async move {
            while let Some(req_result) = req_stream.next().await {
                match req_result {
                    Ok(req) => {
                        debug!("Receive LeaseKeepAliveRequest {:?}", req);
                        assert!(
                            tx.send(Ok(LeaseKeepAliveResponse {
                                id: req.id,
                                ttl: 1,
                                ..LeaseKeepAliveResponse::default()
                            }))
                            .await
                            .is_ok(),
                            "Command receiver dropped"
                        );
                    }
                    Err(e) => {
                        warn!("Receive LeaseKeepAliveRequest error {:?}", e);
                        break;
                        //panic!("Failed to receive LeaseKeepAliveRequest {:?}", e),
                    }
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    /// LeaseTimeToLive retrieves lease information.
    async fn lease_time_to_live(
        &self,
        request: tonic::Request<LeaseTimeToLiveRequest>,
    ) -> Result<tonic::Response<LeaseTimeToLiveResponse>, tonic::Status> {
        debug!("Receive LeaseTimeToLiveRequest {:?}", request);
        // TODO add real implementation
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Not Implemented".to_owned(),
        ))
    }

    /// LeaseLeases lists all existing leases.
    async fn lease_leases(
        &self,
        request: tonic::Request<LeaseLeasesRequest>,
    ) -> Result<tonic::Response<LeaseLeasesResponse>, tonic::Status> {
        debug!("Receive LeaseLeasesRequest {:?}", request);
        // TODO add real implementation
        Err(tonic::Status::new(
            tonic::Code::Unimplemented,
            "Not Implemented".to_owned(),
        ))
    }
}
