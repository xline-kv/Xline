// TODO: Remove these when the placeholder is implemented.
#![allow(dead_code)]

use std::sync::Arc;

use curp::client::Client as CurpClient;
use tonic::transport::Channel;
use xline::server::Command;

use crate::AuthService;

/// Client for Lease operations.
#[derive(Clone, Debug)]
pub struct LeaseClient {
    /// Name of the LeaseClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The lease RPC client, only communicate with one server at a time
    lease_client: xlineapi::LeaseClient<AuthService<Channel>>,
    /// Auth token
    token: Option<String>,
}

impl LeaseClient {
    /// New `LeaseClient`
    #[inline]
    pub fn new(
        name: String,
        curp_client: Arc<CurpClient<Command>>,
        channel: Channel,
        token: Option<String>,
    ) -> Self {
        Self {
            name,
            curp_client,
            lease_client: xlineapi::LeaseClient::new(AuthService::new(
                channel,
                token.as_ref().and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            token,
        }
    }
}
