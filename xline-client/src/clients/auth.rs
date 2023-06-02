// TODO: Remove these when the placeholder is implemented.
#![allow(dead_code)]

use std::sync::Arc;

use curp::client::Client as CurpClient;
use tonic::transport::Channel;
use xline::server::Command;

use crate::AuthService;

/// Client for Auth operations.
#[derive(Clone, Debug)]
pub struct AuthClient {
    /// Name of the AuthClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The auth RPC client, only communicate with one server at a time
    auth_client: xlineapi::AuthClient<AuthService<Channel>>,
    /// Auth token
    token: Option<String>,
}

impl AuthClient {
    /// New `AuthClient`
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
            auth_client: xlineapi::AuthClient::new(AuthService::new(
                channel,
                token.as_ref().and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            token,
        }
    }
}
