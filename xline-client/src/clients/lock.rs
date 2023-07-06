// TODO: Remove these when the placeholder is implemented.
#![allow(dead_code)]

use std::sync::Arc;

use curp::client::Client as CurpClient;
use tonic::transport::Channel;
use xline::server::Command;

use crate::clients::watch::WatchClient;

/// Client for Lock operations.
#[derive(Clone, Debug)]
pub struct LockClient {
    /// Name of the LockClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The watch client
    watch_client: WatchClient,
    /// Auth token
    token: Option<String>,
}

impl LockClient {
    /// New `LockClient`
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
            watch_client: WatchClient::new(channel, token.clone()),
            token,
        }
    }
}
