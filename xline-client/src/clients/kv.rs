// TODO: Remove these when the placeholder is implemented.
#![allow(dead_code)]

use std::sync::Arc;

use curp::client::Client as CurpClient;
use xline::server::Command;

/// Client for KV operations.
#[derive(Clone, Debug)]
pub struct KvClient {
    /// Name of the KvClient
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// Auth token
    token: Option<String>,
}

impl KvClient {
    /// New `KvClient`
    #[inline]
    pub fn new(name: String, curp_client: Arc<CurpClient<Command>>, token: Option<String>) -> Self {
        Self {
            name,
            curp_client,
            token,
        }
    }
}
