// TODO: Remove these when the placeholder is implemented.
#![allow(dead_code)]

use std::{fmt::Debug, sync::Arc};

use tonic::transport::Channel;

use crate::AuthService;

/// The maintenance client
#[derive(Clone, Debug)]
pub struct MaintenanceClient {
    /// The maintenance RPC client, only communicate with one server at a time
    inner: xlineapi::MaintenanceClient<AuthService<Channel>>,
}

impl MaintenanceClient {
    /// Create a new maintenance client
    #[inline]
    #[must_use]
    pub fn new(channel: Channel, token: Option<String>) -> Self {
        Self {
            inner: xlineapi::MaintenanceClient::new(AuthService::new(
                channel,
                token.and_then(|t| t.parse().ok().map(Arc::new)),
            )),
        }
    }
}
