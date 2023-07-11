use std::{fmt::Debug, sync::Arc};

use tonic::{transport::Channel, Streaming};
use xlineapi::{SnapshotRequest, SnapshotResponse};

use crate::{
    error::{ClientError, Result},
    AuthService,
};

/// Client for Maintenance operations.
#[derive(Clone, Debug)]
pub struct MaintenanceClient {
    /// The maintenance RPC client, only communicate with one server at a time
    inner: xlineapi::MaintenanceClient<AuthService<Channel>>,
}

impl MaintenanceClient {
    /// Creates a new maintenance client
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

    /// Gets a snapshot over a stream
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner RPC client encountered a propose failure
    #[inline]
    pub async fn snapshot(&mut self) -> Result<Streaming<SnapshotResponse>> {
        Ok(self
            .inner
            .snapshot(SnapshotRequest {})
            .await
            .map_err(Into::<ClientError>::into)?
            .into_inner())
    }
}
