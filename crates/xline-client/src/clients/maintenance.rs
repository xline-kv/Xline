use std::{fmt::Debug, sync::Arc};

use tonic::{transport::Channel, Streaming};
use xlineapi::{
    AlarmRequest, AlarmResponse, SnapshotRequest, SnapshotResponse, StatusRequest, StatusResponse,
};

use crate::{error::Result, AuthService};

/// Client for Maintenance operations.
#[derive(Clone, Debug)]
pub struct MaintenanceClient {
    /// The maintenance RPC client, only communicate with one server at a time
    #[cfg(not(madsim))]
    inner: xlineapi::MaintenanceClient<AuthService<Channel>>,
    /// The maintenance RPC client, only communicate with one server at a time
    #[cfg(madsim)]
    inner: xlineapi::MaintenanceClient<Channel>,
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .maintenance_client();
    ///
    ///     // snapshot
    ///     let mut msg = client.snapshot().await?;
    ///     let mut snapshot = vec![];
    ///     loop {
    ///         if let Some(resp) = msg.message().await? {
    ///             snapshot.extend_from_slice(&resp.blob);
    ///             if resp.remaining_bytes == 0 {
    ///                 break;
    ///             }
    ///         }
    ///     }
    ///     println!("snapshot size: {}", snapshot.len());
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn snapshot(&mut self) -> Result<Streaming<SnapshotResponse>> {
        Ok(self.inner.snapshot(SnapshotRequest {}).await?.into_inner())
    }

    /// Sends a alarm request
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner RPC client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use xlineapi::{AlarmAction, AlarmRequest, AlarmType};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .maintenance_client();
    ///
    ///     client.alarm(AlarmRequest::new(AlarmAction::Get, 0, AlarmType::None)).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn alarm(&mut self, request: AlarmRequest) -> Result<AlarmResponse> {
        Ok(self.inner.alarm(request).await?.into_inner())
    }

    /// Sends a status request
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner RPC client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .maintenance_client();
    ///
    ///     client.status().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn status(&mut self) -> Result<StatusResponse> {
        Ok(self
            .inner
            .status(StatusRequest::default())
            .await?
            .into_inner())
    }
}
