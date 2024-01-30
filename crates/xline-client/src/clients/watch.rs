use std::{fmt::Debug, sync::Arc};

use futures::channel::mpsc::channel;
use tonic::transport::Channel;
use xlineapi::{self, RequestUnion};

use crate::{
    error::{Result, XlineClientError},
    types::watch::{WatchRequest, WatchStreaming, Watcher},
    AuthService,
};

/// Channel size for watch request stream
const CHANNEL_SIZE: usize = 128;

/// Client for Watch operations.
#[derive(Clone, Debug)]
pub struct WatchClient {
    /// The watch RPC client, only communicate with one server at a time
    #[cfg(not(madsim))]
    inner: xlineapi::WatchClient<AuthService<Channel>>,
    /// The watch RPC client, only communicate with one server at a time
    #[cfg(madsim)]
    inner: xlineapi::WatchClient<Channel>,
}

impl WatchClient {
    /// Creates a new maintenance client
    #[inline]
    #[must_use]
    pub fn new(channel: Channel, token: Option<String>) -> Self {
        Self {
            inner: xlineapi::WatchClient::new(AuthService::new(
                channel,
                token.and_then(|t| t.parse().ok().map(Arc::new)),
            )),
        }
    }

    /// Watches for events happening or that have happened. Both input and output
    /// are streams; the input stream is for creating and canceling watcher and the output
    /// stream sends events. The entire event history can be watched starting from the
    /// last compaction revision.
    ///
    /// # Errors
    ///
    /// This function will return an error if the RPC client fails to send request
    ///
    /// # Panics
    ///
    /// This function will panic if the RPC server doesn't return a create watch response
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{
    ///     types::{kv::PutRequest, watch::WatchRequest},
    ///     Client, ClientOptions,
    /// };
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default()).await?;
    ///     let mut watch_client = client.watch_client();
    ///     let mut kv_client = client.kv_client();
    ///
    ///     let (mut watcher, mut stream) = watch_client.watch(WatchRequest::new("key1")).await?;
    ///     kv_client.put(PutRequest::new("key1", "value1")).await?;
    ///
    ///     let resp = stream.message().await?.unwrap();
    ///     let kv = resp.events[0].kv.as_ref().unwrap();
    ///
    ///     println!(
    ///         "got key: {}, value: {}",
    ///         String::from_utf8_lossy(&kv.key),
    ///         String::from_utf8_lossy(&kv.value)
    ///     );
    ///
    ///     // cancel the watch
    ///     watcher.cancel()?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn watch(&mut self, request: WatchRequest) -> Result<(Watcher, WatchStreaming)> {
        let (mut request_sender, request_receiver) =
            channel::<xlineapi::WatchRequest>(CHANNEL_SIZE);

        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(request.into())),
        };

        request_sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))?;

        let mut response_stream = self.inner.watch(request_receiver).await?.into_inner();

        let watch_id = match response_stream.message().await? {
            Some(resp) => {
                assert!(resp.created, "not a create watch response");
                resp.watch_id
            }
            None => {
                return Err(XlineClientError::WatchError(String::from(
                    "failed to create watch",
                )));
            }
        };

        Ok((
            Watcher::new(watch_id, request_sender.clone()),
            WatchStreaming::new(response_stream, request_sender),
        ))
    }
}
