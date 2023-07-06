use std::{fmt::Debug, sync::Arc};

use futures::channel::mpsc::channel;
use tonic::transport::Channel;
use xlineapi::{self, RequestUnion, WatchResponse};

use crate::{
    error::{ClientError, Result},
    types::watch::{WatchRequest, Watcher},
    AuthService,
};

/// Channel size for watch request stream
const CHANNEL_SIZE: usize = 128;

/// Client for Watch operations.
#[derive(Clone, Debug)]
pub struct WatchClient {
    /// The watch RPC client, only communicate with one server at a time
    inner: xlineapi::WatchClient<AuthService<Channel>>,
}

impl WatchClient {
    /// Create a new maintenance client
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
    /// If the RPC client fails to send request
    ///
    /// # Panics
    ///
    /// If the RPC server returns the wrong value
    #[inline]
    pub async fn watch(
        &mut self,
        request: WatchRequest,
    ) -> Result<(Watcher, tonic::Streaming<WatchResponse>)> {
        let (mut request_sender, request_receiver) =
            channel::<xlineapi::WatchRequest>(CHANNEL_SIZE);

        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(request.into())),
        };

        request_sender
            .try_send(request)
            .map_err(|e| ClientError::WatchError(e.to_string()))?;

        let mut response_stream = self.inner.watch(request_receiver).await?.into_inner();

        let watch_id = match response_stream.message().await? {
            Some(resp) => {
                assert!(resp.created, "not a create watch response");
                resp.watch_id
            }
            None => {
                return Err(ClientError::WatchError(String::from(
                    "failed to create watch",
                )));
            }
        };

        Ok((Watcher::new(watch_id, request_sender), response_stream))
    }
}
