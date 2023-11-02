use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use futures::channel::mpsc::Sender;
use xlineapi::{command::KeyRange, RequestUnion, WatchCancelRequest, WatchProgressRequest};
pub use xlineapi::{Event, EventType, KeyValue, WatchResponse};

use crate::error::{Result, XlineClientError};

/// The watching handle.
#[derive(Debug)]
pub struct Watcher {
    /// Id of the watcher
    watch_id: i64,
    /// The channel sender
    sender: Sender<xlineapi::WatchRequest>,
}

impl Watcher {
    /// Creates a new `Watcher`.
    #[inline]
    #[must_use]
    pub fn new(watch_id: i64, sender: Sender<xlineapi::WatchRequest>) -> Self {
        Self { watch_id, sender }
    }

    /// The ID of the watcher.
    #[inline]
    #[must_use]
    pub const fn watch_id(&self) -> i64 {
        self.watch_id
    }

    /// Watches for events happening or that have happened.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn watch(&mut self, request: WatchRequest) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(request.into())),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }

    /// Cancels this watcher.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn cancel(&mut self) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(WatchCancelRequest {
                watch_id: self.watch_id,
            })),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }

    /// Cancels watch by specified `watch_id`.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn cancel_by_id(&mut self, watch_id: i64) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::CancelRequest(WatchCancelRequest { watch_id })),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }

    /// Requests a watch stream progress status be sent in the watch response stream as soon as
    /// possible.
    ///
    /// # Errors
    ///
    /// If sender fails to send to channel
    #[inline]
    pub fn request_progress(&mut self) -> Result<()> {
        let request = xlineapi::WatchRequest {
            request_union: Some(RequestUnion::ProgressRequest(WatchProgressRequest {})),
        };

        self.sender
            .try_send(request)
            .map_err(|e| XlineClientError::WatchError(e.to_string()))
    }
}

/// Watch Request
#[derive(Clone, Debug, PartialEq)]
pub struct WatchRequest {
    /// Inner watch create request
    inner: xlineapi::WatchCreateRequest,
}

impl WatchRequest {
    /// Creates a New `WatchRequest`
    ///
    /// `key` is the key to register for watching.
    #[inline]
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            inner: xlineapi::WatchCreateRequest {
                key: key.into(),
                ..Default::default()
            },
        }
    }

    /// If set, Xline will watch all keys with the matching prefix
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
            self.inner.range_end = vec![0];
        } else {
            self.inner.range_end = KeyRange::get_prefix(&self.inner.key);
        }
        self
    }

    /// If set, Xline will watch all keys that are equal to or greater than the given key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// `range_end` is the end of the range [key, `range_end`) to watch. If `range_end` is not given,
    /// only the key argument is watched. If `range_end` is equal to '\0', all keys greater than
    /// or equal to the key argument are watched.
    /// If the `range_end` is one bit larger than the given key,
    /// then all keys with the prefix (the given key) will be watched.
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }

    /// Sets the start revision to watch from (inclusive). No `start_revision` is "now".
    #[inline]
    #[must_use]
    pub const fn with_start_revision(mut self, revision: i64) -> Self {
        self.inner.start_revision = revision;
        self
    }

    /// `progress_notify` is set so that the Xline server will periodically send a `WatchResponse` with no events to the new watcher if there are no recent events. It is useful when clients wish to recover a disconnected watcher starting from a recent known revision. The xline server may decide how often it will send notifications based on current load.
    #[inline]
    #[must_use]
    pub const fn with_progress_notify(mut self) -> Self {
        self.inner.progress_notify = true;
        self
    }

    /// `filters` filter the events on server side before it sends back to the watcher.
    #[inline]
    #[must_use]
    pub fn with_filters(mut self, filters: impl Into<Vec<WatchFilterType>>) -> Self {
        self.inner.filters = filters.into().into_iter().map(Into::into).collect();
        self
    }

    /// If `prev_kv` is set, created watcher gets the previous KV before the event happens.
    /// If the previous KV is already compacted, nothing will be returned.
    #[inline]
    #[must_use]
    pub const fn with_prev_kv(mut self) -> Self {
        self.inner.prev_kv = true;
        self
    }

    /// If `watch_id` is provided and non-zero, it will be assigned to this watcher.
    /// this can be used ensure that ordering is correct when creating multiple
    /// watchers on the same stream. Creating a watcher with an ID already in
    /// use on the stream will cause an error to be returned.
    #[inline]
    #[must_use]
    pub const fn with_watch_id(mut self, watch_id: i64) -> Self {
        self.inner.watch_id = watch_id;
        self
    }

    /// fragment enables splitting large revisions into multiple watch responses.
    #[inline]
    #[must_use]
    pub const fn with_fragment(mut self) -> Self {
        self.inner.fragment = true;
        self
    }
}

impl From<WatchRequest> for xlineapi::WatchCreateRequest {
    #[inline]
    fn from(request: WatchRequest) -> Self {
        request.inner
    }
}

/// Watch filter type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum WatchFilterType {
    /// Filter out put event.
    NoPut = 0,
    /// Filter out delete event.
    NoDelete = 1,
}

impl From<WatchFilterType> for i32 {
    #[inline]
    fn from(value: WatchFilterType) -> Self {
        match value {
            WatchFilterType::NoPut => 0,
            WatchFilterType::NoDelete => 1,
        }
    }
}

/// Watch response stream
#[derive(Debug)]
pub struct WatchStreaming {
    /// Inner tonic stream
    inner: tonic::Streaming<WatchResponse>,
    /// A sender of WatchResponse, used to keep response stream alive
    _sender: Sender<xlineapi::WatchRequest>,
}

impl WatchStreaming {
    /// Create a new watch streaming
    #[inline]
    #[must_use]
    pub fn new(
        inner: tonic::Streaming<WatchResponse>,
        sender: Sender<xlineapi::WatchRequest>,
    ) -> Self {
        Self {
            inner,
            _sender: sender,
        }
    }
}

impl Deref for WatchStreaming {
    type Target = tonic::Streaming<WatchResponse>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for WatchStreaming {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
