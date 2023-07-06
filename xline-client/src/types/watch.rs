use std::fmt::Debug;

use futures::channel::mpsc::Sender;
use xline::server::KeyRange;
use xlineapi::{RequestUnion, WatchCancelRequest, WatchProgressRequest};

use crate::error::{ClientError, Result};

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
            .map_err(|e| ClientError::WatchError(e.to_string()))
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
            .map_err(|e| ClientError::WatchError(e.to_string()))
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
            .map_err(|e| ClientError::WatchError(e.to_string()))
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
            .map_err(|e| ClientError::WatchError(e.to_string()))
    }
}

/// Watch Request
#[derive(Clone, Debug)]
pub struct WatchRequest {
    /// inner watch create request
    inner: xlineapi::WatchCreateRequest,
}

impl WatchRequest {
    /// New `WatchRequest`
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

    /// Set `key` and `range_end` when with prefix
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

    /// Set `key` and `range_end` when with from key
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        if self.inner.key.is_empty() {
            self.inner.key = vec![0];
        }
        self.inner.range_end = vec![0];
        self
    }

    /// Set `range_end`
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.inner.range_end = range_end.into();
        self
    }

    /// Set `start_revision`
    #[inline]
    #[must_use]
    pub const fn with_start_revision(mut self, revision: i64) -> Self {
        self.inner.start_revision = revision;
        self
    }

    /// Set `progress_notify`
    #[inline]
    #[must_use]
    pub const fn with_progress_notify(mut self) -> Self {
        self.inner.progress_notify = true;
        self
    }

    /// Set `filters`
    #[inline]
    #[must_use]
    pub fn with_filters(mut self, filters: impl Into<Vec<WatchFilterType>>) -> Self {
        self.inner.filters = filters.into().into_iter().map(Into::into).collect();
        self
    }

    /// Set `prev_kv`
    #[inline]
    #[must_use]
    pub const fn with_prev_kv(mut self) -> Self {
        self.inner.prev_kv = true;
        self
    }

    /// Set `watch_id`
    #[inline]
    #[must_use]
    pub const fn with_watch_id(mut self, watch_id: i64) -> Self {
        self.inner.watch_id = watch_id;
        self
    }

    /// Set `fragment`
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
