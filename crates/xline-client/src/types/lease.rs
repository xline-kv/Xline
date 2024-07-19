use futures::channel::mpsc::Sender;
pub use xlineapi::{
    LeaseGrantResponse, LeaseKeepAliveResponse, LeaseLeasesResponse, LeaseRevokeResponse,
    LeaseStatus, LeaseTimeToLiveResponse,
};

use crate::error::{Result, XlineClientError};

/// The lease keep alive handle.
#[derive(Debug)]
pub struct LeaseKeeper {
    /// lease id
    id: i64,
    /// sender to send keep alive request
    sender: Sender<xlineapi::LeaseKeepAliveRequest>,
}

impl LeaseKeeper {
    /// Creates a new `LeaseKeeper`.
    #[inline]
    #[must_use]
    pub fn new(id: i64, sender: Sender<xlineapi::LeaseKeepAliveRequest>) -> Self {
        Self { id, sender }
    }

    /// The lease id which user want to keep alive.
    #[inline]
    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// Sends a keep alive request and receive response
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner channel is closed
    #[inline]
    pub fn keep_alive(&mut self) -> Result<()> {
        self.sender
            .try_send(xlineapi::LeaseKeepAliveRequest { id: self.id })
            .map_err(|e| XlineClientError::LeaseError(e.to_string()))
    }
}
