// TODO: Remove these when the placeholder is implemented.
#![allow(missing_copy_implementations)]
#![allow(clippy::new_without_default)]

/// Client for Cluster operations.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClusterClient;

impl ClusterClient {
    /// Create a new cluster client
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}
