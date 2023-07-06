// TODO: Remove these when the placeholder is implemented.
#![allow(missing_copy_implementations)]
#![allow(clippy::new_without_default)]

/// Client for Election operations.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ElectionClient;

impl ElectionClient {
    /// Create a new election client
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}
