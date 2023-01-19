use std::sync::atomic::{AtomicI64, Ordering};

/// Revision number
#[derive(Debug)]
pub(crate) struct RevisionNumber(AtomicI64);

impl RevisionNumber {
    /// Create a new revision
    pub(crate) fn new() -> Self {
        Self(AtomicI64::new(1))
    }
    /// Get the revision number
    pub(crate) fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
    /// Get the next revision number
    pub(crate) fn next(&self) -> i64 {
        self.0.fetch_add(1, Ordering::SeqCst).wrapping_add(1)
    }
}
