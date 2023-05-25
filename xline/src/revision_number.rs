use std::sync::atomic::{AtomicI64, Ordering};

/// Revision number
#[derive(Debug)]
pub(crate) struct RevisionNumberGenerator(AtomicI64);

impl RevisionNumberGenerator {
    /// Create a new revision
    pub(crate) fn new(rev: i64) -> Self {
        Self(AtomicI64::new(rev))
    }

    /// Get the revision number
    pub(crate) fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Get the next revision number
    pub(crate) fn next(&self) -> i64 {
        self.0.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    /// Set the revision number
    pub(crate) fn set(&self, rev: i64) {
        self.0.store(rev, Ordering::Relaxed);
    }
}

impl Default for RevisionNumberGenerator {
    #[inline]
    fn default() -> Self {
        RevisionNumberGenerator::new(1)
    }
}
