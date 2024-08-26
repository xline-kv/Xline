use std::sync::atomic::{AtomicI64, Ordering};

/// Revision number
#[derive(Debug)]
pub(crate) struct RevisionNumberGenerator {
    /// The current revision number
    current: AtomicI64,
}

impl RevisionNumberGenerator {
    /// Create a new revision
    pub(crate) fn new(rev: i64) -> Self {
        Self {
            current: AtomicI64::new(rev),
        }
    }

    /// Get the current revision number
    pub(crate) fn get(&self) -> i64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Set the revision number
    pub(crate) fn set(&self, rev: i64) {
        self.current.store(rev, Ordering::Relaxed);
    }

    /// Gets a temporary state
    pub(crate) fn state(&self) -> RevisionNumberGeneratorState {
        RevisionNumberGeneratorState {
            current: &self.current,
            next: AtomicI64::new(self.get()),
        }
    }
}

impl Default for RevisionNumberGenerator {
    #[inline]
    fn default() -> Self {
        RevisionNumberGenerator::new(1)
    }
}

/// Revision generator with temporary state
pub(crate) struct RevisionNumberGeneratorState<'a> {
    /// The current revision number
    current: &'a AtomicI64,
    /// Next revision number
    next: AtomicI64,
}

impl RevisionNumberGeneratorState<'_> {
    /// Get the current revision number
    pub(crate) fn get(&self) -> i64 {
        self.next.load(Ordering::Relaxed)
    }

    /// Increases the next revision number
    pub(crate) fn next(&self) -> i64 {
        self.next.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    /// Commit the revision number
    pub(crate) fn commit(&self) {
        self.current
            .store(self.next.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}
