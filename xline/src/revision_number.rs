use clippy_utilities::OverflowArithmetic;
use parking_lot::RwLock;

/// Revision number generator
#[derive(Debug)]
pub(crate) struct RevisionNumberGenerator {
    /// The inner generator
    inner: RwLock<RevisionNumberGeneratorInner>,
}

/// Revision number generator inner
#[derive(Debug)]
struct RevisionNumberGeneratorInner {
    /// The revision
    revision: i64,
    /// The pre-allocated revision
    pre_revision: i64,
}

impl RevisionNumberGenerator {
    /// Create a new revision
    pub(crate) fn new(revision: i64) -> Self {
        Self {
            inner: RwLock::new(RevisionNumberGeneratorInner {
                revision,
                pre_revision: revision,
            }),
        }
    }

    /// Get the revision number
    pub(crate) fn get(&self) -> i64 {
        self.inner.read().pre_revision
    }

    /// Get the next pre-allocated revision number
    pub(crate) fn next(&self) -> i64 {
        let mut inner_w = self.inner.write();
        inner_w.pre_revision = inner_w.pre_revision.overflow_add(1);
        inner_w.pre_revision
    }

    /// Commit the revision number
    pub(crate) fn next_commit(&self) -> i64 {
        let mut inner_w = self.inner.write();
        inner_w.revision = inner_w.revision.overflow_add(1);
        // Followers only call `next_commit`, so we also need to update `pre_revision`
        if inner_w.pre_revision < inner_w.revision {
            inner_w.pre_revision = inner_w.revision;
        }
        inner_w.revision
    }

    /// Revert the revision number
    pub(crate) fn reset(&self) -> i64 {
        let mut inner_w = self.inner.write();
        inner_w.pre_revision = inner_w.revision;
        inner_w.revision
    }

    /// Set the revision number
    pub(crate) fn set(&self, revision: i64) {
        let mut inner_w = self.inner.write();
        inner_w.revision = revision;
        inner_w.pre_revision = revision;
    }
}

impl Default for RevisionNumberGenerator {
    #[inline]
    fn default() -> Self {
        RevisionNumberGenerator::new(1)
    }
}
