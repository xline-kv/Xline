use parking_lot::RwLock;

/// Revision number generator
#[derive(Debug)]
pub(crate) struct RevisionNumberGenerator {
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

    /// Get the next pre-allocated revision number
    pub(crate) fn next(&self) -> i64 {
        let mut inner_w = self.inner.write();
        inner_w.pre_revision += 1;
        inner_w.pre_revision
    }

    /// Commit the revision number
    pub(crate) fn next_commit(&self) -> i64 {
        let mut inner_w = self.inner.write();
        inner_w.revision += 1;
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
}
