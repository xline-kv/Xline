use std::fmt::Debug;

use engine::engine_api::SnapshotApi;

/// Snapshot
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// Snapshot metadata
    pub(crate) meta: SnapshotMeta,
    /// Snapshot
    inner: Box<dyn SnapshotApi>,
}

impl Snapshot {
    /// Create a new snapshot
    pub(crate) fn new(meta: SnapshotMeta, snapshot: Box<dyn SnapshotApi>) -> Self {
        Self {
            meta,
            inner: snapshot,
        }
    }

    /// Into inner snapshot
    pub(crate) fn into_inner(self) -> Box<dyn SnapshotApi> {
        self.inner
    }
}

/// Metadata for snapshot
#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotMeta {
    /// Last included index
    pub(crate) last_included_index: u64,
    /// Last included term
    pub(crate) last_included_term: u64,
}
