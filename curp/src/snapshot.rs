use std::fmt::Debug;

use engine::snapshot_api::SnapshotProxy;

/// Snapshot
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// Snapshot metadata
    pub(crate) meta: SnapshotMeta,
    /// Snapshot
    inner: SnapshotProxy,
}

impl Snapshot {
    /// Create a new snapshot
    pub(crate) fn new(meta: SnapshotMeta, inner: SnapshotProxy) -> Self {
        Self { meta, inner }
    }

    /// Into inner snapshot
    pub(crate) fn into_inner(self) -> SnapshotProxy {
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
