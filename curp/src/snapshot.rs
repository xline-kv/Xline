use std::fmt::Debug;

#[allow(clippy::module_name_repetitions)] // it's re-exported in lib
pub use curp_external_api::snapshot::SnapshotAllocator;
use engine::Snapshot as EngineSnapshot;

/// Snapshot
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// Snapshot metadata
    pub(crate) meta: SnapshotMeta,
    /// Snapshot
    inner: EngineSnapshot,
}

impl Snapshot {
    /// Create a new snapshot
    pub(crate) fn new(meta: SnapshotMeta, inner: EngineSnapshot) -> Self {
        Self { meta, inner }
    }

    /// Into inner snapshot
    pub(crate) fn into_inner(self) -> EngineSnapshot {
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
