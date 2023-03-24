use std::{error::Error, fmt::Debug};

use async_trait::async_trait;
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

/// The snapshot allocation is handled by the upper-level application
#[allow(clippy::module_name_repetitions)] // it's re-exported in lib
#[async_trait]
pub trait SnapshotAllocator: Send + Sync {
    /// Allocate a new snapshot
    async fn allocate_new_snapshot(&self) -> Result<SnapshotProxy, Box<dyn Error>>;
}
