use std::error::Error;

use async_trait::async_trait;
use engine::Snapshot as EngineSnapshot;

/// The snapshot allocation is handled by the upper-level application
#[allow(clippy::module_name_repetitions)] // it's re-exported in lib
#[async_trait]
pub trait SnapshotAllocator: Send + Sync {
    /// Allocate a new snapshot
    async fn allocate_new_snapshot(&self) -> Result<EngineSnapshot, Box<dyn Error>>;
}
