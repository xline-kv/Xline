use std::error::Error;

use async_trait::async_trait;
use curp::SnapshotAllocator;
use engine::{EngineType, Snapshot};

/// Rocks snapshot allocator
pub(crate) struct RocksSnapshotAllocator;

#[async_trait]
impl SnapshotAllocator for RocksSnapshotAllocator {
    async fn allocate_new_snapshot(&self) -> Result<Snapshot, Box<dyn Error>> {
        let tmp_path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
        Ok(Snapshot::new_for_receiving(EngineType::Rocks(
            tmp_path.into(),
        ))?)
    }
}

/// Memory snapshot allocator
pub(crate) struct MemorySnapshotAllocator;

#[async_trait]
impl SnapshotAllocator for MemorySnapshotAllocator {
    async fn allocate_new_snapshot(&self) -> Result<Snapshot, Box<dyn Error>> {
        Ok(Snapshot::new_for_receiving(EngineType::Memory)?)
    }
}
