use std::{env::temp_dir, error::Error};

use crate::{api::snapshot_api::SnapshotAllocator, EngineType, Snapshot};

/// Rocks snapshot allocator
#[derive(Debug, Copy, Clone, Default)]
#[non_exhaustive]
#[allow(clippy::module_name_repetitions)]
pub struct RocksSnapshotAllocator;

#[async_trait::async_trait]
impl SnapshotAllocator for RocksSnapshotAllocator {
    #[inline]
    async fn allocate_new_snapshot(&self) -> Result<Snapshot, Box<dyn Error>> {
        let tmp_path = temp_dir().join(format!("snapshot-{}", uuid::Uuid::new_v4()));
        Ok(Snapshot::new_for_receiving(EngineType::Rocks(tmp_path))?)
    }
}

/// Memory snapshot allocator
#[derive(Debug, Copy, Clone, Default)]
#[non_exhaustive]
#[allow(clippy::module_name_repetitions)]
pub struct MemorySnapshotAllocator;

#[async_trait::async_trait]
impl SnapshotAllocator for MemorySnapshotAllocator {
    #[inline]
    async fn allocate_new_snapshot(&self) -> Result<Snapshot, Box<dyn Error>> {
        Ok(Snapshot::new_for_receiving(EngineType::Memory)?)
    }
}
