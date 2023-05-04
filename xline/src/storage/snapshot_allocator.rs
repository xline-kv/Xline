use std::error::Error;

use async_trait::async_trait;
use curp::SnapshotAllocator;
use engine::snapshot_api::{MemorySnapshot, RocksSnapshot, SnapshotProxy};

/// Rocks snapshot allocator
pub(crate) struct RocksSnapshotAllocator;

#[async_trait]
impl SnapshotAllocator for RocksSnapshotAllocator {
    async fn allocate_new_snapshot(&self) -> Result<SnapshotProxy, Box<dyn Error>> {
        let tmp_path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
        Ok(SnapshotProxy::Rocks(RocksSnapshot::new_for_receiving(
            tmp_path,
        )?))
    }
}

/// Memory snapshot allocator
pub(crate) struct MemorySnapshotAllocator;

#[async_trait]
impl SnapshotAllocator for MemorySnapshotAllocator {
    async fn allocate_new_snapshot(&self) -> Result<SnapshotProxy, Box<dyn Error>> {
        Ok(SnapshotProxy::Memory(MemorySnapshot::default()))
    }
}
