// TODO: add memory snapshot allocator
use std::error::Error;

use async_trait::async_trait;
use curp::SnapshotAllocator;
use engine::snapshot_api::{MemorySnapshot, SnapshotProxy};

/// Rocks snapshot allocator
pub(crate) struct RocksSnapshotAllocator;

#[async_trait]
impl SnapshotAllocator for RocksSnapshotAllocator {
    #[allow(clippy::todo)]
    async fn allocate_new_snapshot(&self) -> Result<SnapshotProxy, Box<dyn Error>> {
        // TODO: create real rocks db snapshot
        todo!()
    }
}

/// Memory snapshot allocator
pub(crate) struct MemorySnapshotAllocator;

#[async_trait]
impl SnapshotAllocator for MemorySnapshotAllocator {
    #[allow(clippy::todo)]
    async fn allocate_new_snapshot(&self) -> Result<SnapshotProxy, Box<dyn Error>> {
        Ok(SnapshotProxy::Memory(MemorySnapshot::default()))
    }
}
