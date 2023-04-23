pub use self::{memory_snapshot::MemorySnapshot, rocks_snapshot::RocksSnapshot};

/// Memory snapshot
mod memory_snapshot;
/// Rocksdb snapshot
mod rocks_snapshot;

/// This trait is a abstraction of the snapshot, We can Read/Write the snapshot like a file.
#[async_trait::async_trait]
pub trait SnapshotApi: Send + Sync + std::fmt::Debug {
    /// Get the size of the snapshot
    fn size(&self) -> u64;

    /// Rewind the snapshot to the beginning
    fn rewind(&mut self) -> std::io::Result<()>;

    /// Read the snapshot to the given buffer
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()>;

    /// Write the given buffer to the snapshot
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;

    /// Clean files of current snapshot
    async fn clean(&mut self) -> std::io::Result<()>;
}

/// Union of snapshot types, hiding the difference between different implementations
#[derive(Debug)]
#[non_exhaustive]
pub enum SnapshotProxy {
    /// Rocks snapshot
    Rocks(RocksSnapshot),
    /// Memory snapshot
    Memory(MemorySnapshot),
}

#[async_trait::async_trait]
impl SnapshotApi for SnapshotProxy {
    #[inline]
    fn size(&self) -> u64 {
        match *self {
            SnapshotProxy::Rocks(ref s) => s.size(),
            SnapshotProxy::Memory(ref s) => s.size(),
        }
    }

    #[inline]
    fn rewind(&mut self) -> std::io::Result<()> {
        match *self {
            SnapshotProxy::Rocks(ref mut s) => s.rewind(),
            SnapshotProxy::Memory(ref mut s) => s.rewind(),
        }
    }

    #[inline]
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match *self {
            SnapshotProxy::Rocks(ref mut s) => s.read_exact(buf).await,
            SnapshotProxy::Memory(ref mut s) => s.read_exact(buf).await,
        }
    }

    #[inline]
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match *self {
            SnapshotProxy::Rocks(ref mut s) => s.write_all(buf).await,
            SnapshotProxy::Memory(ref mut s) => s.write_all(buf).await,
        }
    }

    #[inline]
    async fn clean(&mut self) -> std::io::Result<()> {
        match *self {
            SnapshotProxy::Rocks(ref mut s) => s.clean().await,
            SnapshotProxy::Memory(ref mut s) => s.clean().await,
        }
    }
}
