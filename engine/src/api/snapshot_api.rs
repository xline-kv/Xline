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
