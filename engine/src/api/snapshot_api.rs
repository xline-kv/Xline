use std::io::{Error, ErrorKind};

use bytes::{Bytes, BytesMut};

/// This trait is a abstraction of the snapshot, We can Read/Write the snapshot like a file.
#[async_trait::async_trait]
pub trait SnapshotApi: Send + Sync + std::fmt::Debug {
    /// Get the size of the snapshot
    fn size(&self) -> u64;

    /// Rewind the snapshot to the beginning
    fn rewind(&mut self) -> std::io::Result<()>;

    /// Pull some bytes of the snapshot to the given uninitialized buffer
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<()>;

    /// Read the exact capacity of the given uninitialized buffer
    #[inline]
    async fn read_buf_exact(&mut self, buf: &mut BytesMut) -> std::io::Result<()> {
        while buf.len() < buf.capacity() {
            let prev_len = buf.len();
            match self.read_buf(buf).await {
                Ok(()) => {}
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }

            if buf.len() == prev_len {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
        }

        Ok(())
    }

    /// Write the given buffer to the snapshot
    async fn write_all(&mut self, buf: Bytes) -> std::io::Result<()>;

    /// Clean files of current snapshot
    async fn clean(&mut self) -> std::io::Result<()>;
}
