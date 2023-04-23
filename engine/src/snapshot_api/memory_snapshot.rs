use std::io::{Cursor, Seek};

use clippy_utilities::NumericCast;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::SnapshotApi;

/// A snapshot of the `MemoryEngine`
#[derive(Debug, Default)]
pub struct MemorySnapshot {
    /// data of the snapshot
    data: Cursor<Vec<u8>>,
}

impl MemorySnapshot {
    /// Create a new `MemorySnapshot`
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self {
            data: Cursor::new(data),
        }
    }

    /// Get the inner data of the snapshot
    pub(crate) fn into_inner(self) -> Vec<u8> {
        self.data.into_inner()
    }
}

#[async_trait::async_trait]
impl SnapshotApi for MemorySnapshot {
    #[inline]
    fn size(&self) -> u64 {
        self.data.get_ref().len().numeric_cast()
    }

    #[inline]
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.data.read_exact(buf).await.map(drop)
    }

    #[inline]
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.data.write_all(buf).await
    }

    #[inline]
    fn rewind(&mut self) -> std::io::Result<()> {
        Seek::rewind(&mut self.data)
    }

    #[inline]
    async fn clean(&mut self) -> std::io::Result<()> {
        self.data.get_mut().clear();
        Ok(())
    }
}
