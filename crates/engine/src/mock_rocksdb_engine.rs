use std::{
    fs,
    path::{Path, PathBuf},
};

use bytes::{Bytes, BytesMut};

use crate::{
    api::{
        engine_api::{StorageEngine, WriteOperation},
        snapshot_api::SnapshotApi,
    },
    error::EngineError,
    memory_engine::{MemoryEngine, MemorySnapshot},
};

/// Mock `RocksDB` Storage Engine
#[derive(Debug)]
pub struct RocksEngine {
    /// The inner storage engine of Mock `RocksDB`
    inner: MemoryEngine,
    /// Persistent path
    path: PathBuf,
}

impl RocksEngine {
    /// New `RocksEngine`
    ///
    /// # Errors
    ///
    /// Return `EngineError` when encounter fs error or bincode deserialize fails.
    #[inline]
    pub fn new(data_dir: impl AsRef<Path>, tables: &[&'static str]) -> Result<Self, EngineError> {
        let mut path = data_dir.as_ref().to_path_buf();
        path.push(Path::new("persistent"));

        if path.exists() {
            let data = fs::read(path)?;
            let db = bincode::deserialize(&data).map_err(|e| {
                EngineError::UnderlyingError(format!("deserialize memory engine failed: {e:?}"))
            })?;
            Ok(Self {
                inner: MemoryEngine::new_from_db(db),
                path: data_dir.as_ref().to_path_buf(),
            })
        } else {
            fs::create_dir_all(data_dir.as_ref())?;
            Ok(Self {
                inner: MemoryEngine::new(tables),
                path: data_dir.as_ref().to_path_buf(),
            })
        }
    }

    /// Sync the memory engine to file
    ///
    /// # Errors
    ///
    /// Return `EngineError` when get snapshot failed or encounter fs error.
    fn fs_sync(&self) -> Result<(), EngineError> {
        let db = self.inner.get_snapshot("", &[])?.into_inner();
        let mut path = self.path.clone();
        path.push(Path::new("persistent"));
        fs::write(path, db)?;
        Ok(())
    }

    /// Apply snapshot from file
    #[allow(clippy::unused_async)]
    pub async fn apply_snapshot_from_file(
        &self,
        _path: impl AsRef<Path>,
        _tables: &[&'static str],
    ) -> Result<(), EngineError> {
        unreachable!("mock engine does not support apply snapshot from file")
    }
}

#[async_trait::async_trait]
impl StorageEngine for RocksEngine {
    type Snapshot = RocksSnapshot;
    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        self.inner.get(table, key)
    }

    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        self.inner.get_multi(table, keys)
    }

    #[inline]
    fn get_all(&self, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        self.inner.get_all(table)
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError> {
        self.inner.write_batch(wr_ops, sync)?;
        self.fs_sync()
    }

    #[inline]
    fn get_snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError> {
        let path = path.as_ref();
        let snapshot = self.inner.get_snapshot(path, tables)?;
        Ok(RocksSnapshot::new(snapshot, path))
    }

    #[inline]
    async fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        tables: &[&'static str],
    ) -> Result<(), EngineError> {
        self.inner.apply_snapshot(snapshot.inner, tables).await?;
        self.fs_sync()
    }

    #[inline]
    fn estimated_file_size(&self) -> u64 {
        0
    }

    #[inline]
    fn file_size(&self) -> Result<u64, EngineError> {
        Ok(0)
    }
}

/// A mock snapshot of the `RocksEngine`
#[derive(Debug, Default)]
pub struct RocksSnapshot {
    /// data of the snapshot
    inner: MemorySnapshot,
    /// Persistent path
    _path: PathBuf,
}

impl RocksSnapshot {
    /// New empty mock `RocksSnapshot`
    fn new<P>(snapshot: MemorySnapshot, dir: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            inner: snapshot,
            _path: dir.into(),
        }
    }

    /// Create a new mock snapshot for receiving
    /// # Errors
    /// Return `EngineError` when create directory failed.
    #[inline]
    #[allow(clippy::unnecessary_wraps)] // the real rocksdb engine need the Result wrap
    pub fn new_for_receiving<P>(dir: P) -> Result<Self, EngineError>
    where
        P: Into<PathBuf>,
    {
        Ok(Self::new(MemorySnapshot::new(Vec::new()), dir))
    }
}

#[async_trait::async_trait]
impl SnapshotApi for RocksSnapshot {
    #[inline]
    fn size(&self) -> u64 {
        self.inner.size()
    }

    #[inline]
    fn rewind(&mut self) -> std::io::Result<()> {
        self.inner.rewind()
    }

    #[inline]
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<()> {
        self.inner.read_buf(buf).await
    }

    #[inline]
    async fn write_all(&mut self, buf: Bytes) -> std::io::Result<()> {
        self.inner.write_all(buf).await
    }

    #[inline]
    async fn clean(&mut self) -> std::io::Result<()> {
        self.inner.clean().await
    }
}
