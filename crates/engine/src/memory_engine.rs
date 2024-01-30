use std::{
    cmp::Ordering,
    collections::HashMap,
    io::{Cursor, Seek},
    path::Path,
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use clippy_utilities::NumericCast;
use parking_lot::RwLock;
use tokio::io::AsyncWriteExt;
use tokio_util::io::read_buf;

use crate::{
    api::{
        engine_api::{StorageEngine, WriteOperation},
        snapshot_api::SnapshotApi,
    },
    error::EngineError,
};

/// A helper type to store the key-value pairs for the `MemoryEngine`
type MemoryTable = HashMap<Vec<u8>, Vec<u8>>;

/// Memory Storage Engine Implementation
#[derive(Debug, Default)]
pub struct MemoryEngine {
    /// The inner storage engine of `MemoryStorage`
    inner: Arc<RwLock<HashMap<String, MemoryTable>>>,
}

impl MemoryEngine {
    /// New `MemoryEngine`
    #[inline]
    pub(crate) fn new(tables: &[&'static str]) -> Self {
        let mut inner: HashMap<String, HashMap<Vec<u8>, Vec<u8>>> = HashMap::new();
        for table in tables {
            let _ignore = inner.entry((*table).to_owned()).or_insert(HashMap::new());
        }
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// New `MemoryEngine`
    #[cfg(madsim)]
    #[inline]
    pub(crate) fn new_from_db(db: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(db)),
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for MemoryEngine {
    type Snapshot = MemorySnapshot;

    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        Ok(table.get(&key.as_ref().to_vec()).cloned())
    }

    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

        Ok(keys
            .iter()
            .map(|key| table.get(&key.as_ref().to_vec()).cloned())
            .collect())
    }

    #[inline]
    fn get_all(&self, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        let mut values = table
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        values.sort_by(|v1, v2| v1.0.cmp(&v2.0));
        Ok(values)
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, _sync: bool) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        for op in wr_ops {
            match op {
                WriteOperation::Put { table, key, value } => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.insert(key, value);
                }
                WriteOperation::Delete { table, key } => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.remove(key);
                }
                WriteOperation::DeleteRange { table, from, to } => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    table.retain(|key, _value| {
                        let key_slice = key.as_slice();
                        match key_slice.cmp(from) {
                            Ordering::Less => true,
                            Ordering::Equal => false,
                            Ordering::Greater => match key_slice.cmp(to) {
                                Ordering::Less => false,
                                Ordering::Equal | Ordering::Greater => true,
                            },
                        }
                    });
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn get_snapshot(
        &self,
        _path: impl AsRef<Path>,
        _tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError> {
        let inner_r = self.inner.read();
        let db = &*inner_r;
        let data = bincode::serialize(db).map_err(|e| {
            EngineError::UnderlyingError(format!("serialize memory engine failed: {e:?}"))
        })?;
        Ok(MemorySnapshot::new(data))
    }

    #[inline]
    async fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        _tables: &[&'static str],
    ) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        let db = &mut *inner;
        let data = snapshot.into_inner();
        let new_db = bincode::deserialize(&data).map_err(|e| {
            EngineError::UnderlyingError(format!("deserialize memory engine failed: {e:?}"))
        })?;
        *db = new_db;
        Ok(())
    }

    fn estimated_file_size(&self) -> u64 {
        0
    }

    fn file_size(&self) -> Result<u64, EngineError> {
        Ok(0)
    }
}

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
    fn rewind(&mut self) -> std::io::Result<()> {
        Seek::rewind(&mut self.data)
    }

    #[inline]
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<()> {
        read_buf(&mut self.data, buf).await.map(|_n| ())
    }

    #[inline]
    async fn write_all(&mut self, buf: Bytes) -> std::io::Result<()> {
        self.data.write_all(&buf).await
    }

    #[inline]
    async fn clean(&mut self) -> std::io::Result<()> {
        self.data.get_mut().clear();
        Ok(())
    }
}
