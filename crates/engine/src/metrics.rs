use std::{io, path::Path, time::Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use opentelemetry::metrics::Histogram;
use utils::define_metrics;

#[cfg(madsim)]
use crate::mock_rocksdb_engine::RocksEngine;
#[cfg(not(madsim))]
use crate::rocksdb_engine::RocksEngine;
use crate::{EngineError, SnapshotApi, StorageEngine, StorageOps, TransactionApi, WriteOperation};

define_metrics! {
    "engine",
    engine_apply_snapshot_duration_seconds: Histogram<u64> = meter()
        .u64_histogram("engine_apply_snapshot_duration_seconds")
        .with_description("The backend engine apply snapshot duration in seconds.")
        .init(),
    engine_write_batch_duration_seconds: Histogram<u64> = meter()
        .u64_histogram("engine_write_batch_duration_seconds")
        .with_description("The backend engine write batch engine, `batch_size` refer to the size and `sync` if sync option is on.")
        .init()
}

/// Apply metrics to the storage engine
#[derive(Debug, Clone)]
pub struct Layer<E> {
    /// Inner engine
    engine: E,
}

impl<E> Layer<E> {
    /// Create metrics layer
    pub(crate) fn new(engine: E) -> Self {
        Self { engine }
    }
}

impl Layer<RocksEngine> {
    /// Apply snapshot from file, only works for `RocksEngine`
    ///
    /// # Errors
    ///
    /// Return `EngineError` when `RocksDB` returns an error.
    #[inline]
    pub async fn apply_snapshot_from_file(
        &self,
        snap_path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<(), EngineError> {
        self.engine
            .apply_snapshot_from_file(snap_path, tables)
            .await
    }
}

#[async_trait]
impl<E> StorageEngine for Layer<E>
where
    E: StorageEngine,
{
    /// The snapshot type
    type Snapshot = Layer<E::Snapshot>;
    type Transaction<'db> = Layer<E::Transaction<'db>>;

    /// Creates a transaction
    fn transaction(&self) -> Self::Transaction<'_> {
        Layer::new(self.engine.transaction())
    }

    /// Get all the values of the given table
    ///
    /// # Errors
    ///
    /// - Return `EngineError::TableNotFound` if the given table does not exist
    /// - Return `EngineError` if met some errors
    #[allow(clippy::type_complexity)] // it's clear that (Vec<u8>, Vec<u8>) is a key-value pair
    fn get_all(&self, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        self.engine.get_all(table)
    }

    /// Get a snapshot of the current state of the database
    ///
    /// # Errors
    ///
    /// Return `EngineError` if met some errors when creating the snapshot
    fn get_snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError> {
        self.engine.get_snapshot(path, tables).map(Layer::new)
    }

    /// Apply a snapshot to the database
    ///
    /// # Errors
    ///
    /// Return `EngineError` if met some errors when applying the snapshot
    async fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        tables: &[&'static str],
    ) -> Result<(), EngineError> {
        let start = Instant::now();
        let res = self.engine.apply_snapshot(snapshot.engine, tables).await;
        get()
            .engine_apply_snapshot_duration_seconds
            .record(start.elapsed().as_secs(), &[]);
        res
    }

    /// Get the cached size of the engine (Measured in bytes)
    fn estimated_file_size(&self) -> u64 {
        self.engine.estimated_file_size()
    }

    /// Get the file size of the engine (Measured in bytes)
    fn file_size(&self) -> Result<u64, EngineError> {
        self.engine.file_size()
    }
}

impl<E> StorageOps for Layer<E>
where
    E: StorageOps,
{
    fn write(&self, op: WriteOperation<'_>, sync: bool) -> Result<(), EngineError> {
        self.engine.write(op, sync)
    }

    fn write_multi<'a, Ops>(&self, ops: Ops, sync: bool) -> Result<(), EngineError>
    where
        Ops: IntoIterator<Item = WriteOperation<'a>>,
    {
        self.engine.write_multi(ops, sync)
    }

    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        self.engine.get(table, key)
    }

    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        self.engine.get_multi(table, keys)
    }
}

#[async_trait]
impl<E> SnapshotApi for Layer<E>
where
    E: SnapshotApi,
{
    /// Get the size of the snapshot
    fn size(&self) -> u64 {
        self.engine.size()
    }

    /// Rewind the snapshot to the beginning
    fn rewind(&mut self) -> io::Result<()> {
        self.engine.rewind()
    }

    /// Pull some bytes of the snapshot to the given uninitialized buffer
    async fn read_buf(&mut self, buf: &mut BytesMut) -> io::Result<()> {
        self.engine.read_buf(buf).await
    }

    /// Write the given buffer to the snapshot
    async fn write_all(&mut self, buf: Bytes) -> io::Result<()> {
        self.engine.write_all(buf).await
    }

    /// Clean files of current snapshot
    async fn clean(&mut self) -> io::Result<()> {
        self.engine.clean().await
    }
}

#[async_trait]
impl<E> TransactionApi for Layer<E>
where
    E: TransactionApi,
{
    /// Commits the changes
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn commit(self) -> Result<(), EngineError> {
        self.engine.commit()
    }

    /// Rollbacks the changes
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn rollback(&self) -> Result<(), EngineError> {
        self.engine.rollback()
    }
}
