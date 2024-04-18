use std::path::Path;

use crate::{api::snapshot_api::SnapshotApi, error::EngineError, TransactionApi, WriteOperation};

/// The `StorageEngine` trait
#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync + 'static + std::fmt::Debug {
    /// The snapshot type
    type Snapshot: SnapshotApi;
    /// The transaction type
    type Transaction: TransactionApi;

    /// Creates a transaction
    fn transaction(&self) -> Self::Transaction;

    /// Get the value associated with a key value and the given table
    ///
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError>;

    /// Get the values associated with the given keys
    ///
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError>;

    /// Get all the values of the given table
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    #[allow(clippy::type_complexity)] // it's clear that (Vec<u8>, Vec<u8>) is a key-value pair
    fn get_all(&self, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError>;

    /// Commit a batch of write operations
    /// If sync is true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete. If this
    /// flag is true, writes will be slower.
    ///
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError>;

    /// Get a snapshot of the current state of the database
    ///
    /// # Errors
    /// Return `EngineError` if met some errors when creating the snapshot
    fn get_snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError>;

    /// Apply a snapshot to the database
    ///
    /// # Errors
    /// Return `EngineError` if met some errors when applying the snapshot
    async fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        tables: &[&'static str],
    ) -> Result<(), EngineError>;

    /// Get the cached size of the engine (Measured in bytes)
    fn estimated_file_size(&self) -> u64;

    /// Get the file size of the engine (Measured in bytes)
    ///
    /// # Errors
    /// Return `EngineError` if met some errors when get file size
    fn file_size(&self) -> Result<u64, EngineError>;
}
