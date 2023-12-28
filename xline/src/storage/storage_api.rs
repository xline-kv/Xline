use std::path::Path;

use engine::{Snapshot, Transaction};
use xlineapi::execute_error::ExecuteError;

use super::{db::WriteOp, revision::KeyRevision};

/// The Stable Storage Api
#[async_trait::async_trait]
pub trait StorageApi: Send + Sync + 'static + std::fmt::Debug {
    /// Creates a transaction
    fn transaction(&self) -> Transaction<'_>;

    /// Get values by keys from storage
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn get_values<K>(
        &self,
        table: &'static str,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug;

    /// Get values by keys from storage
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn get_value<K>(&self, table: &'static str, key: K) -> Result<Option<Vec<u8>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug;

    /// Get all values of the given table from storage
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    #[allow(clippy::type_complexity)] // it's clear that (Vec<u8>, Vec<u8>) is a key-value pair
    fn get_all(&self, table: &'static str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecuteError>;

    /// Reset the storage by given snapshot
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    async fn reset(&self, snapshot: Option<Snapshot>) -> Result<(), ExecuteError>;

    /// Get the snapshot of the storage
    fn get_snapshot(&self, snap_path: impl AsRef<Path>) -> Result<Snapshot, ExecuteError>;

    /// Flush the operations to storage
    fn flush_ops(&self, ops: Vec<WriteOp>) -> Result<Vec<(Vec<u8>, KeyRevision)>, ExecuteError>;

    /// Get the cached size of the engine
    fn size(&self) -> u64;

    /// Get the file size of the engine
    fn file_size(&self) -> Result<u64, ExecuteError>;
}

/// The Stable Storage Transaction Api
pub(crate) trait StorageTxnApi {
    /// Get values by keys from storage
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn get_values<K>(
        &self,
        table: &'static str,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug;

    /// Get values by keys from storage
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn get_value<K>(&self, table: &'static str, key: K) -> Result<Option<Vec<u8>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug;

    /// Write an operation to the transaction
    fn write_op(&self, op: WriteOp) -> Result<(), ExecuteError>;

    /// Write a batch of operations to the transaction
    fn write_ops(&self, ops: Vec<WriteOp>) -> Result<(), ExecuteError>;
}
