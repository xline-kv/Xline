use std::path::Path;

use crate::{api::snapshot_api::SnapshotApi, error::EngineError};

/// Write operation
#[non_exhaustive]
#[derive(Debug)]
pub enum WriteOperation<'a> {
    /// `Put` operation
    Put {
        /// The table name
        table: &'a str,
        /// Key
        key: Vec<u8>,
        /// Value
        value: Vec<u8>,
    },
    /// `Delete` operation
    Delete {
        /// The table name
        table: &'a str,
        /// The target key
        key: &'a [u8],
    },
    /// Delete range operation, it will remove the database entries in the range [from, to)
    DeleteRange {
        /// The table name
        table: &'a str,
        /// The `from` key
        from: &'a [u8],
        /// The `to` key
        to: &'a [u8],
    },
}

impl<'a> WriteOperation<'a> {
    /// Create a new `Put` operation
    #[inline]
    #[must_use]
    pub fn new_put(table: &'a str, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::Put { table, key, value }
    }

    /// Create a new `Delete` operation
    #[inline]
    #[must_use]
    pub fn new_delete(table: &'a str, key: &'a [u8]) -> Self {
        Self::Delete { table, key }
    }

    /// Create a new `DeleteRange` operation
    #[inline]
    #[must_use]
    pub fn new_delete_range(table: &'a str, from: &'a [u8], to: &'a [u8]) -> Self {
        Self::DeleteRange { table, from, to }
    }
}

/// The `StorageEngine` trait
#[async_trait::async_trait]
pub trait StorageEngine: Send + Sync + 'static + std::fmt::Debug {
    /// The snapshot type
    type Snapshot: SnapshotApi;

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
    fn file_size(&self) -> Result<u64, EngineError>;
}
