use std::{
    io::{Read, Write},
    path::Path,
};

use crate::error::EngineError;

/// Write operation
#[non_exhaustive]
#[derive(Debug)]
pub enum WriteOperation<'a> {
    /// `Put` operation
    Put(Put<'a>),
    /// `Delete` operation
    Delete(Delete<'a>),
    /// `DeleteRange` operation
    DeleteRange(DeleteRange<'a>),
}

/// Put operation
#[derive(Debug)]
pub struct Put<'a> {
    /// The table name
    pub(crate) table: &'a str,
    /// Key
    pub(crate) key: Vec<u8>,
    /// Value
    pub(crate) value: Vec<u8>,
}

impl<'a> Put<'a> {
    /// Create a new `Put` operation
    #[inline]
    #[must_use]
    pub fn new(table: &'a str, key: Vec<u8>, value: Vec<u8>) -> Put<'a> {
        Put { table, key, value }
    }
}

/// Delete operation,
#[allow(dead_code)]
#[derive(Debug)]
pub struct Delete<'a> {
    /// The table name
    pub(crate) table: &'a str,
    /// The target key
    pub(crate) key: &'a [u8],
}

impl<'a> Delete<'a> {
    /// Create a new `Delete` operation
    #[inline]
    #[must_use]
    pub fn new(table: &'a str, key: &'a [u8]) -> Delete<'a> {
        Delete { table, key }
    }
}

/// Delete range operation, it will remove the database
/// entries in the range [from, to)
#[allow(dead_code)]
#[derive(Debug)]
pub struct DeleteRange<'a> {
    /// The table name
    pub(crate) table: &'a str,
    /// The `from` key
    pub(crate) from: &'a [u8],
    /// The `to` key
    pub(crate) to: &'a [u8],
}

impl<'a> DeleteRange<'a> {
    /// Create a new `DeleteRange` operation
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn new(table: &'a str, from: &'a [u8], to: &'a [u8]) -> DeleteRange<'a> {
        DeleteRange { table, from, to }
    }
}

/// This trait is a abstraction of the snapshot, We can Read/Write the snapshot like a file.
pub trait SnapshotApi: Read + Write {
    /// Get the size of the snapshot
    fn size(&self) -> u64;
}

/// The `StorageEngine` trait
pub trait StorageEngine: Send + Sync + 'static + std::fmt::Debug {
    /// Snapshot type
    type Snapshot: SnapshotApi;

    /// Get the value associated with a key value and the given table
    ///
    /// # Errors
    /// Return `TableNotFound` if the given table does not exist
    /// Return `IoError` if met some io errors
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError>;

    /// Get the values associated with the given keys
    ///
    /// # Errors
    /// Return `TableNotFound` if the given table does not exist
    /// Return `IoError` if met some io errors
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError>;

    /// Commit a batch of write operations
    /// If sync is true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete. If this
    /// flag is true, writes will be slower.
    ///
    /// # Errors
    /// Return `TableNotFound` if the given table does not exist
    /// Return `IoError` if met some io errors
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError>;

    /// Get a snapshot of the current state of the database
    ///
    /// # Errors
    /// Return `UnderlyingError` if met some errors when creating the snapshot
    fn snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError>;

    /// Apply a snapshot to the database
    ///
    /// # Errors
    /// Return `UnderlyingError` if met some errors when applying the snapshot
    fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        tables: &[&'static str],
    ) -> Result<(), EngineError>;
}
