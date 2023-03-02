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
#[allow(dead_code)]
#[derive(Debug)]
pub struct Put<'a> {
    /// The table name
    pub(crate) table: &'a str,
    /// Key
    pub(crate) key: Vec<u8>,
    /// Value
    pub(crate) value: Vec<u8>,
    /// If true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete. If this
    /// flag is true, writes will be slower.
    pub(crate) sync: bool,
}

impl<'a> Put<'a> {
    /// Create a new `Put` operation
    #[inline]
    #[must_use]
    pub fn new(table: &'a str, key: Vec<u8>, value: Vec<u8>, sync: bool) -> Put<'a> {
        Put {
            table,
            key,
            value,
            sync,
        }
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
    /// See `Put::sync` for more details
    pub(crate) sync: bool,
}

impl<'a> Delete<'a> {
    /// Create a new `Delete` operation
    #[inline]
    #[must_use]
    pub fn new(table: &'a str, key: &'a [u8], sync: bool) -> Delete<'a> {
        Delete { table, key, sync }
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
    /// See `Put::sync` for more details
    pub(crate) sync: bool,
}

impl<'a> DeleteRange<'a> {
    /// Create a new `DeleteRange` operation
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn new(table: &'a str, from: &'a [u8], to: &'a [u8], sync: bool) -> DeleteRange<'a> {
        DeleteRange {
            table,
            from,
            to,
            sync,
        }
    }
}

/// The `StorageEngine` trait
pub trait StorageEngine: Send + Sync + 'static + std::fmt::Debug {
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
    ///
    /// # Errors
    /// Return `TableNotFound` if the given table does not exist
    /// Return `IoError` if met some io errors
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>) -> Result<(), EngineError>;
}
