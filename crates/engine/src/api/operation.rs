use crate::EngineError;

/// Storage operations
pub trait StorageOps {
    /// Write an op to the transaction
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn write(&self, op: WriteOperation<'_>, sync: bool) -> Result<(), EngineError>;

    /// Commit a batch of write operations
    /// If sync is true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete. If this
    /// flag is true, writes will be slower.
    ///
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    fn write_multi(&self, ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError>;

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
}

/// Write operation
#[allow(clippy::module_name_repetitions)]
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
