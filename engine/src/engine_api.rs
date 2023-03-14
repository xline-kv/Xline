use crate::error::EngineError;

/// Write operation
#[non_exhaustive]
#[derive(Debug)]
pub enum WriteOperation {
    /// `Put` operation
    Put {
        /// The table name
        table: &'static str,
        /// Key
        key: Vec<u8>,
        /// Value
        value: Vec<u8>,
    },
    /// `Delete` operation
    Delete {
        /// The table name
        table: &'static str,
        /// The target key
        key: Vec<u8>,
    },
    /// Delete range operation, it will remove the database entries in the range [from, to)
    DeleteRange {
        /// The table name
        table: &'static str,
        /// The `from` key
        from: Vec<u8>,
        /// The `to` key
        to: Vec<u8>,
    },
}

impl WriteOperation {
    /// Create a new `Put` operation
    #[inline]
    #[must_use]
    pub fn new_put<K, V>(table: &'static str, key: K, value: V) -> Self
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        Self::Put {
            table,
            key: key.into(),
            value: value.into(),
        }
    }

    /// Create a new `Delete` operation
    #[inline]
    #[must_use]
    pub fn new_delete<K>(table: &'static str, key: K) -> Self
    where
        K: Into<Vec<u8>>,
    {
        Self::Delete {
            table,
            key: key.into(),
        }
    }

    /// Create a new `DeleteRange` operation
    #[inline]
    pub fn new_delete_range<K>(table: &'static str, from: K, to: K) -> Self
    where
        K: Into<Vec<u8>>,
    {
        Self::DeleteRange {
            table,
            from: from.into(),
            to: to.into(),
        }
    }
}

/// The `StorageEngine` trait
pub trait StorageEngine: Send + Sync + 'static + std::fmt::Debug {
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
    fn write_batch(&self, wr_ops: Vec<WriteOperation>, sync: bool) -> Result<(), EngineError>;
}
