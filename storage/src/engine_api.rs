/// Write operation
#[allow(dead_code)]
pub(super) enum WriteOperation<'a> {
    /// `Put` operation
    Put(Put),
    /// `Delete` operation
    Delete(Delete<'a>),
    /// `DeleteRange` operation
    DeleteRange(DeleteRange<'a>),
}

/// Put operation
#[allow(dead_code)]
pub(crate) struct Put {
    /// The table name
    pub(crate) table: String,
    /// Key
    pub(crate) key: Vec<u8>,
    /// Value
    pub(crate) value: Vec<u8>,
    /// If true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete. If this
    /// flag is true, writes will be slower.
    pub(crate) sync: bool,
}

impl Put {
    /// Create a new `Put` operation
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn new(table: &str, key: Vec<u8>, value: Vec<u8>, sync: bool) -> Put {
        Put {
            table: table.to_owned(),
            key,
            value,
            sync,
        }
    }
}

/// Delete operation,
#[allow(dead_code)]
pub(crate) struct Delete<'a> {
    /// The table name
    pub(crate) table: String,
    /// The target key
    pub(crate) key: &'a [u8],
    /// See `Put::sync` for more details
    pub(crate) sync: bool,
}

impl<'a> Delete<'a> {
    /// Create a new `Delete` operation
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn new(table: &str, key: &'a [u8], sync: bool) -> Delete<'a> {
        Delete {
            table: table.to_owned(),
            key,
            sync,
        }
    }
}

/// Delete range operation, it will remove the database
/// entries in the range [from, to)
#[allow(dead_code)]
pub(super) struct DeleteRange<'a> {
    /// The table name
    pub(crate) table: String,
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
    pub(crate) fn new(table: &str, from: &'a [u8], to: &'a [u8], sync: bool) -> DeleteRange<'a> {
        DeleteRange {
            table: table.to_owned(),
            from,
            to,
            sync,
        }
    }
}

/// The `StorageEngine` trait
pub(super) trait StorageEngine: Send + Sync + 'static {
    /// The associated error type
    type Error: std::error::Error;
    /// The associated key type
    type Key: AsRef<[u8]>;
    /// The associated value type
    type Value: AsRef<[u8]>;

    /// Create a logical table with the given name
    fn create_table(&self, table: &str) -> Result<(), Self::Error>;

    /// Get the value associated with a key value and the given table
    fn get(&self, table: &str, key: &Self::Key) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Get the values associated with the given keys
    fn get_multi(
        &self,
        table: &str,
        keys: &[Self::Key],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    /// Commit a batch of write operations
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>) -> Result<(), Self::Error>;
}
