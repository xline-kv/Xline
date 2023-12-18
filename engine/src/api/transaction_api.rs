use crate::{EngineError, WriteOperation};

/// Api for database transactions
pub trait TransactionApi {
    /// Write an op to the transaction
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn write(&self, op: WriteOperation<'_>) -> Result<(), EngineError>;

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

    /// Commits the changes
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn commit(self) -> Result<(), EngineError>;

    /// Rollbacks the changes
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn rollback(&self) -> Result<(), EngineError>;
}
