use crate::EngineError;

/// Api for database transactions
pub trait TransactionApi {
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
