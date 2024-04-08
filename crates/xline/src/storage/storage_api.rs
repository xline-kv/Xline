use xlineapi::execute_error::ExecuteError;

use super::db::WriteOp;

/// Storage operations in xline
pub(crate) trait XlineStorageOps {
    /// Write an operation to the transaction
    fn write_op(&self, op: WriteOp) -> Result<(), ExecuteError>;

    /// Write a batch of operations to the transaction
    fn write_ops(&self, ops: Vec<WriteOp>) -> Result<(), ExecuteError>;

    /// Get values by keys from storage
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn get_value<K>(&self, table: &'static str, key: K) -> Result<Option<Vec<u8>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug;

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
}
