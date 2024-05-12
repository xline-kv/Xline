#![allow(clippy::module_name_repetitions)]
#![allow(clippy::multiple_inherent_impl)]

use crate::TransactionApi;

/// A transaction of the `RocksEngine`
#[derive(Copy, Clone, Debug, Default)]
pub struct RocksTransaction;

impl TransactionApi for RocksTransaction {
    fn commit(self) -> Result<(), crate::EngineError> {
        Ok(())
    }

    fn rollback(&self) -> Result<(), crate::EngineError> {
        Ok(())
    }
}
