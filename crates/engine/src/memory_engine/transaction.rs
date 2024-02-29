#![allow(clippy::module_name_repetitions)]
#![allow(clippy::multiple_inherent_impl)]

use crate::TransactionApi;

/// A transaction of the `MemoryEngine`
#[derive(Copy, Clone, Debug, Default)]
pub struct MemoryTransaction;

impl TransactionApi for MemoryTransaction {
    fn commit(self) -> Result<(), crate::EngineError> {
        Ok(())
    }

    fn rollback(&self) -> Result<(), crate::EngineError> {
        Ok(())
    }
}
