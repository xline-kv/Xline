#![allow(clippy::module_name_repetitions)]
#![allow(clippy::multiple_inherent_impl)]

use std::{cmp::Ordering, collections::HashMap};

use parking_lot::{RwLock, RwLockWriteGuard};

use crate::{
    api::transaction_api::TransactionApi, error::EngineError, memory_engine::MemoryEngine,
    StorageOps, WriteOperation,
};

/// The memory table used in transaction state
type StateMemoryTable = HashMap<Vec<u8>, Option<Vec<u8>>>;

/// A transaction of the `MemoryEngine`
#[derive(Debug, Default)]
pub struct MemoryTransaction {
    /// The memory engine
    pub(super) db: MemoryEngine,
    /// The inner storage engine of `MemoryStorage`
    pub(super) state: RwLock<HashMap<String, StateMemoryTable>>,
}

impl StorageOps for MemoryTransaction {
    fn write(&self, op: WriteOperation<'_>, _sync: bool) -> Result<(), EngineError> {
        let mut state_w = self.state.write();
        self.write_op(&mut state_w, op)
    }

    fn write_multi<'a, Ops>(&self, ops: Ops, _sync: bool) -> Result<(), EngineError>
    where
        Ops: IntoIterator<Item = WriteOperation<'a>>,
    {
        let mut state_w = self.state.write();
        for op in ops {
            self.write_op(&mut state_w, op)?;
        }
        Ok(())
    }

    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        let state_r = self.state.read();
        let state_table = state_r
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

        if let Some(val) = state_table.get(key.as_ref()) {
            return Ok(val.clone());
        }

        let db_inner_r = self.db.inner.read();
        let db_table = db_inner_r
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

        Ok(db_table.get(key.as_ref()).cloned())
    }

    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        keys.iter().map(|key| self.get(table, key)).collect()
    }
}

impl TransactionApi for MemoryTransaction {
    fn commit(self) -> Result<(), EngineError> {
        let mut state_w = self.state.write();
        let mut db_inner_w = self.db.inner.write();
        for (name, mut table) in state_w.drain() {
            let db_table = db_inner_w
                .get_mut(&name)
                .ok_or_else(|| EngineError::TableNotFound(name.clone()))?;

            for (key, val_opt) in table.drain() {
                if let Some(val) = val_opt {
                    let _ignore = db_table.insert(key, val);
                } else {
                    let _ignore = db_table.remove(&key);
                }
            }
        }

        Ok(())
    }

    fn rollback(&self) -> Result<(), EngineError> {
        let mut state_w = self.state.write();
        for table in state_w.values_mut() {
            table.clear();
        }

        Ok(())
    }
}

impl MemoryTransaction {
    /// Write an op to the transaction
    fn write_op(
        &self,
        state_w: &mut RwLockWriteGuard<'_, HashMap<String, StateMemoryTable>>,
        op: WriteOperation<'_>,
    ) -> Result<(), EngineError> {
        match op {
            WriteOperation::Put { table, key, value } => {
                let table = state_w
                    .get_mut(table)
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                let _ignore = table.insert(key, Some(value));
            }
            WriteOperation::Delete { table, key } => {
                let table = state_w
                    .get_mut(table)
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                let _ignore = table.insert(key.to_vec(), None);
            }
            WriteOperation::DeleteRange { table, from, to } => {
                let db_inner_r = self.db.inner.read();
                let db_table = db_inner_r
                    .get(table)
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                let state_table = state_w
                    .get(table)
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

                let to_delete: Vec<_> = db_table
                    .keys()
                    .chain(state_table.keys())
                    .filter(|key| {
                        let key_slice = key.as_slice();
                        match key_slice.cmp(from) {
                            Ordering::Less => false,
                            Ordering::Equal => true,
                            Ordering::Greater => match key_slice.cmp(to) {
                                Ordering::Less => true,
                                Ordering::Equal | Ordering::Greater => false,
                            },
                        }
                    })
                    .cloned()
                    .collect();

                let table = state_w
                    .get_mut(table)
                    .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                // `None` works as a tombstone of the key
                for key in to_delete {
                    let _ignore = table.insert(key.clone(), None);
                }
            }
        }
        Ok(())
    }
}
