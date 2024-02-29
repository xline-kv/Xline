#![allow(clippy::module_name_repetitions)]
#![allow(clippy::multiple_inherent_impl)]

use std::{
    iter::repeat,
    sync::{atomic::AtomicU64, Arc},
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use parking_lot::Mutex;
use rocksdb::{
    Direction, IteratorMode, OptimisticTransactionDB, Transaction, WriteBatchWithTransaction,
    WriteOptions,
};

use crate::{
    api::transaction_api::TransactionApi, error::EngineError, rocksdb_engine::RocksEngine,
    StorageOps, WriteOperation,
};

/// Transaction type for `RocksDB`
#[derive(Debug)]
pub struct RocksTransaction {
    /// Inner state
    inner: Mutex<Option<Inner>>,
}

/// Inner state of the transaction
///
/// WARN: `db` should never be dropped before `txn`
struct Inner {
    /// The inner DB
    db: Arc<OptimisticTransactionDB>,
    /// A transaction of the DB
    txn: Option<Transaction<'static, OptimisticTransactionDB>>,
    /// Cached write operaions
    write_ops: Vec<WriteOperationOwned>,
    /// The size of the engine
    engine_size: Arc<AtomicU64>,
    /// The size of the txn
    txn_size: usize,
}

/// Write operation
/// This is an owned type of `WriteOperation`
#[non_exhaustive]
#[derive(Debug)]
enum WriteOperationOwned {
    /// `Put` operation
    Put {
        /// The table name
        table: String,
        /// Key
        key: Vec<u8>,
        /// Value
        value: Vec<u8>,
    },
    /// `Delete` operation
    Delete {
        /// The table name
        table: String,
        /// The target key
        key: Vec<u8>,
    },
    /// Delete range operation, it will remove the database entries in the range [from, to)
    DeleteRange {
        /// The table name
        table: String,
        /// The `from` key
        from: Vec<u8>,
        /// The `to` key
        to: Vec<u8>,
    },
}

impl RocksTransaction {
    /// Creates a new `RocksTransaction`
    pub(super) fn new(db: Arc<OptimisticTransactionDB>, engine_size: Arc<AtomicU64>) -> Self {
        let inner = Inner {
            db,
            txn: None,
            write_ops: vec![],
            engine_size,
            txn_size: 0,
        };
        Self {
            inner: Mutex::new(Some(inner)),
        }
    }
}

#[allow(clippy::unwrap_used)]
#[allow(clippy::unwrap_in_result)]
impl StorageOps for RocksTransaction {
    fn write(&self, op: WriteOperation<'_>, sync: bool) -> Result<(), EngineError> {
        self.inner.lock().as_mut().unwrap().write(op, sync)
    }

    fn write_multi(&self, ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError> {
        self.inner.lock().as_mut().unwrap().write_multi(ops, sync)
    }

    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        self.inner.lock().as_mut().unwrap().get(table, key)
    }

    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        self.inner.lock().as_mut().unwrap().get_multi(table, keys)
    }
}

#[allow(clippy::unwrap_used)]
#[allow(clippy::unwrap_in_result)]
impl TransactionApi for RocksTransaction {
    fn commit(self) -> Result<(), EngineError> {
        self.inner.lock().take().unwrap().commit()
    }

    fn rollback(&self) -> Result<(), EngineError> {
        self.inner.lock().as_mut().unwrap().rollback()
    }
}

impl Inner {
    /// Replace txn with a new transaction
    #[allow(unsafe_code)]
    #[allow(clippy::unwrap_used)]
    fn enable_transaction(&mut self) -> Result<(), EngineError> {
        if self.txn.is_some() {
            return Ok(());
        }

        let txn = self.db.transaction();
        let txn_static =
            // SAFETY: In `RocksTransaction` we hold an Arc reference to the DB, 
            // so a `Transaction<'db, DB>` won't outlive the lifetime of the DB.
            unsafe { std::mem::transmute::<_, Transaction<'static, OptimisticTransactionDB>>(txn) };

        for op in self.write_ops.drain(..).collect::<Vec<_>>() {
            self.txn_write_op(op, &txn_static)?;
        }

        self.txn = Some(txn_static);

        Ok(())
    }

    #[allow(clippy::pattern_type_mismatch)]
    /// Batch write operation
    fn batch_write_op(
        &self,
        op: WriteOperationOwned,
        batch: &mut WriteBatchWithTransaction<true>,
    ) -> Result<(), EngineError> {
        match op {
            WriteOperationOwned::Put { table, key, value } => {
                let cf = self
                    .db
                    .cf_handle(&table)
                    .ok_or(EngineError::TableNotFound(table.clone()))?;
                batch.put_cf(&cf, key, value);
            }
            WriteOperationOwned::Delete { table, key } => {
                let cf = self
                    .db
                    .cf_handle(&table)
                    .ok_or(EngineError::TableNotFound(table.clone()))?;
                batch.delete_cf(&cf, key);
            }
            WriteOperationOwned::DeleteRange { table, from, to } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.clone()))?;
                let mode = IteratorMode::From(&from, Direction::Forward);
                let kvs: Vec<_> = self
                    .db
                    .iterator_cf(&cf, mode)
                    .take_while(|res| {
                        res.as_ref()
                            .is_ok_and(|(key, _)| key.as_ref() < to.as_slice())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                for (key, _) in kvs {
                    batch.delete_cf(&cf, key);
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::pattern_type_mismatch)]
    /// Applies write ops to txn
    fn txn_write_op(
        &self,
        op: WriteOperationOwned,
        txn: &Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<(), EngineError> {
        match op {
            WriteOperationOwned::Put { table, key, value } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.clone()))?;
                txn.put_cf(&cf, key, value).map_err(EngineError::from)?;
            }
            WriteOperationOwned::Delete { table, key } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.clone()))?;
                txn.delete_cf(&cf, key).map_err(EngineError::from)?;
            }
            WriteOperationOwned::DeleteRange { table, from, to } => {
                let cf = self
                    .db
                    .cf_handle(table.as_ref())
                    .ok_or_else(|| EngineError::TableNotFound(table.clone()))?;
                let mode = IteratorMode::From(&from, Direction::Forward);
                let kvs: Vec<_> = txn
                    .iterator_cf(&cf, mode)
                    .take_while(|res| {
                        res.as_ref()
                            .is_ok_and(|(key, _)| key.as_ref() < to.as_slice())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                for (key, _) in kvs {
                    txn.delete_cf(&cf, key)?;
                }
            }
        }

        Ok(())
    }
}

#[allow(clippy::unwrap_used)]
#[allow(clippy::unwrap_in_result)]
impl Inner {
    /// Write an op to the transaction
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn write(&mut self, op: WriteOperation<'_>, _sync: bool) -> Result<(), EngineError> {
        if let Some(ref txn) = self.txn {
            return self.txn_write_op(op.into(), txn);
        }
        #[allow(clippy::pattern_type_mismatch)] // can't be fixed
        match op {
            WriteOperation::Put {
                table,
                ref key,
                ref value,
            } => {
                self.txn_size = self.txn_size.overflow_add(RocksEngine::max_write_size(
                    table.len(),
                    key.len(),
                    value.len(),
                ));
            }
            WriteOperation::Delete { .. } | WriteOperation::DeleteRange { .. } => {}
        };

        self.write_ops.push(op.into());

        Ok(())
    }

    /// Commit a batch of write operations
    /// If sync is true, the write will be flushed from the operating system
    /// buffer cache before the write is considered complete. If this
    /// flag is true, writes will be slower.
    ///
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    fn write_multi(&mut self, ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError> {
        for op in ops {
            self.write(op, sync)?;
        }
        Ok(())
    }

    /// Get the value associated with a key value and the given table
    ///
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    fn get(&mut self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        self.enable_transaction()?;
        let cf = self
            .db
            .cf_handle(table.as_ref())
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        let txn = self.txn.as_ref().unwrap();
        txn.get_cf(&cf, key).map_err(EngineError::from)
    }

    /// Get the values associated with the given keys
    ///
    /// # Errors
    /// Return `EngineError::TableNotFound` if the given table does not exist
    /// Return `EngineError` if met some errors
    fn get_multi(
        &mut self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        self.enable_transaction()?;
        let txn = self.txn.as_ref().unwrap();
        let cf = self
            .db
            .cf_handle(table.as_ref())
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        txn.multi_get_cf(repeat(&cf).zip(keys.iter()))
            .into_iter()
            .collect::<Result<_, _>>()
            .map_err(EngineError::from)
    }

    /// Commits the changes
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn commit(mut self) -> Result<(), EngineError> {
        let _ignore = self.engine_size.fetch_add(
            self.txn_size.numeric_cast(),
            std::sync::atomic::Ordering::Relaxed,
        );

        if let Some(txn) = self.txn {
            return txn.commit().map_err(Into::into);
        }

        let mut batch = WriteBatchWithTransaction::<true>::default();
        for op in self.write_ops.drain(..).collect::<Vec<_>>() {
            self.batch_write_op(op, &mut batch)?;
        }
        self.db.write_opt(batch, &WriteOptions::default())?;

        Ok(())
    }

    /// Rollbacks the changes
    ///
    /// # Errors
    ///
    /// if error occurs in storage, return `Err(error)`
    fn rollback(&mut self) -> Result<(), EngineError> {
        if let Some(ref txn) = self.txn {
            txn.rollback()?;
            self.txn = None;
        } else {
            self.write_ops.clear();
        }

        Ok(())
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksTransaction")
            .field("db", &self.db)
            .finish()
    }
}

impl From<WriteOperation<'_>> for WriteOperationOwned {
    fn from(op: WriteOperation<'_>) -> Self {
        match op {
            WriteOperation::Put { table, key, value } => Self::Put {
                table: table.to_owned(),
                key,
                value,
            },
            WriteOperation::Delete { table, key } => Self::Delete {
                table: table.to_owned(),
                key: key.to_owned(),
            },
            WriteOperation::DeleteRange { table, from, to } => Self::DeleteRange {
                table: table.to_owned(),
                from: from.to_owned(),
                to: to.to_owned(),
            },
        }
    }
}
