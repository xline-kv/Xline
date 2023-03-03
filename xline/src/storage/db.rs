use std::sync::Arc;

use engine::{
    engine_api::StorageEngine, memory_engine::MemoryEngine, rocksdb_engine::RocksEngine, Delete,
    Put, WriteOperation,
};
use utils::config::StorageConfig;

use super::{storage_api::StorageApi, ExecuteError};

/// Xline Server Storage Table
const XLINETABLES: [&str; 3] = ["kv", "lease", "auth"];

/// Database to store revision to kv mapping
#[derive(Debug, Clone)]
pub struct DB<S: StorageEngine> {
    /// internal storage of `DB`
    engine: Arc<S>,
}

impl<S> DB<S>
where
    S: StorageEngine,
{
    /// New `DB`
    #[inline]
    #[must_use]
    pub fn new(engine: S) -> Self {
        Self {
            engine: Arc::new(engine),
        }
    }
}

impl<S> StorageApi for DB<S>
where
    S: StorageEngine,
{
    fn insert<K, V>(&self, table: &str, key: K, value: V) -> Result<(), ExecuteError>
    where
        K: Into<Vec<u8>> + std::fmt::Debug + Sized,
        V: Into<Vec<u8>> + std::fmt::Debug + Sized,
    {
        let put_op = WriteOperation::Put(Put::new(table, key.into(), value.into(), false));
        self.engine.write_batch(vec![put_op]).map_err(|e| {
            ExecuteError::DbError(format!("Failed to insert key-value, error: {e}"))
        })?;
        Ok(())
    }

    fn get_values<K>(&self, table: &str, keys: &[K]) -> Result<Vec<Vec<u8>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug + Sized,
    {
        let values = self
            .engine
            .get_multi(table, keys)
            .map_err(|e| ExecuteError::DbError(format!("Failed to get keys {keys:?}: {e}")))?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(values.len(), keys.len(), "Index doesn't match with DB");

        Ok(values)
    }

    /// Delete key from storage
    fn delete<K>(&self, table: &str, key: K) -> Result<(), ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug + Sized,
    {
        let del_op = WriteOperation::Delete(Delete::new(table, key.as_ref(), false));
        self.engine
            .write_batch(vec![del_op])
            .map_err(|e| ExecuteError::DbError(format!("Failed to delete Lease, error: {e}")))?;
        Ok(())
    }
}

/// `DBProxy` is designed to mask the different type of `DB<MemoryEngine>` and `DB<RocksEngine>`
/// and provides an uniform type to the upper layer.
///
/// Why don't we use dyn trait object to erase the type difference?
/// There are two reasons behind doing so:
/// 1. The `StorageApi` trait has some method with generic parameters, like insert<K, V>.
/// This breaks the object safety rules. If we remove these generic parameters, we will
/// lose some flexibility when calling these methods.
/// 2. A dyn object should not be bounded by Sized trait, and some async functions, like
/// `XlineServer::new`, requires its parameter to satisfy the Sized trait when we await
/// it. So here is a contradictory.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum DBProxy {
    /// DB which base on the Memory Engine
    MemDB(DB<MemoryEngine>),
    /// DB which base on the Rocks Engine
    RocksDB(DB<RocksEngine>),
}

impl StorageApi for DBProxy {
    fn get_values<K>(&self, table: &str, keys: &[K]) -> Result<Vec<Vec<u8>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug + Sized,
    {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.get_values(table, keys),
            DBProxy::RocksDB(ref inner_db) => inner_db.get_values(table, keys),
        }
    }

    fn insert<K, V>(&self, table: &str, key: K, value: V) -> Result<(), ExecuteError>
    where
        K: Into<Vec<u8>> + std::fmt::Debug,
        V: Into<Vec<u8>> + std::fmt::Debug,
    {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.insert(table, key, value),
            DBProxy::RocksDB(ref inner_db) => inner_db.insert(table, key, value),
        }
    }

    fn delete<K>(&self, table: &str, key: K) -> Result<(), ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug,
    {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.delete(table, key),
            DBProxy::RocksDB(ref inner_db) => inner_db.delete(table, key),
        }
    }
}

impl DBProxy {
    /// Create a new `DBProxy`
    ///
    /// # Errors
    ///
    /// Return `ExecuteError::DbError` when open db failed
    #[inline]
    pub fn new(config: &StorageConfig) -> Result<Arc<DBProxy>, ExecuteError> {
        match *config {
            StorageConfig::Memory => {
                let engine = MemoryEngine::new(&XLINETABLES)
                    .map_err(|e| ExecuteError::DbError(format!("Cannot open database: {e}")))?;
                Ok(Arc::new(DBProxy::MemDB(DB::new(engine))))
            }
            StorageConfig::RocksDB(ref path) => {
                let engine = RocksEngine::new(path, &XLINETABLES)
                    .map_err(|e| ExecuteError::DbError(format!("Cannot open database: {e}")))?;
                Ok(Arc::new(DBProxy::RocksDB(DB::new(engine))))
            }
            _ => unreachable!(),
        }
    }
}
