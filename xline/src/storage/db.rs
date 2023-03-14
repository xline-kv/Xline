use std::sync::Arc;

use engine::{
    engine_api::StorageEngine, memory_engine::MemoryEngine, rocksdb_engine::RocksEngine,
    WriteOperation,
};
use utils::config::StorageConfig;

use crate::server::command::META_TABLE;

use super::{
    auth_store::{AUTH_TABLE, ROLE_TABLE, USER_TABLE},
    kv_store::KV_TABLE,
    lease_store::LEASE_TABLE,
    storage_api::StorageApi,
    ExecuteError,
};

/// Xline Server Storage Table
const XLINE_TABLES: [&str; 6] = [
    META_TABLE,
    KV_TABLE,
    LEASE_TABLE,
    AUTH_TABLE,
    USER_TABLE,
    ROLE_TABLE,
];

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
    fn get_values<K>(
        &self,
        table: &'static str,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug + Sized,
    {
        let values = self
            .engine
            .get_multi(table, keys)
            .map_err(|e| ExecuteError::DbError(format!("Failed to get keys {keys:?}: {e}")))?
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(values.len(), keys.len(), "Index doesn't match with DB");

        Ok(values)
    }

    fn get_value<K>(&self, table: &'static str, key: K) -> Result<Option<Vec<u8>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug,
    {
        self.engine
            .get(table, key.as_ref())
            .map_err(|e| ExecuteError::DbError(format!("Failed to get key {key:?}: {e}")))
    }

    fn get_all(&self, table: &'static str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecuteError> {
        self.engine.get_all(table).map_err(|e| {
            ExecuteError::DbError(format!("Failed to get all keys from {table:?}: {e}"))
        })
    }

    fn write_batch(&self, wr_ops: Vec<WriteOperation>, sync: bool) -> Result<(), ExecuteError> {
        self.engine
            .write_batch(wr_ops, sync)
            .map_err(|e| ExecuteError::DbError(format!("Failed to write batch, error: {e}")))
    }

    fn reset(&self) -> Result<(), ExecuteError> {
        let start = vec![];
        let end = vec![0xff];
        let ops = XLINE_TABLES
            .iter()
            .map(|table| WriteOperation::new_delete_range(table, start.as_slice(), end.as_slice()))
            .collect();
        self.write_batch(ops, true)
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
    fn get_values<K>(
        &self,
        table: &'static str,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug + Sized,
    {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.get_values(table, keys),
            DBProxy::RocksDB(ref inner_db) => inner_db.get_values(table, keys),
        }
    }

    fn get_value<K>(&self, table: &'static str, key: K) -> Result<Option<Vec<u8>>, ExecuteError>
    where
        K: AsRef<[u8]> + std::fmt::Debug,
    {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.get_value(table, key),
            DBProxy::RocksDB(ref inner_db) => inner_db.get_value(table, key),
        }
    }

    fn get_all(&self, table: &'static str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecuteError> {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.get_all(table),
            DBProxy::RocksDB(ref inner_db) => inner_db.get_all(table),
        }
    }

    fn write_batch(&self, wr_ops: Vec<WriteOperation>, sync: bool) -> Result<(), ExecuteError> {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.write_batch(wr_ops, sync),
            DBProxy::RocksDB(ref inner_db) => inner_db.write_batch(wr_ops, sync),
        }
    }

    fn reset(&self) -> Result<(), ExecuteError> {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.reset(),
            DBProxy::RocksDB(ref inner_db) => inner_db.reset(),
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
    pub fn open(config: &StorageConfig) -> Result<Arc<DBProxy>, ExecuteError> {
        match *config {
            StorageConfig::Memory => {
                let engine = MemoryEngine::new(&XLINE_TABLES)
                    .map_err(|e| ExecuteError::DbError(format!("Cannot open database: {e}")))?;
                Ok(Arc::new(DBProxy::MemDB(DB::new(engine))))
            }
            StorageConfig::RocksDB(ref path) => {
                let engine = RocksEngine::new(path, &XLINE_TABLES)
                    .map_err(|e| ExecuteError::DbError(format!("Cannot open database: {e}")))?;
                Ok(Arc::new(DBProxy::RocksDB(DB::new(engine))))
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::*;
    #[test]
    fn test_reset() -> Result<(), ExecuteError> {
        let data_dir = PathBuf::from("/tmp/test_reset");
        let db = DBProxy::open(&StorageConfig::RocksDB(data_dir))?;

        let op = WriteOperation::new_put(KV_TABLE, "key1", "value1");
        db.write_batch(vec![op], true)?;
        let res = db.get_value(KV_TABLE, "key1")?;
        assert_eq!(res, Some("value1".as_bytes().to_vec()));

        db.reset()?;

        let res = db.get_values(KV_TABLE, &["key1"])?;
        assert_eq!(res, vec![None]);
        let res = db.get_all(KV_TABLE)?;
        assert!(res.is_empty());

        Ok(())
    }
}
