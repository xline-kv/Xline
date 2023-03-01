use std::sync::Arc;

use engine::{engine_api::StorageEngine, Delete, Put, WriteOperation};

use super::{storage_api::StorageApi, ExecuteError};

/// Xline Server Storage Table
#[allow(dead_code)]
pub const XLINETABLES: [&str; 3] = ["kv", "lease", "auth"];

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
