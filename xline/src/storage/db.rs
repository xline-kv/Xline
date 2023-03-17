use std::{collections::HashMap, sync::Arc};

use curp::cmd::ProposeId;
use engine::{
    engine_api::StorageEngine, memory_engine::MemoryEngine, rocksdb_engine::RocksEngine,
    WriteOperation,
};
use parking_lot::Mutex;
use prost::Message;
use utils::config::StorageConfig;

use crate::{
    rpc::{PbLease, Role, User},
    server::command::{APPLIED_INDEX_KEY, META_TABLE},
};

use super::{
    auth_store::{AUTH_ENABLE_KEY, AUTH_REVISION_KEY, AUTH_TABLE, ROLE_TABLE, USER_TABLE},
    kv_store::KV_TABLE,
    lease_store::LEASE_TABLE,
    storage_api::StorageApi,
    ExecuteError, Revision,
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
#[derive(Debug)]
pub struct DB<S: StorageEngine> {
    /// internal storage of `DB`
    engine: Arc<S>,
    /// Buffer
    buffer: Mutex<HashMap<ProposeId, Vec<WriteOp>>>,
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
            buffer: Mutex::new(HashMap::new()),
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

    fn reset(&self) -> Result<(), ExecuteError> {
        let start = vec![];
        let end = vec![0xff];
        let ops = XLINE_TABLES
            .iter()
            .map(|table| WriteOperation::new_delete_range(table, start.as_slice(), end.as_slice()))
            .collect();
        self.engine
            .write_batch(ops, true)
            .map_err(|e| ExecuteError::DbError(format!("Failed to reset database, error: {e}")))
    }

    fn buffer_op(&self, propose_id: &ProposeId, op: WriteOp) {
        let mut buffer = self.buffer.lock();
        if let Some(ops) = buffer.get_mut(propose_id) {
            ops.push(op);
        } else {
            let _ignore = buffer.insert(propose_id.clone(), vec![op]);
        }
    }

    fn flush(&self, id: &ProposeId) -> Result<(), ExecuteError> {
        if let Some(ops) = self.buffer.lock().remove(id) {
            let wr_ops = ops.into_iter().map(WriteOperation::from).collect();
            self.engine
                .write_batch(wr_ops, false)
                .map_err(|e| ExecuteError::DbError(format!("Failed to flush ops, error: {e}")))?;
        }
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
#[derive(Debug)]
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

    fn reset(&self) -> Result<(), ExecuteError> {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.reset(),
            DBProxy::RocksDB(ref inner_db) => inner_db.reset(),
        }
    }

    fn buffer_op(&self, id: &ProposeId, op: WriteOp) {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.buffer_op(id, op),
            DBProxy::RocksDB(ref inner_db) => inner_db.buffer_op(id, op),
        }
    }

    fn flush(&self, id: &ProposeId) -> Result<(), ExecuteError> {
        match *self {
            DBProxy::MemDB(ref inner_db) => inner_db.flush(id),
            DBProxy::RocksDB(ref inner_db) => inner_db.flush(id),
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

/// Buffered Write Operation
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WriteOp {
    /// Put a key-value pair to kv table
    PutKeyValue(Revision, Vec<u8>),
    /// Put the applied index to meta table
    PutAppliedIndex(u64),
    /// Put a lease to lease table
    PutLease(PbLease),
    /// Delete a lease from lease table
    DeleteLease(i64),
    /// Put a auth enable flag to auth table
    PutAuthEnable(bool),
    /// Put a auth revision to auth table
    PutAuthRevision(i64),
    /// Put a user to user table
    PutUser(User),
    /// Delete a user from user table
    DeleteUser(String),
    /// Put a role to role table
    PutRole(Role),
    /// Delete a role from role table
    DeleteRole(String),
}

impl From<WriteOp> for WriteOperation {
    #[inline]
    fn from(wr_op: WriteOp) -> Self {
        match wr_op {
            WriteOp::PutKeyValue(rev, value) => {
                let key = rev.encode_to_vec();
                WriteOperation::new_put(KV_TABLE, key, value)
            }
            WriteOp::PutAppliedIndex(index) => {
                WriteOperation::new_put(META_TABLE, APPLIED_INDEX_KEY, index.to_le_bytes())
            }
            WriteOp::PutLease(lease) => WriteOperation::new_put(
                LEASE_TABLE,
                lease.id.encode_to_vec(),
                lease.encode_to_vec(),
            ),
            WriteOp::DeleteLease(id) => WriteOperation::new_delete(LEASE_TABLE, id.encode_to_vec()),
            WriteOp::PutAuthEnable(enable) => {
                WriteOperation::new_put(AUTH_TABLE, AUTH_ENABLE_KEY, vec![u8::from(enable)])
            }
            WriteOp::PutAuthRevision(rev) => {
                WriteOperation::new_put(AUTH_TABLE, AUTH_REVISION_KEY, rev.encode_to_vec())
            }
            WriteOp::PutUser(user) => {
                let value = user.encode_to_vec();
                WriteOperation::new_put(USER_TABLE, user.name, value)
            }
            WriteOp::DeleteUser(name) => WriteOperation::new_delete(USER_TABLE, name),
            WriteOp::PutRole(role) => {
                let value = role.encode_to_vec();
                WriteOperation::new_put(ROLE_TABLE, role.name, value)
            }
            WriteOp::DeleteRole(name) => WriteOperation::new_delete(ROLE_TABLE, name),
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

        let revision = Revision::new(1, 1);
        let key = revision.encode_to_vec();
        let id = ProposeId::new("test-id".to_owned());
        db.buffer_op(&id, WriteOp::PutKeyValue(revision, "value1".into()));
        db.flush(&id)?;
        let res = db.get_value(KV_TABLE, &key)?;
        assert_eq!(res, Some("value1".as_bytes().to_vec()));

        db.reset()?;

        let res = db.get_values(KV_TABLE, &[&key])?;
        assert_eq!(res, vec![None]);
        let res = db.get_all(KV_TABLE)?;
        assert!(res.is_empty());

        Ok(())
    }
}
