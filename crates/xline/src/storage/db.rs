use std::{collections::HashMap, path::Path, sync::Arc};

use engine::{Engine, EngineType, Snapshot, StorageEngine, WriteOperation};
use prost::Message;
use utils::{
    config::EngineConfig,
    table_names::{
        ALARM_TABLE, AUTH_TABLE, KV_TABLE, LEASE_TABLE, META_TABLE, ROLE_TABLE, USER_TABLE,
        XLINE_TABLES,
    },
};
use xlineapi::{execute_error::ExecuteError, AlarmMember};

use super::{
    auth_store::{AUTH_ENABLE_KEY, AUTH_REVISION_KEY},
    revision::KeyRevision,
    storage_api::StorageApi,
};
use crate::{
    rpc::{KeyValue, PbLease, Role, User},
    server::command::APPLIED_INDEX_KEY,
    storage::Revision,
};

/// Key of finished compact revision
pub(crate) const FINISHED_COMPACT_REVISION: &str = "finished_compact_revision";
/// Key of scheduled compact revision
pub(crate) const SCHEDULED_COMPACT_REVISION: &str = "scheduled_compact_revision";

/// Database to store revision to kv mapping
#[derive(Debug)]
pub struct DB {
    /// internal storage of `DB`
    engine: Arc<Engine>,
}

impl DB {
    /// Create a new `DB`
    ///
    /// # Errors
    /// Return `ExecuteError::DbError` when open db failed
    #[inline]
    pub fn open(config: &EngineConfig) -> Result<Arc<Self>, ExecuteError> {
        let engine_type = match *config {
            EngineConfig::Memory => EngineType::Memory,
            EngineConfig::RocksDB(ref path) => EngineType::Rocks(path.clone()),
            _ => unreachable!("Not supported storage type"),
        };
        let engine = Engine::new(engine_type, &XLINE_TABLES)
            .map_err(|e| ExecuteError::DbError(format!("Cannot open database: {e}")))?;
        Ok(Arc::new(Self {
            engine: Arc::new(engine),
        }))
    }

    /// Get del lease key buffer
    #[inline]
    fn get_del_lease_key_buffer(ops: &[WriteOp]) -> HashMap<i64, Vec<u8>> {
        ops.iter()
            .filter_map(|op| {
                if let WriteOp::DeleteLease(lease_id) = *op {
                    Some((lease_id, lease_id.encode_to_vec()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>()
    }

    /// get del alarm buffer
    #[inline]
    fn get_del_alarm_buffer(ops: &[WriteOp]) -> Vec<u8> {
        ops.iter()
            .find_map(|op| {
                if let WriteOp::DeleteAlarm(ref key) = *op {
                    Some(key.encode_to_vec())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }
}
#[async_trait::async_trait]
impl StorageApi for DB {
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

    fn get_snapshot(&self, snap_path: impl AsRef<Path>) -> Result<Snapshot, ExecuteError> {
        self.engine
            .get_snapshot(snap_path, &XLINE_TABLES)
            .map_err(|e| ExecuteError::DbError(format!("Failed to get snapshot, error: {e}")))
    }

    async fn reset(&self, snapshot: Option<Snapshot>) -> Result<(), ExecuteError> {
        if let Some(snap) = snapshot {
            self.engine
                .apply_snapshot(snap, &XLINE_TABLES)
                .await
                .map_err(|e| ExecuteError::DbError(format!("Failed to reset database, error: {e}")))
        } else {
            let start = vec![];
            let end = vec![0xff];
            let ops = XLINE_TABLES
                .iter()
                .map(|table| {
                    WriteOperation::new_delete_range(table, start.as_slice(), end.as_slice())
                })
                .collect();
            self.engine
                .write_batch(ops, true)
                .map_err(|e| ExecuteError::DbError(format!("Failed to reset database, error: {e}")))
        }
    }

    fn flush_ops(&self, ops: Vec<WriteOp>) -> Result<Vec<(Vec<u8>, KeyRevision)>, ExecuteError> {
        let mut wr_ops = Vec::new();
        let mut revs = Vec::new();
        let del_lease_key_buffer = Self::get_del_lease_key_buffer(&ops);
        let del_alarm_buffer = Self::get_del_alarm_buffer(&ops);
        for op in ops {
            let wop = match op {
                WriteOp::PutKeyValue(rev, value) => {
                    revs.push((
                        value.key.clone(),
                        KeyRevision::new(
                            value.create_revision,
                            value.version,
                            rev.revision(),
                            rev.sub_revision(),
                        ),
                    ));
                    let key = rev.encode_to_vec();
                    WriteOperation::new_put(KV_TABLE, key, value.encode_to_vec())
                }
                WriteOp::PutAppliedIndex(index) => WriteOperation::new_put(
                    META_TABLE,
                    APPLIED_INDEX_KEY.as_bytes().to_vec(),
                    index.to_le_bytes().to_vec(),
                ),
                WriteOp::PutLease(lease) => WriteOperation::new_put(
                    LEASE_TABLE,
                    lease.id.encode_to_vec(),
                    lease.encode_to_vec(),
                ),
                WriteOp::PutFinishedCompactRevision(rev) => WriteOperation::new_put(
                    META_TABLE,
                    FINISHED_COMPACT_REVISION.as_bytes().to_vec(),
                    rev.to_le_bytes().to_vec(),
                ),
                WriteOp::PutScheduledCompactRevision(rev) => WriteOperation::new_put(
                    META_TABLE,
                    SCHEDULED_COMPACT_REVISION.as_bytes().to_vec(),
                    rev.to_le_bytes().to_vec(),
                ),
                WriteOp::DeleteKeyValue(rev) => WriteOperation::new_delete(KV_TABLE, rev),
                WriteOp::DeleteLease(lease_id) => {
                    let key = del_lease_key_buffer.get(&lease_id).unwrap_or_else(|| {
                        panic!("lease_id({lease_id}) is not in del_lease_key_buffer")
                    });
                    WriteOperation::new_delete(LEASE_TABLE, key)
                }
                WriteOp::PutAuthEnable(enable) => WriteOperation::new_put(
                    AUTH_TABLE,
                    AUTH_ENABLE_KEY.to_vec(),
                    vec![u8::from(enable)],
                ),
                WriteOp::PutAuthRevision(rev) => WriteOperation::new_put(
                    AUTH_TABLE,
                    AUTH_REVISION_KEY.to_vec(),
                    rev.encode_to_vec(),
                ),
                WriteOp::PutUser(user) => {
                    let value = user.encode_to_vec();
                    WriteOperation::new_put(USER_TABLE, user.name.clone(), value)
                }
                WriteOp::DeleteUser(name) => {
                    WriteOperation::new_delete(USER_TABLE, name.as_bytes())
                }
                WriteOp::PutRole(role) => {
                    let value = role.encode_to_vec();
                    WriteOperation::new_put(ROLE_TABLE, role.name.clone(), value)
                }
                WriteOp::DeleteRole(name) => {
                    WriteOperation::new_delete(ROLE_TABLE, name.as_bytes())
                }
                WriteOp::PutAlarm(alarm) => {
                    let key = alarm.encode_to_vec();
                    WriteOperation::new_put(ALARM_TABLE, key, vec![])
                }
                WriteOp::DeleteAlarm(_key) => {
                    WriteOperation::new_delete(ALARM_TABLE, del_alarm_buffer.as_ref())
                }
            };
            wr_ops.push(wop);
        }
        self.engine
            .write_batch(wr_ops, false)
            .map_err(|e| ExecuteError::DbError(format!("Failed to flush ops, error: {e}")))?;
        Ok(revs)
    }

    fn hash(&self) -> Result<u32, ExecuteError> {
        let mut hasher = crc32fast::Hasher::new();
        for table in XLINE_TABLES {
            hasher.update(table.as_bytes());
            let kv_pairs = self.engine.get_all(table).map_err(|e| {
                ExecuteError::DbError(format!("Failed to get all keys from {table:?}: {e}"))
            })?;
            for (k, v) in kv_pairs {
                hasher.update(&k);
                hasher.update(&v);
            }
        }
        Ok(hasher.finalize())
    }

    fn estimated_file_size(&self) -> u64 {
        self.engine.estimated_file_size()
    }

    fn file_size(&self) -> Result<u64, ExecuteError> {
        self.engine
            .file_size()
            .map_err(|e| ExecuteError::DbError(format!("Failed to get file size, error: {e}")))
    }
}

/// Buffered Write Operation
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WriteOp<'a> {
    /// Put a key-value pair to kv table
    PutKeyValue(Revision, KeyValue),
    /// Put the applied index to meta table
    PutAppliedIndex(u64),
    /// Put a lease to lease table
    PutLease(PbLease),
    /// Put a finished compact revision into meta table
    PutFinishedCompactRevision(i64),
    /// Put a scheduled compact revision into meta table
    PutScheduledCompactRevision(i64),
    /// Delete a key-value pair from kv table
    DeleteKeyValue(&'a [u8]),
    /// Delete a lease from lease table
    DeleteLease(i64),
    /// Put a auth enable flag to auth table
    PutAuthEnable(bool),
    /// Put a auth revision to auth table
    PutAuthRevision(i64),
    /// Put a user to user table
    PutUser(User),
    /// Delete a user from user table
    DeleteUser(&'a str),
    /// Put a role to role table
    PutRole(Role),
    /// Delete a role from role table
    DeleteRole(&'a str),
    /// Put a alarm member to alarm table
    PutAlarm(AlarmMember),
    /// Delete a alarm member from alarm table
    DeleteAlarm(AlarmMember),
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use engine::SnapshotApi;
    use test_macros::abort_on_panic;

    use super::*;
    use crate::storage::Revision;
    #[tokio::test]
    #[abort_on_panic]
    async fn test_reset() -> Result<(), ExecuteError> {
        let data_dir = PathBuf::from("/tmp/test_reset");
        let db = DB::open(&EngineConfig::RocksDB(data_dir.clone()))?;

        let revision = Revision::new(1, 1);
        let key = revision.encode_to_vec();
        let kv = KeyValue {
            key: "value1".into(),
            ..Default::default()
        };
        let ops = vec![WriteOp::PutKeyValue(revision, kv.clone())];
        _ = db.flush_ops(ops)?;
        let res = db.get_value(KV_TABLE, &key)?;
        assert_eq!(res, Some(kv.encode_to_vec()));

        db.reset(None).await?;

        let res = db.get_values(KV_TABLE, &[&key])?;
        assert_eq!(res, vec![None]);
        let res = db.get_all(KV_TABLE)?;
        assert!(res.is_empty());

        std::fs::remove_dir_all(data_dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_db_snapshot() -> Result<(), ExecuteError> {
        let dir = PathBuf::from("/tmp/test_db_snapshot");
        let origin_db_path = dir.join("origin_db");
        let new_db_path = dir.join("new_db");
        let snapshot_path = dir.join("snapshot");
        let origin_db = DB::open(&EngineConfig::RocksDB(origin_db_path))?;

        let revision = Revision::new(1, 1);
        let key = revision.encode_to_vec();
        let kv = KeyValue {
            key: "value1".into(),
            ..Default::default()
        };
        let ops = vec![WriteOp::PutKeyValue(revision, kv.clone())];
        _ = origin_db.flush_ops(ops)?;

        let snapshot = origin_db.get_snapshot(snapshot_path)?;

        let new_db = DB::open(&EngineConfig::RocksDB(new_db_path))?;
        new_db.reset(Some(snapshot)).await?;

        let res = new_db.get_values(KV_TABLE, &[&key])?;
        assert_eq!(res, vec![Some(kv.encode_to_vec())]);

        std::fs::remove_dir_all(dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_db_snapshot_wrong_type() -> Result<(), ExecuteError> {
        let dir = PathBuf::from("/tmp/test_db_snapshot_wrong_type");
        let db_path = dir.join("db");
        let snapshot_path = dir.join("snapshot");
        let rocks_db = DB::open(&EngineConfig::RocksDB(db_path))?;
        let mem_db = DB::open(&EngineConfig::Memory)?;

        let rocks_snap = rocks_db.get_snapshot(snapshot_path)?;
        let res = mem_db.reset(Some(rocks_snap)).await;
        assert!(res.is_err());

        std::fs::remove_dir_all(dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_get_snapshot() -> Result<(), ExecuteError> {
        let dir = PathBuf::from("/tmp/test_get_snapshot");
        let data_path = dir.join("data");
        let snapshot_path = dir.join("snapshot");
        let db = DB::open(&EngineConfig::RocksDB(data_path))?;
        let mut res = db.get_snapshot(snapshot_path)?;
        assert_ne!(res.size(), 0);
        res.clean().await.unwrap();

        std::fs::remove_dir_all(dir).unwrap();
        Ok(())
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn test_db_write_ops() {
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let lease = PbLease {
            id: 1,
            ttl: 10,
            remaining_ttl: 10,
        };
        let lease_bytes = lease.encode_to_vec();
        let user = User {
            name: "user".into(),
            password: "password".into(),
            roles: vec!["role".into()],
            options: None,
        };
        let user_bytes = user.encode_to_vec();
        let role = Role {
            name: "role".into(),
            key_permission: vec![],
        };
        let role_bytes = role.encode_to_vec();
        let kv = KeyValue {
            key: b"value".to_vec(),
            ..Default::default()
        };
        let write_ops = vec![
            WriteOp::PutKeyValue(Revision::new(1, 2), kv.clone()),
            WriteOp::PutAppliedIndex(5),
            WriteOp::PutLease(lease),
            WriteOp::PutAuthEnable(true),
            WriteOp::PutAuthRevision(1),
            WriteOp::PutUser(user),
            WriteOp::PutRole(role),
        ];
        _ = db.flush_ops(write_ops).unwrap();
        assert_eq!(
            db.get_value(KV_TABLE, Revision::new(1, 2).encode_to_vec())
                .unwrap(),
            Some(kv.encode_to_vec())
        );
        assert_eq!(
            db.get_value(META_TABLE, b"applied_index").unwrap(),
            Some(5u64.to_le_bytes().to_vec())
        );
        assert_eq!(
            db.get_value(LEASE_TABLE, 1i64.encode_to_vec()).unwrap(),
            Some(lease_bytes)
        );
        assert_eq!(
            db.get_value(AUTH_TABLE, b"enable").unwrap(),
            Some(vec![u8::from(true)])
        );
        assert_eq!(
            db.get_value(AUTH_TABLE, b"revision").unwrap(),
            Some(1u64.encode_to_vec())
        );
        assert_eq!(db.get_value(USER_TABLE, b"user").unwrap(), Some(user_bytes));
        assert_eq!(db.get_value(ROLE_TABLE, b"role").unwrap(), Some(role_bytes));

        let del_ops = vec![
            WriteOp::DeleteLease(1),
            WriteOp::DeleteUser("user"),
            WriteOp::DeleteRole("role"),
        ];
        _ = db.flush_ops(del_ops).unwrap();
        assert_eq!(
            db.get_value(LEASE_TABLE, 1i64.encode_to_vec()).unwrap(),
            None
        );
        assert_eq!(db.get_value(USER_TABLE, b"user").unwrap(), None);
        assert_eq!(db.get_value(ROLE_TABLE, b"role").unwrap(), None);
    }
}
