use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use prost::Message;

use super::{storage_api::StorageApi, ExecuteError, Revision};
use crate::rpc::KeyValue;

/// Database to store revision to kv mapping
#[derive(Debug)]
pub(crate) struct DB<S>
where
    S: StorageApi,
{
    /// internal storage of `DB`
    storage: RwLock<S>,
}

impl<S> DB<S>
where
    S: StorageApi,
{
    /// New `DB`
    pub(crate) fn new(storage: S) -> Self {
        Self {
            storage: RwLock::new(storage),
        }
    }

    /// Insert a `KeyValue`
    pub(crate) fn insert(&self, revision: Revision, kv: &KeyValue) -> Result<(), ExecuteError> {
        let mut storage = self.storage.write();
        let key = revision.encode_to_vec();
        let value = kv.encode_to_vec();
        let _prev_val = storage.insert(key, value).map_err(|e| {
            ExecuteError::DbError(format!("Failed to insert key-value, error: {e}"))
        })?;
        Ok(())
    }

    /// Get a list of `KeyValue` by `Revision`
    pub(crate) fn get_values(&self, revisions: &[Revision]) -> Result<Vec<KeyValue>, ExecuteError> {
        let storage = self.storage.read();
        let keys = revisions
            .iter()
            .map(Revision::encode_to_vec)
            .collect::<Vec<_>>();
        let values = storage
            .batch_get(&keys)
            .map_err(|e| {
                ExecuteError::DbError(format!("Failed to get revisions {revisions:?}: {e}"))
            })?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(values.len(), revisions.len(), "Index doesn't match with DB");
        let kvs = values
            .into_iter()
            .map(|v| KeyValue::decode(v.as_slice()))
            .collect::<Result<_, _>>()
            .map_err(|e| {
                ExecuteError::DbError(format!("Failed to decode key-value from DB, error: {e}"))
            })?;
        Ok(kvs)
    }

    /// Mark deletion for keys
    pub(crate) fn mark_deletions(
        &self,
        revisions: &[(Revision, Revision)],
    ) -> Result<Vec<KeyValue>, ExecuteError> {
        let storage = self.storage.upgradable_read();
        let prev_revisions = revisions
            .iter()
            .map(|&(prev_rev, _)| prev_rev)
            .collect::<Vec<_>>();
        let prev_kvs = self.get_values(&prev_revisions)?;
        assert!(
            prev_kvs.len() == revisions.len(),
            "Index doesn't match with DB"
        );
        let mut storage = RwLockUpgradableReadGuard::upgrade(storage);
        prev_kvs
            .iter()
            .zip(revisions.iter())
            .try_for_each(|(kv, &(_, new_rev))| {
                let del_kv = KeyValue {
                    key: kv.key.clone(),
                    mod_revision: new_rev.revision(),
                    ..KeyValue::default()
                };
                let key = new_rev.encode_to_vec();
                let value = del_kv.encode_to_vec();
                let _prev_val = storage.insert(key, value).map_err(|e| {
                    ExecuteError::DbError(format!("Failed to insert key-value, error: {e}"))
                })?;
                Ok(())
            })?;
        Ok(prev_kvs)
    }
}
