use curp::error::ExecuteError;
use parking_lot::RwLock;
use prost::Message;

use super::{revision::Revision, storage_api::StorageApi};
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
            ExecuteError::InvalidCommand(format!("Failed to insert revision {revision:?}: {e}"))
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
                ExecuteError::InvalidCommand(format!("Failed to get revisions {revisions:?}: {e}"))
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
                ExecuteError::InvalidCommand(format!(
                    "Failed to decode revisions {revisions:?}: {e}",
                ))
            })?;
        Ok(kvs)
    }

    /// Mark deletion for keys
    pub(crate) fn mark_deletions(
        &self,
        revisions: &[(Revision, Revision)],
    ) -> Result<Vec<KeyValue>, ExecuteError> {
        let mut storage = self.storage.write();
        let prev_kvs: Vec<KeyValue> = revisions
            .iter()
            .map(|&(prev_rev, _)| {
                let key = prev_rev.encode_to_vec();
                let value = storage.get(key).map_err(|e| {
                    ExecuteError::InvalidCommand(format!(
                        "Failed to get revision {prev_rev:?}: {e}",
                    ))
                })?;
                if let Some(value) = value {
                    let kv = KeyValue::decode(value.as_slice()).map_err(|e| {
                        ExecuteError::InvalidCommand(format!(
                            "Failed to decode revision {prev_rev:?}: {e}",
                        ))
                    })?;
                    Ok(kv)
                } else {
                    Err(ExecuteError::InvalidCommand(format!(
                        "Failed to get revision {prev_rev:?} from DB",
                    )))
                }
            })
            .collect::<Result<_, _>>()?;
        assert!(
            prev_kvs.len() == revisions.len(),
            "Index doesn't match with DB"
        );
        prev_kvs
            .iter()
            .zip(revisions.iter())
            .map(|(kv, &(_, new_rev))| {
                let del_kv = KeyValue {
                    key: kv.key.clone(),
                    mod_revision: new_rev.revision(),
                    ..KeyValue::default()
                };
                let key = new_rev.encode_to_vec();
                let value = del_kv.encode_to_vec();
                let _prev_val = storage.insert(key, value).map_err(|e| {
                    ExecuteError::InvalidCommand(format!(
                        "Failed to insert revision {new_rev:?}: {e}",
                    ))
                });
                Ok(del_kv)
            })
            .collect()
    }
}
