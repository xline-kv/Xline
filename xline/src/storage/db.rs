use std::collections::HashMap;

//use clippy_utilities::OverflowArithmetic;
use parking_lot::Mutex;

use super::revision::Revision;
use crate::rpc::KeyValue;

/// Database to store revision to kv mapping
#[derive(Debug)]
pub(crate) struct DB {
    /// internal storage of `DB`
    storage: Mutex<HashMap<Revision, KeyValue>>,
}

impl DB {
    /// New `DB`
    pub(crate) fn new() -> Self {
        Self {
            storage: Mutex::new(HashMap::new()),
        }
    }

    /// Insert a `KeyValue`
    pub(crate) fn insert(&self, revision: Revision, kv: KeyValue) -> Option<KeyValue> {
        self.storage.lock().insert(revision, kv)
    }

    /// Get a `KeyValue`
    pub(crate) fn get(&self, revision: &Revision) -> Option<KeyValue> {
        self.storage.lock().get(revision).cloned()
    }

    /// Get a list of `KeyValue`
    pub(crate) fn get_values(&self, revisions: &[Revision]) -> Vec<KeyValue> {
        let storage = self.storage.lock();
        revisions
            .iter()
            .filter_map(|revision| storage.get(revision).cloned())
            .collect()
    }

    /// Mark deletion for keys
    /// TODO support don't return `prev_kvs`
    pub(crate) fn mark_deletions(&self, revisions: &[(Revision, Revision)]) -> Vec<KeyValue> {
        let mut storage = self.storage.lock();
        let prev_kvs: Vec<KeyValue> = revisions
            .iter()
            .map(|&(ref prev_rev, _)| {
                storage
                    .get(prev_rev)
                    .cloned()
                    .unwrap_or_else(|| panic!("Failed to get revision {:?} from DB", prev_rev))
            })
            .collect();
        assert!(
            prev_kvs.len() == revisions.len(),
            "Index doesn't match with DB"
        );
        prev_kvs
            .iter()
            .zip(revisions.iter())
            .for_each(|(kv, &(_, new_rev))| {
                let del_kv = KeyValue {
                    key: kv.key.clone(),
                    mod_revision: new_rev.revision(),
                    ..KeyValue::default()
                };
                let _prev_val = storage.insert(new_rev, del_kv);
            });

        prev_kvs
    }
}
