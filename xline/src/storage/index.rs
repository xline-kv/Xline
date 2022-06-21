use std::collections::BTreeMap;

use clippy_utilities::OverflowArithmetic;
use parking_lot::Mutex;

use super::revision::{KeyRevision, Revision};
use crate::server::command::KeyRange;

/// KV store inner
#[derive(Debug)]
pub(crate) struct Index {
    /// index
    index: Mutex<BTreeMap<Vec<u8>, Vec<KeyRevision>>>,
}

impl Index {
    /// New `Index`
    pub(crate) fn new() -> Self {
        Self {
            index: Mutex::new(BTreeMap::new()),
        }
    }
    /// Get last `KeyRevision` if the key is not deleted
    fn get_last_revision(rvs: &[KeyRevision]) -> Option<KeyRevision> {
        assert!(!rvs.is_empty(), "Get empty revision list");
        rvs.last().filter(|index| !index.is_deleted()).copied()
    }

    /*
    /// Get `KeyRevison` of a key return `None` if key doesn't exist or is deleted
    pub(crate) fn get_one_keyrevision(&self, key: &[u8]) -> Option<KeyRevision> {
        self.index
            .lock()
            .get(key)
            .and_then(|revisions| Self::get_last_revision(revisions))
    }

    /// Get last `KeyRevison` of a key return `None` if key doesn't exist
    pub(crate) fn get_last_keyrevision(&self, key: &[u8]) -> Option<KeyRevision> {
        self.index
            .lock()
            .get(key)
            .and_then(|revisions| revisions.last())
            .cloned()
    }
    */

    /// Get `Revison` of a key
    pub(crate) fn get_one(&self, key: &[u8]) -> Option<Revision> {
        self.index.lock().get(key).and_then(|revisions| {
            Self::get_last_revision(revisions).map(|kv_rev| kv_rev.as_revision())
        })
    }

    /// Get `Revision` of all keys
    pub(crate) fn get_all(&self) -> Vec<Revision> {
        self.index
            .lock()
            .values()
            .filter_map(|revisions| {
                Self::get_last_revision(revisions).map(|kv_rev| kv_rev.as_revision())
            })
            .collect()
    }

    /// Get `Revision` of a range of keys
    pub(crate) fn get_range(&self, range: KeyRange) -> Vec<Revision> {
        self.index
            .lock()
            .range(range)
            .filter_map(|(_k, indexes)| {
                Self::get_last_revision(indexes).map(|kv_rev| kv_rev.as_revision())
            })
            .collect()
    }

    /// Insert or update `KeyRevision` of a key
    pub(crate) fn insert_or_update_revision(
        &self,
        key: &[u8],
        revision: i64,
        sub_revision: &mut i64,
    ) -> KeyRevision {
        let mut index = self.index.lock();
        let kv_rev = if let Some(revisions) = index.get_mut(key) {
            if let Some(rev) = revisions.last() {
                let new_rev = if rev.is_deleted() {
                    KeyRevision::new(revision, 1, revision, *sub_revision)
                } else {
                    KeyRevision::new(
                        rev.create_revision,
                        rev.version.overflow_add(1),
                        revision,
                        *sub_revision,
                    )
                };
                revisions.push(new_rev);
                new_rev
            } else {
                panic!("Get empty revision list for key {:?}", key);
            }
        } else {
            let new_rev = KeyRevision::new(revision, 1, revision, *sub_revision);
            let _prev_val = index.insert(key.to_vec(), vec![new_rev]);
            new_rev
        };
        *sub_revision = sub_revision.overflow_add(1);
        kv_rev
    }

    /// Mark one key as deleted and return latest revision before deletion
    pub(crate) fn delete_one(
        &self,
        key: &[u8],
        revision: i64,
        sub_revision: &mut i64,
    ) -> Option<(Revision, Revision)> {
        let del_rev = KeyRevision::new_deletion(revision, *sub_revision);
        self.index.lock().get_mut(key).and_then(|revisions| {
            Self::get_last_revision(revisions).map(|rev| {
                revisions.push(del_rev);
                *sub_revision = sub_revision.overflow_add(1);
                (rev.as_revision(), del_rev.as_revision())
            })
        })
    }

    /// Mark all keys as deleted and return latest revision before deletion and deletion revision
    pub(crate) fn delete_all(
        &self,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Vec<(Revision, Revision)> {
        self.index
            .lock()
            .values_mut()
            .filter_map(|revisions| {
                Self::get_last_revision(revisions).map(|rev| {
                    let del_rev = KeyRevision::new_deletion(revision, *sub_revision);
                    revisions.push(del_rev);
                    *sub_revision = sub_revision.overflow_add(1);
                    (rev.as_revision(), del_rev.as_revision())
                })
            })
            .collect()
    }

    /// Mark range of keys as deleted and return latest revision before deletion and deletion revision
    pub(crate) fn delete_range(
        &self,
        range: KeyRange,
        revision: i64,
        sub_revision: &mut i64,
    ) -> Vec<(Revision, Revision)> {
        self.index
            .lock()
            .range_mut(range)
            .filter_map(|(_k, revisions)| {
                Self::get_last_revision(revisions).map(|rev| {
                    let del_rev = KeyRevision::new_deletion(revision, *sub_revision);
                    revisions.push(del_rev);
                    *sub_revision = sub_revision.overflow_add(1);
                    (rev.as_revision(), del_rev.as_revision())
                })
            })
            .collect()
    }
}
