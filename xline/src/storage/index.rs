use std::collections::{BTreeMap, HashMap};

use clippy_utilities::OverflowArithmetic;
use parking_lot::Mutex;

use super::revision::{KeyRevision, Revision};
use crate::server::command::{KeyRange, RangeType};

/// Keys to revisions mapping
#[derive(Debug)]
pub(crate) struct Index {
    /// Inner struct of `Index`
    inner: Mutex<IndexInner>,
}

/// Inner struct of `Index`
#[derive(Debug)]
struct IndexInner {
    /// index
    index: BTreeMap<Vec<u8>, Vec<KeyRevision>>,
    /// unavailable cache, used to cache unavailable revisions to keys mapping
    unavailable_cache: HashMap<i64, Vec<Vec<u8>>>,
}

impl Index {
    /// New `Index`
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(IndexInner {
                index: BTreeMap::new(),
                unavailable_cache: HashMap::new(),
            }),
        }
    }

    /// Filter out `KeyRevision` that is less than one revision and convert to `Revision`
    fn filter_revision(revs: &[KeyRevision], revision: i64) -> Vec<Revision> {
        revs.iter()
            .filter(|rev| rev.mod_revision >= revision && rev.available)
            .map(KeyRevision::as_revision)
            .collect()
    }

    /// Get specified or last `KeyRevision` if the key is not deleted, and convert to `Revision`
    fn get_revision(revs: &[KeyRevision], revision: i64) -> Option<Revision> {
        // TODO: handle future revision
        let rev = if revision <= 0 {
            revs.iter().rev().find(|kr| kr.available)
        } else {
            let idx = match revs.binary_search_by(|rev| rev.mod_revision.cmp(&revision)) {
                Ok(idx) => idx,
                Err(e) => {
                    if e == 0 {
                        return None;
                    }
                    e.overflow_sub(1)
                }
            };
            revs.get(idx)
        };
        rev.filter(|kr| !kr.is_deleted())
            .map(KeyRevision::as_revision)
    }

    /// Insert `KeyRevision` of deleted and generate `Revision` pair of deleted
    fn gen_del_revision(
        revs: &mut Vec<KeyRevision>,
        revision: i64,
        sub_revision: i64,
    ) -> Option<(Revision, Revision)> {
        let last_rev = Self::get_revision(revs, 0)?;
        let del_rev = KeyRevision::new_deletion(revision, sub_revision);
        revs.push(del_rev);
        Some((last_rev, del_rev.as_revision()))
    }

    /// Mark the `KeyRevision` as available
    pub(crate) fn mark_available(&self, revision: i64) {
        let mut inner = self.inner.lock();
        let Some(keys) = inner.unavailable_cache.remove(&revision) else {
            return;
        };
        for key in keys {
            let Some(revs) = inner.index.get_mut(&key) else {
                panic!("key({key:?}) should exist in index");
            };
            if let Some(rev) = revs.iter_mut().find(|rev| rev.mod_revision == revision) {
                rev.available = true;
            }
        }
    }
}

/// Operations of Index
pub(crate) trait IndexOperate {
    /// Get `Revision` of keys, get the latest `Revision` when revision <= 0
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision>;

    /// Get `Revision` of keys from one revision
    fn get_from_rev(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision>;

    /// Mark keys as deleted and return latest revision before deletion and deletion revision
    fn delete(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> Vec<(Revision, Revision)>;

    /// Insert or update `KeyRevision` of a key
    fn insert_or_update_revision(
        &self,
        key: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> KeyRevision;

    /// Restore `KeyRevision` of a key
    fn restore(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    );

    // TODO: fn compact(rev:i64)
}

impl IndexOperate for Index {
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        let inner = self.inner.lock();
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => inner
                .index
                .get(key)
                .and_then(|revs| Self::get_revision(revs, revision))
                .map(|rev| vec![rev])
                .unwrap_or_default(),
            RangeType::AllKeys => inner
                .index
                .values()
                .filter_map(|revs| Self::get_revision(revs, revision))
                .collect(),
            RangeType::Range => inner
                .index
                .range(KeyRange::new(key, range_end))
                .filter_map(|(_k, revs)| Self::get_revision(revs, revision))
                .collect(),
        }
    }

    fn get_from_rev(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        let inner = self.inner.lock();
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => inner
                .index
                .get(key)
                .map(|revs| Self::filter_revision(revs, revision))
                .unwrap_or_default(),
            RangeType::AllKeys => inner
                .index
                .values()
                .flat_map(|revs| Self::filter_revision(revs, revision))
                .collect(),
            RangeType::Range => inner
                .index
                .range(KeyRange::new(key, range_end))
                .flat_map(|(_k, revs)| Self::filter_revision(revs, revision))
                .collect(),
        }
    }

    fn delete(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> Vec<(Revision, Revision)> {
        let mut inner = self.inner.lock();

        let (pairs, keys) = match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => {
                let pairs: Vec<(Revision, Revision)> = inner
                    .index
                    .get_mut(key)
                    .into_iter()
                    .filter_map(|revs| Self::gen_del_revision(revs, revision, sub_revision))
                    .collect();
                let keys = if pairs.is_empty() {
                    vec![]
                } else {
                    vec![key.to_vec()]
                };
                (pairs, keys)
            }
            RangeType::AllKeys => {
                let pairs = inner
                    .index
                    .values_mut()
                    .zip(0..)
                    .filter_map(|(revs, i)| {
                        Self::gen_del_revision(revs, revision, sub_revision.overflow_add(i))
                    })
                    .collect();
                let keys = inner.index.keys().cloned().collect();
                (pairs, keys)
            }
            RangeType::Range => {
                let mut keys = vec![];
                let pairs = inner
                    .index
                    .range_mut(KeyRange::new(key, range_end))
                    .zip(0..)
                    .filter_map(|((k, revs), i)| {
                        keys.push(k.clone());
                        Self::gen_del_revision(revs, revision, sub_revision.overflow_add(i))
                    })
                    .collect();
                (pairs, keys)
            }
        };
        if !keys.is_empty() {
            assert!(inner.unavailable_cache.insert(revision, keys).is_none());
        }
        pairs
    }

    fn insert_or_update_revision(
        &self,
        key: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> KeyRevision {
        let mut inner = self.inner.lock();
        let new_rev = if let Some(revisions) = inner.index.get_mut(key) {
            if let Some(rev) = revisions.last() {
                let new_rev = if rev.is_deleted() {
                    KeyRevision::new(revision, 1, revision, sub_revision)
                } else {
                    KeyRevision::new(
                        rev.create_revision,
                        rev.version.overflow_add(1),
                        revision,
                        sub_revision,
                    )
                };
                revisions.push(new_rev);
                new_rev
            } else {
                panic!("Get empty revision list for key {key:?}");
            }
        } else {
            let new_rev = KeyRevision::new(revision, 1, revision, sub_revision);
            let _prev_val = inner.index.insert(key.to_vec(), vec![new_rev]);
            new_rev
        };
        inner
            .unavailable_cache
            .entry(revision)
            .or_default()
            .push(key.to_vec());
        new_rev
    }

    fn restore(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    ) {
        let mut new_rev = KeyRevision::new(create_revision, version, revision, sub_revision);
        new_rev.available = true;
        self.inner
            .lock()
            .index
            .entry(key)
            .or_insert_with(Vec::new)
            .push(new_rev);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn new_available_key_revision(
        create_revision: i64,
        version: i64,
        revision: i64,
        sub_revision: i64,
    ) -> KeyRevision {
        let mut rev = KeyRevision::new(create_revision, version, revision, sub_revision);
        rev.available = true;
        rev
    }

    fn init_and_test_insert() -> Index {
        let index = Index::new();

        index.insert_or_update_revision(b"key", 1, 3);
        index.insert_or_update_revision(b"key", 2, 2);
        index.insert_or_update_revision(b"key", 3, 1);

        index.mark_available(1);
        index.mark_available(2);
        index.mark_available(3);

        assert_eq!(
            index.inner.lock().index,
            BTreeMap::from_iter(vec![(
                b"key".to_vec(),
                vec![
                    new_available_key_revision(1, 1, 1, 3),
                    new_available_key_revision(1, 2, 2, 2),
                    new_available_key_revision(1, 3, 3, 1),
                ]
            ),])
        );

        index
    }

    #[test]
    fn test_get() {
        let index = init_and_test_insert();
        assert_eq!(index.get(b"key", b"", 0), vec![Revision::new(3, 1)]);
        assert_eq!(index.get(b"key", b"", 1), vec![Revision::new(1, 3)]);
        assert_eq!(
            index.get_from_rev(b"key", b"", 2),
            vec![Revision::new(2, 2), Revision::new(3, 1)]
        );
    }

    #[test]
    fn test_delete() {
        let index = init_and_test_insert();
        assert_eq!(
            index.delete(b"key", b"", 4, 0),
            vec![(Revision::new(3, 1), Revision::new(4, 0))]
        );
        assert_eq!(
            index.inner.lock().index,
            BTreeMap::from_iter(vec![(
                b"key".to_vec(),
                vec![
                    new_available_key_revision(1, 1, 1, 3),
                    new_available_key_revision(1, 2, 2, 2),
                    new_available_key_revision(1, 3, 3, 1),
                    KeyRevision::new_deletion(4, 0),
                ]
            ),])
        );
    }

    #[test]
    fn test_restore() {
        let index = Index::new();
        index.restore(b"key".to_vec(), 2, 0, 2, 1);
        index.restore(b"key".to_vec(), 3, 0, 2, 2);
        index.restore(b"foo".to_vec(), 4, 0, 4, 1);
        assert_eq!(
            index.inner.lock().index,
            BTreeMap::from_iter(vec![
                (
                    b"foo".to_vec(),
                    vec![new_available_key_revision(4, 1, 4, 0)]
                ),
                (
                    b"key".to_vec(),
                    vec![
                        new_available_key_revision(2, 1, 2, 0),
                        new_available_key_revision(2, 2, 3, 0)
                    ],
                ),
            ])
        );
    }
}
