use std::collections::HashSet;

use clippy_utilities::OverflowArithmetic;
use crossbeam_skiplist::SkipMap;
use itertools::Itertools;
use parking_lot::RwLock;
use utils::parking_lot_lock::RwLockMap;
use xlineapi::command::KeyRange;

use super::revision::{KeyRevision, Revision};
use crate::server::command::RangeType;

/// Keys to revisions mapping
#[derive(Debug)]
pub(crate) struct Index {
    /// Inner struct of `Index`
    inner: SkipMap<Vec<u8>, RwLock<Vec<KeyRevision>>>,
}

impl Index {
    /// New `Index`
    pub(crate) fn new() -> Self {
        Self {
            inner: SkipMap::new(),
        }
    }

    /// Filter out `KeyRevision` that is less than one revision and convert to `Revision`
    fn filter_revision(revs: &[KeyRevision], revision: i64) -> Vec<Revision> {
        revs.iter()
            .filter(|rev| rev.mod_revision >= revision)
            .map(KeyRevision::as_revision)
            .collect()
    }

    /// Get specified or last `KeyRevision` if the key is not deleted, and convert to `Revision`
    fn get_revision(revs: &[KeyRevision], revision: i64) -> Option<Revision> {
        let rev = if revision <= 0 {
            revs.last()
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
        let last_available_rev = Self::get_revision(revs, 0)?;
        let del_rev = KeyRevision::new_deletion(revision, sub_revision);
        revs.push(del_rev);
        Some((last_available_rev, del_rev.as_revision()))
    }

    /// Get all revisions that need to be kept after compact at the given revision
    pub(crate) fn keep(&self, at_rev: i64) -> HashSet<Revision> {
        let mut revs = HashSet::new();
        self.inner.iter().for_each(|entry| {
            entry.value().map_read(|revisions| {
                if let Some(revision) = revisions.first() {
                    if revision.mod_revision < at_rev {
                        let pivot = revisions.partition_point(|rev| rev.mod_revision <= at_rev);
                        let compacted_last_idx = pivot.overflow_sub(1);
                        let key_rev = revisions.get(compacted_last_idx).unwrap_or_else(|| {
                            unreachable!(
                                "Oops, the key revision at {compacted_last_idx} should not be None",
                            )
                        });
                        if !key_rev.is_deleted() {
                            _ = revs.insert(key_rev.as_revision());
                        }
                    }
                }
            });
        });
        revs
    }
}

/// Operations of Index
pub(super) trait IndexOperate {
    /// Get `Revision` of keys, get the latest `Revision` when revision <= 0
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision>;

    /// Get `Revision` of keys from one revision
    fn get_from_rev(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision>;

    /// Mark keys as deleted and return latest revision before deletion and deletion revision
    /// return all revision pairs and all keys in range
    fn delete(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>);

    /// Insert or update `KeyRevision`
    fn insert(&self, key_revisions: Vec<(Vec<u8>, KeyRevision)>);

    /// Register a new `KeyRevision` of the given key
    fn register_revision(&self, key: &[u8], revision: i64, sub_revision: i64) -> KeyRevision;

    /// Restore `KeyRevision` of a key
    fn restore(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    );

    /// Compact a `KeyRevision` by removing the versions with smaller or equal
    /// revision than the given atRev except the largest one (If the largest one is
    /// a tombstone, it will not be kept).
    fn compact(&self, at_rev: i64) -> Vec<KeyRevision>;
}

impl IndexOperate for Index {
    fn get(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => self
                .inner
                .get(key)
                .and_then(|entry| {
                    entry
                        .value()
                        .map_read(|revs| Self::get_revision(revs.as_ref(), revision))
                })
                .map(|rev| vec![rev])
                .unwrap_or_default(),
            RangeType::AllKeys => self
                .inner
                .iter()
                .filter_map(|entry| {
                    entry
                        .value()
                        .map_read(|revs| Self::get_revision(revs.as_ref(), revision))
                })
                .collect(),
            RangeType::Range => self
                .inner
                .range(KeyRange::new(key, range_end))
                .filter_map(|entry| {
                    entry
                        .value()
                        .map_read(|revs| Self::get_revision(revs.as_ref(), revision))
                })
                .collect(),
        }
    }

    fn get_from_rev(&self, key: &[u8], range_end: &[u8], revision: i64) -> Vec<Revision> {
        match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => self
                .inner
                .get(key)
                .map(|entry| {
                    entry
                        .value()
                        .map_read(|revs| Self::filter_revision(revs.as_ref(), revision))
                })
                .unwrap_or_default(),
            RangeType::AllKeys => self
                .inner
                .iter()
                .flat_map(|entry| {
                    entry
                        .value()
                        .map_read(|revs| Self::filter_revision(revs.as_ref(), revision))
                })
                .sorted()
                .collect(),
            RangeType::Range => self
                .inner
                .range(KeyRange::new(key, range_end))
                .flat_map(|entry| {
                    entry
                        .value()
                        .map_read(|revs| Self::filter_revision(revs.as_ref(), revision))
                })
                .sorted()
                .collect(),
        }
    }

    fn delete(
        &self,
        key: &[u8],
        range_end: &[u8],
        revision: i64,
        sub_revision: i64,
    ) -> (Vec<(Revision, Revision)>, Vec<Vec<u8>>) {
        let (pairs, keys) = match RangeType::get_range_type(key, range_end) {
            RangeType::OneKey => {
                let pairs: Vec<(Revision, Revision)> = self
                    .inner
                    .get(key)
                    .into_iter()
                    .filter_map(|entry| {
                        entry.value().map_write(|mut revs| {
                            Self::gen_del_revision(revs.as_mut(), revision, sub_revision)
                        })
                    })
                    .collect();
                let keys = if pairs.is_empty() {
                    vec![]
                } else {
                    vec![key.to_vec()]
                };
                (pairs, keys)
            }
            RangeType::AllKeys => self
                .inner
                .iter()
                .zip(0..)
                .filter_map(|(entry, i)| {
                    entry.value().map_write(|mut revs| {
                        Self::gen_del_revision(
                            revs.as_mut(),
                            revision,
                            sub_revision.overflow_add(i),
                        )
                        .map(|pair| (pair, entry.key().clone()))
                    })
                })
                .unzip(),
            RangeType::Range => self
                .inner
                .range(KeyRange::new(key, range_end))
                .zip(0..)
                .filter_map(|(entry, i)| {
                    entry.value().map_write(|mut revs| {
                        Self::gen_del_revision(
                            revs.as_mut(),
                            revision,
                            sub_revision.overflow_add(i),
                        )
                        .map(|pair| (pair, entry.key().clone()))
                    })
                })
                .unzip(),
        };
        (pairs, keys)
    }

    fn insert(&self, key_revisions: Vec<(Vec<u8>, KeyRevision)>) {
        for (key, revision) in key_revisions {
            if let Some(entry) = self.inner.get::<[u8]>(key.as_ref()) {
                entry.value().map_write(|mut revs| revs.push(revision));
            } else {
                _ = self.inner.insert(key, RwLock::new(vec![revision]));
            }
        }
    }

    fn register_revision(&self, key: &[u8], revision: i64, sub_revision: i64) -> KeyRevision {
        if let Some(entry) = self.inner.get(key) {
            entry.value().map_read(|revisions| {
                if let Some(rev) = revisions.last() {
                    if rev.is_deleted() {
                        KeyRevision::new(revision, 1, revision, sub_revision)
                    } else {
                        KeyRevision::new(
                            rev.create_revision,
                            rev.version.overflow_add(1),
                            revision,
                            sub_revision,
                        )
                    }
                } else {
                    panic!("Get empty revision list for key {key:?}");
                }
            })
        } else {
            KeyRevision::new(revision, 1, revision, sub_revision)
        }
    }

    fn restore(
        &self,
        key: Vec<u8>,
        revision: i64,
        sub_revision: i64,
        create_revision: i64,
        version: i64,
    ) {
        self.inner
            .get_or_insert(key, RwLock::new(Vec::new()))
            .value()
            .map_write(|mut revisions| {
                revisions.push(KeyRevision::new(
                    create_revision,
                    version,
                    revision,
                    sub_revision,
                ));
            });
    }

    fn compact(&self, at_rev: i64) -> Vec<KeyRevision> {
        let mut revs = Vec::new();
        let mut del_keys = Vec::new();

        self.inner.iter().for_each(|entry| {
            entry.value().map_write(|mut revisions| {
                if let Some(revision) = revisions.first() {
                    if revision.mod_revision < at_rev {
                        let pivot = revisions.partition_point(|rev| rev.mod_revision <= at_rev);
                        let compacted_last_idx = pivot.overflow_sub(1);
                        // There is at least 1 element in the first partition, so the key revision at `compacted_last_idx`
                        // must exist.
                        let key_rev = revisions.get(compacted_last_idx).unwrap_or_else(|| {
                            unreachable!(
                                "Oops, the key revision at {compacted_last_idx} should not be None",
                            )
                        });
                        let compact_revs = if key_rev.is_deleted() {
                            revisions.drain(..=compacted_last_idx)
                        } else {
                            revisions.drain(..compacted_last_idx)
                        };
                        revs.extend(compact_revs.into_iter());

                        if revisions.is_empty() {
                            del_keys.push(entry.key().clone());
                        }
                    }
                }
            });
        });
        for key in del_keys {
            let _ignore = self.inner.remove(&key);
        }
        revs
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[allow(clippy::expect_used)]
    fn match_values(index: &Index, key: impl AsRef<[u8]>, expected_values: &[KeyRevision]) {
        index
            .inner
            .get(key.as_ref())
            .expect("index entry should not be None")
            .value()
            .map_read(|revs| assert_eq!(*revs, expected_values));
    }

    fn init_and_test_insert() -> Index {
        let index = Index::new();

        index.insert(vec![
            (b"key".to_vec(), index.register_revision(b"key", 1, 3)),
            (b"foo".to_vec(), index.register_revision(b"foo", 4, 5)),
            (b"bar".to_vec(), index.register_revision(b"bar", 5, 4)),
        ]);

        index.insert(vec![
            (b"key".to_vec(), index.register_revision(b"key", 2, 2)),
            (b"foo".to_vec(), index.register_revision(b"foo", 6, 6)),
            (b"bar".to_vec(), index.register_revision(b"bar", 7, 7)),
        ]);
        index.insert(vec![
            (b"key".to_vec(), index.register_revision(b"key", 3, 1)),
            (b"foo".to_vec(), index.register_revision(b"foo", 8, 8)),
            (b"bar".to_vec(), index.register_revision(b"bar", 9, 9)),
        ]);

        match_values(
            &index,
            b"key",
            &[
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
                KeyRevision::new(1, 3, 3, 1),
            ],
        );

        match_values(
            &index,
            b"foo",
            &[
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(4, 2, 6, 6),
                KeyRevision::new(4, 3, 8, 8),
            ],
        );

        match_values(
            &index,
            b"bar",
            &[
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(5, 2, 7, 7),
                KeyRevision::new(5, 3, 9, 9),
            ],
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
        assert_eq!(
            index.get_from_rev(b"a", b"g", 3),
            vec![
                Revision::new(4, 5),
                Revision::new(5, 4),
                Revision::new(6, 6),
                Revision::new(7, 7),
                Revision::new(8, 8),
                Revision::new(9, 9)
            ]
        );
        assert_eq!(
            index.get_from_rev(b"\0", b"\0", 3),
            vec![
                Revision::new(3, 1),
                Revision::new(4, 5),
                Revision::new(5, 4),
                Revision::new(6, 6),
                Revision::new(7, 7),
                Revision::new(8, 8),
                Revision::new(9, 9)
            ]
        );
    }

    #[test]
    fn test_delete() {
        let index = init_and_test_insert();

        assert_eq!(
            index.delete(b"key", b"", 10, 0),
            (
                vec![(Revision::new(3, 1), Revision::new(10, 0))],
                vec![b"key".to_vec()]
            )
        );

        assert_eq!(
            index.delete(b"a", b"g", 11, 0),
            (
                vec![
                    (Revision::new(9, 9), Revision::new(11, 0)),
                    (Revision::new(8, 8), Revision::new(11, 1)),
                ],
                vec![b"bar".to_vec(), b"foo".to_vec()]
            )
        );

        assert_eq!(index.delete(b"\0", b"\0", 12, 0), (vec![], vec![]));
        match_values(
            &index,
            b"key",
            &[
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
                KeyRevision::new(1, 3, 3, 1),
                KeyRevision::new_deletion(10, 0),
            ],
        );

        match_values(
            &index,
            b"foo",
            &[
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(4, 2, 6, 6),
                KeyRevision::new(4, 3, 8, 8),
                KeyRevision::new_deletion(11, 1),
            ],
        );

        match_values(
            &index,
            b"bar",
            &[
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(5, 2, 7, 7),
                KeyRevision::new(5, 3, 9, 9),
                KeyRevision::new_deletion(11, 0),
            ],
        );
    }

    #[test]
    fn test_restore() {
        let index = Index::new();
        index.restore(b"key".to_vec(), 2, 0, 2, 1);
        index.restore(b"key".to_vec(), 3, 0, 2, 2);
        index.restore(b"foo".to_vec(), 4, 0, 4, 1);
        match_values(
            &index,
            b"key",
            &[KeyRevision::new(2, 1, 2, 0), KeyRevision::new(2, 2, 3, 0)],
        );
    }

    #[test]
    fn test_compact() {
        let index = init_and_test_insert();
        let res = index.compact(7);
        match_values(&index, b"key", &[KeyRevision::new(1, 3, 3, 1)]);

        match_values(
            &index,
            b"foo",
            &[KeyRevision::new(4, 2, 6, 6), KeyRevision::new(4, 3, 8, 8)],
        );

        match_values(
            &index,
            b"bar",
            &[KeyRevision::new(5, 2, 7, 7), KeyRevision::new(5, 3, 9, 9)],
        );
        assert_eq!(
            res,
            vec![
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
            ]
        );
    }

    #[test]
    fn test_compact_with_deletion() {
        let index = init_and_test_insert();
        index.delete(b"a", b"g", 10, 0);
        index.insert(vec![(
            b"bar".to_vec(),
            index.register_revision(b"bar", 11, 0),
        )]);

        let res = index.compact(10);

        match_values(&index, b"key", &[KeyRevision::new(1, 3, 3, 1)]);

        match_values(&index, b"bar", &[KeyRevision::new(11, 1, 11, 0)]);

        assert_eq!(
            res,
            vec![
                KeyRevision::new(5, 1, 5, 4),
                KeyRevision::new(5, 2, 7, 7),
                KeyRevision::new(5, 3, 9, 9),
                KeyRevision::new(0, 0, 10, 0),
                KeyRevision::new(4, 1, 4, 5),
                KeyRevision::new(4, 2, 6, 6),
                KeyRevision::new(4, 3, 8, 8),
                KeyRevision::new(0, 0, 10, 1),
                KeyRevision::new(1, 1, 1, 3),
                KeyRevision::new(1, 2, 2, 2),
            ]
        );
    }
}
