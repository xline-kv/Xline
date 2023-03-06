use std::{
    cmp::Ordering,
    collections::HashMap,
    io::{self, Cursor, Read, Write},
    path::Path,
    sync::Arc,
};

use clippy_utilities::NumericCast;
use parking_lot::RwLock;

use crate::{
    engine_api::{Delete, DeleteRange, Put, SnapshotApi, StorageEngine, WriteOperation},
    error::EngineError,
};

/// A helper type to store the key-value pairs for the `MemoryEngine`
type MemoryTable = HashMap<Vec<u8>, Vec<u8>>;

/// Memory Storage Engine Implementation
#[derive(Debug, Default, Clone)]
pub struct MemoryEngine {
    /// The inner storage engine of `MemoryStorage`
    inner: Arc<RwLock<HashMap<String, MemoryTable>>>,
}

/// A snapshot of the `MemoryEngine`
#[derive(Debug, Default)]
pub struct MemorySnapshot {
    /// data of the snapshot
    data: Cursor<Vec<u8>>,
}

impl Read for MemorySnapshot {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }
}

impl Write for MemorySnapshot {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.data.flush()
    }
}

impl SnapshotApi for MemorySnapshot {
    #[inline]
    fn size(&self) -> u64 {
        self.data.get_ref().len().numeric_cast()
    }
}

impl MemoryEngine {
    /// New `MemoryEngine`
    ///
    /// # Errors
    ///
    /// Returns `EngineError` when DB create tables failed or open failed.
    #[inline]
    pub fn new(tables: &[&'static str]) -> Result<Self, EngineError> {
        let mut inner: HashMap<String, HashMap<Vec<u8>, Vec<u8>>> = HashMap::new();
        for table in tables {
            let _ignore = inner.entry((*table).to_owned()).or_insert(HashMap::new());
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }
}

impl StorageEngine for MemoryEngine {
    type Snapshot = MemorySnapshot;

    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        Ok(table.get(&key.as_ref().to_vec()).cloned())
    }

    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

        Ok(keys
            .iter()
            .map(|key| table.get(&key.as_ref().to_vec()).cloned())
            .collect())
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, _sync: bool) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        for op in wr_ops {
            match op {
                WriteOperation::Put(Put { table, key, value }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.insert(key, value);
                }
                WriteOperation::Delete(Delete { table, key }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.remove(key);
                }
                WriteOperation::DeleteRange(DeleteRange { table, from, to }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    table.retain(|key, _value| {
                        let key_slice = key.as_slice();
                        match key_slice.cmp(from) {
                            Ordering::Less => true,
                            Ordering::Equal => false,
                            Ordering::Greater => match key_slice.cmp(to) {
                                Ordering::Less => false,
                                Ordering::Equal | Ordering::Greater => true,
                            },
                        }
                    });
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn snapshot(
        &self,
        _path: impl AsRef<Path>,
        _tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError> {
        let inner_r = self.inner.read();
        let db = &*inner_r;
        let data = bincode::serialize(db).map_err(|e| {
            EngineError::UnderlyingError(format!("serialize memory engine failed: {e:?}"))
        })?;
        Ok(MemorySnapshot {
            data: Cursor::new(data),
        })
    }

    #[inline]
    fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        _tables: &[&'static str],
    ) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        let db = &mut *inner;
        let data = snapshot.data.into_inner();
        let new_db = bincode::deserialize(&data).map_err(|e| {
            EngineError::UnderlyingError(format!("deserialize memory engine failed: {e:?}"))
        })?;
        *db = new_db;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::iter::{repeat, zip};

    use super::*;
    use crate::engine_api::Put;

    const TESTTABLES: [&'static str; 3] = ["kv", "lease", "auth"];

    #[test]
    fn write_batch_into_a_non_existing_table_should_fail() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();

        let put = WriteOperation::Put(Put::new(
            "hello",
            "hello".as_bytes().to_vec(),
            "world".as_bytes().to_vec(),
        ));
        assert!(engine.write_batch(vec![put], false).is_err());

        let delete = WriteOperation::Delete(Delete::new("hello", b"hello"));
        assert!(engine.write_batch(vec![delete], false).is_err());

        let delete_range =
            WriteOperation::DeleteRange(DeleteRange::new("hello", b"hello", b"world"));
        assert!(engine.write_batch(vec![delete_range], false).is_err());
    }

    #[test]
    fn write_batch_should_success() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();
        let origin_set: Vec<Vec<u8>> = (1u8..=10u8)
            .map(|val| repeat(val).take(4).collect())
            .collect();
        let keys = origin_set.clone();
        let values = origin_set.clone();
        let puts = zip(keys, values)
            .map(|(k, v)| WriteOperation::Put(Put::new("kv", k, v)))
            .collect::<Vec<WriteOperation<'_>>>();

        assert!(engine.write_batch(puts, false).is_ok());

        let res_1 = engine.get_multi("kv", &origin_set).unwrap();
        assert_eq!(res_1.iter().filter(|v| v.is_some()).count(), 10);

        let delete_key: Vec<u8> = vec![1, 1, 1, 1];
        let delete = WriteOperation::Delete(Delete::new("kv", delete_key.as_slice()));

        let res_2 = engine.write_batch(vec![delete], false);
        assert!(res_2.is_ok());

        let res_3 = engine.get("kv", &delete_key).unwrap();
        assert!(res_3.is_none());

        let delete_start: Vec<u8> = vec![2, 2, 2, 2];
        let delete_end: Vec<u8> = vec![5, 5, 5, 5];
        let delete_range = WriteOperation::DeleteRange(DeleteRange::new(
            "kv",
            delete_start.as_slice(),
            &delete_end.as_slice(),
        ));
        let res_4 = engine.write_batch(vec![delete_range], false);
        assert!(res_4.is_ok());

        let get_key_1: Vec<u8> = vec![5, 5, 5, 5];
        let get_key_2: Vec<u8> = vec![3, 3, 3, 3];
        assert!(engine.get("kv", &get_key_1).unwrap().is_some());
        assert!(engine.get("kv", &get_key_2).unwrap().is_none());
    }

    #[test]
    fn snapshot_should_work() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();
        let put = WriteOperation::Put(Put::new("kv", "key".into(), "value".into()));
        assert!(engine.write_batch(vec![put], false).is_ok());

        let snapshot = engine.snapshot("", &TESTTABLES).unwrap();
        let engine_2 = MemoryEngine::new(&TESTTABLES).unwrap();
        assert!(engine_2.apply_snapshot(snapshot, &TESTTABLES).is_ok());

        let value = engine_2.get("kv", "key").unwrap();
        assert_eq!(value, Some("value".into()));
    }
}
