use std::{cmp::Ordering, collections::HashMap};

use parking_lot::RwLock;

use crate::{
    engine_api::{Delete, DeleteRange, Put, StorageEngine, WriteOperation},
    error::EngineError,
};

/// A helper type to store the key-value pairs for the `MemoryEngine`
type TableStore = HashMap<Vec<u8>, Vec<u8>>;

/// Memory Storage Engine Implementation
#[derive(Debug, Default)]
pub struct MemoryEngine {
    /// The inner storage engine of `MemoryStorage`
    inner: RwLock<HashMap<String, TableStore>>,
}

impl MemoryEngine {
    /// New `MemoryEngine`
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        let inner: HashMap<String, HashMap<Vec<u8>, Vec<u8>>> = HashMap::new();
        Self {
            inner: RwLock::new(inner),
        }
    }
}

impl StorageEngine for MemoryEngine {
    type Key = Vec<u8>;

    #[inline]
    fn create_table(&self, table: &str) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        let _ = inner.entry(table.to_owned()).or_insert(HashMap::new());
        Ok(())
    }

    #[inline]
    fn get(&self, table: &str, key: &Self::Key) -> Result<Option<Vec<u8>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

        Ok(table.get(key).cloned())
    }

    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[Self::Key],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

        Ok(keys.iter().map(|key| table.get(key).cloned()).collect())
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        for op in wr_ops {
            match op {
                WriteOperation::Put(Put {
                    table, key, value, ..
                }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.insert(key, value);
                }
                WriteOperation::Delete(Delete { table, key, .. }) => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.remove(key);
                }
                WriteOperation::DeleteRange(DeleteRange {
                    table, from, to, ..
                }) => {
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
}

#[cfg(test)]
mod test {
    use std::iter::{repeat, zip};

    use super::*;
    use crate::engine_api::Put;

    fn get_data_from_non_existing_table_should_fail() {
        let engine = MemoryEngine::new();
        let single_key = "hello".as_bytes().to_vec();
        let multi_keys = ["hello", "world", "superman"]
            .map(|key| key.as_bytes().to_vec())
            .to_vec();
        let res_1 = engine.get("kv", &single_key);
        assert!(res_1.is_err());
        let res_2 = engine.get_multi("kv", &multi_keys);
        assert!(res_2.is_err());

        let res_3 = engine.create_table("kv");
        assert!(res_3.is_ok());

        let res_4 = engine.get("kv", &single_key);
        assert!(res_4.is_ok());
        let res_5 = engine.get_multi("kv", &multi_keys);
        assert!(res_5.is_ok());
    }

    fn write_batch_into_a_non_existing_table_should_fail() {
        let engine = MemoryEngine::new();

        let put = WriteOperation::Put(Put::new(
            "hello",
            "hello".as_bytes().to_vec(),
            "world".as_bytes().to_vec(),
            false,
        ));
        assert!(engine.write_batch(vec![put]).is_err());

        let delete = WriteOperation::Delete(Delete::new("hello", b"hello", false));
        assert!(engine.write_batch(vec![delete]).is_err());

        let delete_range =
            WriteOperation::DeleteRange(DeleteRange::new("hello", b"hello", b"world", false));
        assert!(engine.write_batch(vec![delete_range]).is_err());
    }

    fn write_batch_should_success() {
        let engine = MemoryEngine::new();
        engine.create_table("table").unwrap();
        let origin_set: Vec<Vec<u8>> = (1u8..=10u8)
            .map(|val| repeat(val).take(4).collect())
            .collect();
        let keys = origin_set.clone();
        let values = origin_set.clone();
        let puts = zip(keys, values)
            .map(|(k, v)| WriteOperation::Put(Put::new("table", k, v, false)))
            .collect::<Vec<WriteOperation<'_>>>();

        assert!(engine.write_batch(puts).is_ok());

        let res_1 = engine.get_multi("table", &origin_set).unwrap();
        assert_eq!(res_1.iter().filter(|v| v.is_some()).count(), 10);

        let delete_key: Vec<u8> = vec![1, 1, 1, 1];
        let delete = WriteOperation::Delete(Delete::new("table", delete_key.as_slice(), false));

        let res_2 = engine.write_batch(vec![delete]);
        assert!(res_2.is_ok());

        let res_3 = engine.get("table", &delete_key).unwrap();
        assert!(res_3.is_none());

        let delete_start: Vec<u8> = vec![2, 2, 2, 2];
        let delete_end: Vec<u8> = vec![5, 5, 5, 5];
        let delete_range = WriteOperation::DeleteRange(DeleteRange::new(
            "table",
            delete_start.as_slice(),
            &delete_end.as_slice(),
            false,
        ));
        let res_4 = engine.write_batch(vec![delete_range]);
        assert!(res_4.is_ok());

        let get_key_1: Vec<u8> = vec![5, 5, 5, 5];
        let get_key_2: Vec<u8> = vec![3, 3, 3, 3];
        assert!(engine.get("table", &get_key_1).unwrap().is_some());
        assert!(engine.get("table", &get_key_2).unwrap().is_none());
    }
}
