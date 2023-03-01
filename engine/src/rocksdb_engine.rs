use std::{iter::repeat, sync::Arc};

use rocksdb::{Options, WriteBatchWithTransaction, DB};

use crate::{
    engine_api::{Delete, DeleteRange, Put, StorageEngine, WriteOperation},
    error::EngineError,
};

/// `RocksDB` Storage Engine
#[derive(Debug, Clone)]
pub struct RocksEngine {
    /// The inner storage engine of `RocksDB`
    inner: Arc<DB>,
}

impl RocksEngine {
    /// New `RocksEngine`
    ///
    /// # Errors
    ///
    /// Return `EngineError` when DB open failed.
    #[inline]
    pub fn new(path: &str, tables: &[&'static str]) -> Result<Self, EngineError> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        Ok(Self {
            inner: Arc::new(
                DB::open_cf(&db_opts, path, tables).map_err(|e| {
                    EngineError::UnderlyingError(format!("cannot open database: {e}"))
                })?,
            ),
        })
    }
}

impl StorageEngine for RocksEngine {
    #[inline]
    fn create_table(&self, table: &str) -> Result<(), EngineError> {
        let mut opts = Options::default();
        opts.create_missing_column_families(true);
        self.inner
            .create_cf(table, &opts)
            .map_err(|e| EngineError::UnderlyingError(format!("create table failed: {e}")))
    }

    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        if let Some(cf) = self.inner.cf_handle(table) {
            Ok(self
                .inner
                .get_cf(&cf, key)
                .map_err(|e| EngineError::IoError(format!("get key from {table} failed: {e}")))?)
        } else {
            Err(EngineError::TableNotFound(table.to_owned()))
        }
    }

    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        if let Some(cf) = self.inner.cf_handle(table) {
            self.inner
                .multi_get_cf(repeat(&cf).zip(keys.iter()))
                .into_iter()
                .map(|res| {
                    res.map_err(|err| {
                        EngineError::IoError(format!("get key from {table} failed: {err}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        } else {
            Err(EngineError::TableNotFound(table.to_owned()))
        }
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>) -> Result<(), EngineError> {
        let mut batch = WriteBatchWithTransaction::<false>::default();

        for op in wr_ops {
            match op {
                WriteOperation::Put(Put {
                    table, key, value, ..
                }) => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.put_cf(&cf, key, value);
                }
                WriteOperation::Delete(Delete { table, key, .. }) => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.delete_cf(&cf, key);
                }
                WriteOperation::DeleteRange(DeleteRange {
                    table, from, to, ..
                }) => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.delete_range_cf(&cf, from, to);
                }
            }
        }
        self.inner
            .write(batch)
            .map_err(|e| EngineError::UnderlyingError(format!("{e}")))
    }
}

/// destroy will remove the db file. It's test only
///
/// # Panics
///
/// Panic if db destroy failed.
#[cfg(test)]
pub fn destroy(path: &str) {
    #[allow(clippy::unwrap_used)]
    DB::destroy(&Options::default(), path).unwrap();
}

#[cfg(test)]
mod test {
    use std::iter::{repeat, zip};

    use super::*;
    const TESTTABLES: [&'static str; 3] = ["kv", "lease", "auth"];

    #[test]
    fn write_batch_into_a_non_existing_table_should_fail() {
        let path = "/tmp/write_batch_into_a_non_existing_table_should_fail";
        let engine = RocksEngine::new(path, &TESTTABLES).unwrap();

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

        drop(engine);
        destroy(path);
    }

    #[test]
    fn write_batch_should_success() {
        let path = "/tmp/write_batch_should_success";
        let engine = RocksEngine::new(path, &TESTTABLES).unwrap();
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
        drop(engine);
        destroy(path);
    }
}
