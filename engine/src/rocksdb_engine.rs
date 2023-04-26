use std::{
    fs,
    io::{Error as IoError, ErrorKind::Other},
    iter::repeat,
    path::Path,
    sync::Arc,
};

use rocksdb::{
    Error as RocksError, IteratorMode, Options, SstFileWriter, WriteBatchWithTransaction,
    WriteOptions, DB,
};

use crate::{
    engine_api::{StorageEngine, WriteOperation},
    error::EngineError,
    snapshot_api::{RocksSnapshot, SnapshotProxy},
};

/// Translate a `RocksError` into a `EngineError`
impl From<RocksError> for EngineError {
    #[inline]
    fn from(err: RocksError) -> Self {
        let err = err.into_string();
        if let Some((err_kind, err_msg)) = err.split_once(':') {
            match err_kind {
                "Corruption" => EngineError::Corruption(err_msg.to_owned()),
                "Invalid argument" => {
                    if let Some(table_name) = err_msg.strip_prefix(" Column family not found: ") {
                        EngineError::TableNotFound(table_name.to_owned())
                    } else {
                        EngineError::InvalidArgument(err_msg.to_owned())
                    }
                }
                "IO error" => EngineError::IoError(IoError::new(Other, err_msg)),
                _ => EngineError::UnderlyingError(err_msg.to_owned()),
            }
        } else {
            EngineError::UnderlyingError(err)
        }
    }
}

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
    pub fn new(data_dir: impl AsRef<Path>, tables: &[&'static str]) -> Result<Self, EngineError> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        Ok(Self {
            inner: Arc::new(DB::open_cf(&db_opts, data_dir, tables)?),
        })
    }
}

#[async_trait::async_trait]
impl StorageEngine for RocksEngine {
    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        if let Some(cf) = self.inner.cf_handle(table) {
            Ok(self.inner.get_cf(&cf, key)?)
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
                .map(|res| res.map_err(EngineError::from))
                .collect::<Result<Vec<_>, EngineError>>()
        } else {
            Err(EngineError::TableNotFound(table.to_owned()))
        }
    }

    #[inline]
    fn get_all(&self, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        if let Some(cf) = self.inner.cf_handle(table) {
            self.inner
                .iterator_cf(&cf, rocksdb::IteratorMode::Start)
                .map(|v| {
                    v.map(|(key, value)| (key.to_vec(), value.to_vec()))
                        .map_err(EngineError::from)
                })
                .collect()
        } else {
            Err(EngineError::TableNotFound(table.to_owned()))
        }
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError> {
        let mut batch = WriteBatchWithTransaction::<false>::default();

        for op in wr_ops {
            match op {
                WriteOperation::Put { table, key, value } => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.put_cf(&cf, key, value);
                }
                WriteOperation::Delete { table, key } => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.delete_cf(&cf, key);
                }
                WriteOperation::DeleteRange { table, from, to } => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.delete_range_cf(&cf, from, to);
                }
            }
        }
        let mut opt = WriteOptions::default();
        opt.set_sync(sync);
        self.inner.write_opt(batch, &opt).map_err(EngineError::from)
    }

    #[inline]
    fn get_snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<SnapshotProxy, EngineError> {
        if path.as_ref().exists() {
            fs::remove_dir_all(&path)?;
        }
        fs::create_dir_all(path.as_ref())?;

        let snap = self.inner.snapshot();
        let opts = Options::default();
        let mut sst_writer_option: Option<SstFileWriter<'_>> = None;
        for cf_name in tables {
            let Some(cf_handle) = self.inner.cf_handle(cf_name) else {
                return Err(EngineError::TableNotFound((*cf_name).to_owned()));
            };
            let iter = snap.iterator_cf(&cf_handle, IteratorMode::Start);
            for r in iter {
                let (key, value) = r?;
                if let Some(ref mut sst_writer) = sst_writer_option {
                    sst_writer.put(key, value)?;
                } else {
                    let mut sst_writer = SstFileWriter::create(&opts);
                    let file_name = format!("{cf_name}.sst");
                    sst_writer.open(path.as_ref().join(file_name))?;
                    sst_writer.put(key, value)?;
                    sst_writer_option = Some(sst_writer);
                }
            }
            if let Some(ref mut sst_writer) = sst_writer_option {
                sst_writer.finish()?;
                sst_writer_option = None;
            }
        }
        RocksSnapshot::new_for_sending(path.as_ref()).map(SnapshotProxy::Rocks)
    }

    #[inline]
    fn apply_snapshot(
        &self,
        snapshot: SnapshotProxy,
        tables: &[&'static str],
    ) -> Result<(), EngineError> {
        let SnapshotProxy::Rocks(snapshot) = snapshot else {
           return  Err(EngineError::UnderlyingError("snapshot type mismatch".to_owned()));
        };
        for cf_name in tables {
            let file_path = snapshot.sst_path(cf_name);
            if file_path.exists() {
                let Some(cf_handle) = self.inner.cf_handle(cf_name) else {
                    return Err(EngineError::TableNotFound((*cf_name).to_owned()));
                };
                self.inner
                    .ingest_external_file_cf(&cf_handle, vec![file_path])?;
            }
        }
        Ok(())
    }
}

/// destroy will remove the db file. It's test only
///
/// # Panics
///
/// Panic if db destroy failed.
#[cfg(test)]
pub fn destroy(data_dir: impl AsRef<Path>) {
    #[allow(clippy::unwrap_used)]
    DB::destroy(&Options::default(), data_dir).unwrap();
}

#[cfg(test)]
mod test {
    use std::{
        iter::{repeat, zip},
        path::PathBuf,
    };

    use clippy_utilities::NumericCast;

    use super::*;
    use crate::snapshot_api::SnapshotApi;
    const TESTTABLES: [&'static str; 3] = ["kv", "lease", "auth"];

    #[test]
    fn write_batch_into_a_non_existing_table_should_fail() {
        let data_dir = PathBuf::from("/tmp/write_batch_into_a_non_existing_table_should_fail");
        let engine = RocksEngine::new(&data_dir, &TESTTABLES).unwrap();

        let put = WriteOperation::new_put(
            "hello",
            "hello".as_bytes().to_vec(),
            "world".as_bytes().to_vec(),
        );
        assert!(engine.write_batch(vec![put], false).is_err());

        let delete = WriteOperation::new_delete("hello", b"hello");
        assert!(engine.write_batch(vec![delete], false).is_err());

        let delete_range = WriteOperation::new_delete_range("hello", b"hello", b"world");
        assert!(engine.write_batch(vec![delete_range], false).is_err());

        drop(engine);
        destroy(&data_dir);
    }

    #[test]
    fn write_batch_should_success() {
        let data_dir = PathBuf::from("/tmp/write_batch_should_success");
        let engine = RocksEngine::new(&data_dir, &TESTTABLES).unwrap();
        let origin_set: Vec<Vec<u8>> = (1u8..=10u8)
            .map(|val| repeat(val).take(4).collect())
            .collect();
        let keys = origin_set.clone();
        let values = origin_set.clone();
        let puts = zip(keys, values)
            .map(|(k, v)| WriteOperation::new_put("kv", k, v))
            .collect::<Vec<WriteOperation<'_>>>();

        assert!(engine.write_batch(puts, false).is_ok());

        let res_1 = engine.get_multi("kv", &origin_set).unwrap();
        assert_eq!(res_1.iter().filter(|v| v.is_some()).count(), 10);

        let delete_key: Vec<u8> = vec![1, 1, 1, 1];
        let delete = WriteOperation::new_delete("kv", &delete_key);

        let res_2 = engine.write_batch(vec![delete], false);
        assert!(res_2.is_ok());

        let res_3 = engine.get("kv", &delete_key).unwrap();
        assert!(res_3.is_none());

        let delete_start: Vec<u8> = vec![2, 2, 2, 2];
        let delete_end: Vec<u8> = vec![5, 5, 5, 5];
        let delete_range = WriteOperation::new_delete_range("kv", &delete_start, &delete_end);
        let res_4 = engine.write_batch(vec![delete_range], false);
        assert!(res_4.is_ok());

        let get_key_1: Vec<u8> = vec![5, 5, 5, 5];
        let get_key_2: Vec<u8> = vec![3, 3, 3, 3];
        assert!(engine.get("kv", &get_key_1).unwrap().is_some());
        assert!(engine.get("kv", &get_key_2).unwrap().is_none());
        drop(engine);
        destroy(&data_dir);
    }

    #[test]
    fn get_operation_should_success() {
        let data_dir = PathBuf::from("/tmp/get_operation_should_success");
        let engine = RocksEngine::new(&data_dir, &TESTTABLES).unwrap();
        let test_set = vec![("hello", "hello"), ("world", "world"), ("foo", "foo")];
        let batch = test_set.iter().map(|&(key, value)| {
            WriteOperation::new_put("kv", key.as_bytes().to_vec(), value.as_bytes().to_vec())
        });
        let res = engine.write_batch(batch.collect(), false);
        assert!(res.is_ok());

        let res_1 = engine.get("kv", "hello").unwrap();
        assert_eq!(res_1, Some("hello".as_bytes().to_vec()));
        let multi_keys = vec!["hello", "world", "bar"];
        let expected_multi_values = vec![
            Some("hello".as_bytes().to_vec()),
            Some("world".as_bytes().to_vec()),
            None,
        ];
        let res_2 = engine.get_multi("kv", &multi_keys).unwrap();
        assert_eq!(multi_keys.len(), res_2.len());
        assert_eq!(res_2, expected_multi_values);

        let mut res_3 = engine.get_all("kv").unwrap();
        let mut expected_all_values = test_set
            .into_iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect::<Vec<(Vec<u8>, Vec<u8>)>>();
        assert_eq!(res_3.sort(), expected_all_values.sort());
        drop(engine);
        destroy(&data_dir);
    }

    #[tokio::test]
    async fn snapshot_should_work() {
        let test_dir = PathBuf::from("/tmp/snapshot_should_work_rocksdb");
        let origin_data_dir = test_dir.join("origin");
        let recover_data_dir = test_dir.join("recover");
        let snapshot_dir = test_dir.join("snapshot");
        let snapshot_bak_dir = test_dir.join("snapshot_bak");

        let engine = RocksEngine::new(&origin_data_dir, &TESTTABLES).unwrap();
        let put = WriteOperation::new_put("kv", "key".into(), "value".into());
        assert!(engine.write_batch(vec![put], false).is_ok());

        let mut snapshot = engine.get_snapshot(&snapshot_dir, &TESTTABLES).unwrap();
        let put = WriteOperation::new_put("kv", "key2".into(), "value2".into());
        assert!(engine.write_batch(vec![put], false).is_ok());

        let mut buf = vec![0u8; snapshot.size().numeric_cast()];
        snapshot.read_exact(&mut buf).await.unwrap();

        buf.extend([0u8; 100]); // add some padding, will be ignored when receiving

        let mut received_snapshot =
            SnapshotProxy::Rocks(RocksSnapshot::new_for_receiving(snapshot_bak_dir).unwrap());
        received_snapshot.write_all(&buf).await.unwrap();

        let engine_2 = RocksEngine::new(&recover_data_dir, &TESTTABLES).unwrap();
        assert!(engine_2
            .apply_snapshot(received_snapshot, &TESTTABLES)
            .is_ok());

        let value = engine_2.get("kv", "key").unwrap();
        assert_eq!(value, Some("value".into()));
        let value2 = engine_2.get("kv", "key2").unwrap();
        assert!(value2.is_none());

        drop(engine);
        drop(engine_2);
        destroy(&origin_data_dir);
        destroy(&recover_data_dir);
        fs::remove_dir_all(&test_dir).unwrap();
    }
}
