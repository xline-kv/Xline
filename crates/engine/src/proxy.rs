use std::path::{Path, PathBuf};

use bytes::{Bytes, BytesMut};

#[cfg(madsim)]
use crate::mock_rocksdb_engine::{RocksEngine, RocksSnapshot};
#[cfg(not(madsim))]
use crate::rocksdb_engine::{RocksEngine, RocksSnapshot};
use crate::{
    error::EngineError,
    memory_engine::{MemoryEngine, MemorySnapshot},
    metrics, SnapshotApi, StorageEngine, WriteOperation,
};

#[derive(Debug)]
#[non_exhaustive]
/// Engine Type, used to create a new `Engine` or `Snapshot`
pub enum EngineType {
    /// Memory engine
    Memory,
    /// Rocks engine, the inner path is path of `Engine` or `Snapshot`
    Rocks(PathBuf),
}

/// `Engine` is designed to mask the different type of `MemoryEngine` and `RocksEngine`
/// and provides an uniform type to the upper layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Engine {
    /// Memory engine
    Memory(MemoryEngine),
    /// Rocks engine
    Rocks(metrics::Layer<RocksEngine>),
}

impl Engine {
    /// Create a new `Engine` instance
    /// # Errors
    /// Return `EngineError` when DB open failed.
    #[inline]
    pub fn new(engine_type: EngineType, tables: &[&'static str]) -> Result<Self, EngineError> {
        match engine_type {
            EngineType::Memory => Ok(Engine::Memory(MemoryEngine::new(tables))),
            EngineType::Rocks(path) => Ok(Engine::Rocks(metrics::Layer::new(RocksEngine::new(
                path, tables,
            )?))),
        }
    }

    /// Apply snapshot from file, only works for `RocksEngine`
    /// # Errors
    /// Return `EngineError` when `RocksDB` returns an error.
    #[inline]
    pub async fn apply_snapshot_from_file(
        &self,
        snapshot_path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<(), EngineError> {
        match *self {
            Engine::Rocks(ref e) => e.apply_snapshot_from_file(snapshot_path, tables).await,
            Engine::Memory(ref _e) => {
                unreachable!("Memory engine does not support apply snapshot from file")
            }
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for Engine {
    type Snapshot = Snapshot;

    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        match *self {
            Engine::Memory(ref e) => e.get(table, key),
            Engine::Rocks(ref e) => e.get(table, key),
        }
    }

    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        match *self {
            Engine::Memory(ref e) => e.get_multi(table, keys),
            Engine::Rocks(ref e) => e.get_multi(table, keys),
        }
    }

    #[inline]
    fn get_all(&self, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        match *self {
            Engine::Memory(ref e) => e.get_all(table),
            Engine::Rocks(ref e) => e.get_all(table),
        }
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError> {
        match *self {
            Engine::Memory(ref e) => e.write_batch(wr_ops, sync),
            Engine::Rocks(ref e) => e.write_batch(wr_ops, sync),
        }
    }

    #[inline]
    fn get_snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError> {
        match *self {
            Engine::Memory(ref e) => e.get_snapshot(path, tables).map(Snapshot::Memory),
            Engine::Rocks(ref e) => e.get_snapshot(path, tables).map(Snapshot::Rocks),
        }
    }

    #[inline]
    async fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        tables: &[&'static str],
    ) -> Result<(), EngineError> {
        match *self {
            Engine::Memory(ref e) => match snapshot {
                Snapshot::Memory(s) => e.apply_snapshot(s, tables).await,
                Snapshot::Rocks(_) => Err(EngineError::InvalidSnapshot),
            },
            Engine::Rocks(ref e) => match snapshot {
                Snapshot::Memory(_) => Err(EngineError::InvalidSnapshot),
                Snapshot::Rocks(s) => e.apply_snapshot(s, tables).await,
            },
        }
    }

    #[inline]
    fn estimated_file_size(&self) -> u64 {
        match *self {
            Engine::Memory(ref e) => e.estimated_file_size(),
            Engine::Rocks(ref e) => e.estimated_file_size(),
        }
    }

    #[inline]
    fn file_size(&self) -> Result<u64, EngineError> {
        match *self {
            Engine::Memory(ref e) => e.file_size(),
            Engine::Rocks(ref e) => e.file_size(),
        }
    }
}

/// `Snapshot` is designed to mask the different type of `MemorySnapshot` and `RocksSnapshot`
/// and provides an uniform type to the upper layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Snapshot {
    /// Memory snapshot
    Memory(MemorySnapshot),
    /// Rocks snapshot
    Rocks(metrics::Layer<RocksSnapshot>),
}

impl Snapshot {
    /// Create a new `Snapshot` instance
    /// # Errors
    /// Return `EngineError` when DB open failed.
    #[inline]
    pub fn new_for_receiving(engine_type: EngineType) -> Result<Self, EngineError> {
        match engine_type {
            EngineType::Memory => Ok(Self::Memory(MemorySnapshot::new(Vec::new()))),
            EngineType::Rocks(path) => Ok(Self::Rocks(metrics::Layer::new(
                RocksSnapshot::new_for_receiving(path)?,
            ))),
        }
    }
}

#[async_trait::async_trait]
impl SnapshotApi for Snapshot {
    #[inline]
    fn size(&self) -> u64 {
        match *self {
            Snapshot::Memory(ref s) => s.size(),
            Snapshot::Rocks(ref s) => s.size(),
        }
    }

    #[inline]
    fn rewind(&mut self) -> std::io::Result<()> {
        match *self {
            Snapshot::Memory(ref mut s) => s.rewind(),
            Snapshot::Rocks(ref mut s) => s.rewind(),
        }
    }

    #[inline]
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<()> {
        match *self {
            Snapshot::Memory(ref mut s) => s.read_buf(buf).await,
            Snapshot::Rocks(ref mut s) => s.read_buf(buf).await,
        }
    }

    #[inline]
    async fn read_buf_exact(&mut self, buf: &mut BytesMut) -> std::io::Result<()> {
        match *self {
            Snapshot::Memory(ref mut s) => s.read_buf_exact(buf).await,
            Snapshot::Rocks(ref mut s) => s.read_buf_exact(buf).await,
        }
    }

    #[inline]
    async fn write_all(&mut self, buf: Bytes) -> std::io::Result<()> {
        match *self {
            Snapshot::Memory(ref mut s) => s.write_all(buf).await,
            Snapshot::Rocks(ref mut s) => s.write_all(buf).await,
        }
    }

    #[inline]
    async fn clean(&mut self) -> std::io::Result<()> {
        match *self {
            Snapshot::Memory(ref mut s) => s.clean().await,
            Snapshot::Rocks(ref mut s) => s.clean().await,
        }
    }
}

#[cfg(test)]
mod test {
    use std::iter::{repeat, zip};

    use clippy_utilities::NumericCast;
    use test_macros::abort_on_panic;

    use super::*;
    use crate::api::snapshot_api::SnapshotApi;

    const TESTTABLES: [&'static str; 3] = ["kv", "lease", "auth"];

    #[test]
    fn write_batch_into_a_non_existing_table_should_fail() {
        let dir = PathBuf::from("/tmp/write_batch_into_a_non_existing_table_should_fail");
        let rocks_engine_path = dir.join("rocks_engine");
        let engines = vec![
            Engine::new(EngineType::Memory, &TESTTABLES).unwrap(),
            Engine::new(EngineType::Rocks(rocks_engine_path), &TESTTABLES).unwrap(),
        ];

        for engine in engines {
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
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn write_batch_should_success() {
        let dir = PathBuf::from("/tmp/write_batch_should_success");
        let rocks_engine_path = dir.join("rocks_engine");
        let engines = vec![
            Engine::new(EngineType::Memory, &TESTTABLES).unwrap(),
            Engine::new(EngineType::Rocks(rocks_engine_path), &TESTTABLES).unwrap(),
        ];
        for engine in engines {
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
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn get_operation_should_success() {
        let dir = PathBuf::from("/tmp/get_operation_should_success");
        let rocks_engine_path = dir.join("rocks_engine");
        let engines = vec![
            Engine::new(EngineType::Memory, &TESTTABLES).unwrap(),
            Engine::new(EngineType::Rocks(rocks_engine_path), &TESTTABLES).unwrap(),
        ];
        for engine in engines {
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
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    #[abort_on_panic]
    async fn snapshot_should_work() {
        let dir = PathBuf::from("/tmp/snapshot_should_work");
        let origin_data_dir = dir.join("origin");
        let recover_data_dir = dir.join("recover");
        let snapshot_dir = dir.join("snapshot");
        let snapshot_bak_dir = dir.join("snapshot_bak");
        let engines = vec![
            Engine::new(EngineType::Memory, &TESTTABLES).unwrap(),
            Engine::new(EngineType::Rocks(origin_data_dir), &TESTTABLES).unwrap(),
        ];
        let recover_engines = vec![
            Engine::new(EngineType::Memory, &TESTTABLES).unwrap(),
            Engine::new(EngineType::Rocks(recover_data_dir), &TESTTABLES).unwrap(),
        ];
        let received_snapshots = vec![
            Snapshot::Memory(MemorySnapshot::new(Vec::new())),
            Snapshot::Rocks(metrics::Layer::new(
                RocksSnapshot::new_for_receiving(snapshot_bak_dir).unwrap(),
            )),
        ];

        for ((engine, mut received_snapshot), recover_engine) in engines
            .into_iter()
            .zip(received_snapshots.into_iter())
            .zip(recover_engines)
        {
            let put_kv = WriteOperation::new_put("kv", "key".into(), "value".into());
            assert!(engine.write_batch(vec![put_kv], false).is_ok());

            let put_lease = WriteOperation::new_put("lease", "lease_id".into(), "lease".into());
            assert!(engine.write_batch(vec![put_lease], false).is_ok());

            let mut snapshot = engine.get_snapshot(&snapshot_dir, &TESTTABLES).unwrap();
            let put = WriteOperation::new_put("kv", "key2".into(), "value2".into());
            assert!(engine.write_batch(vec![put], false).is_ok());

            let mut buf = BytesMut::with_capacity(snapshot.size().numeric_cast());
            snapshot.read_buf_exact(&mut buf).await.unwrap();

            buf.extend([0u8; 100]); // add some padding, will be ignored when receiving

            received_snapshot.write_all(buf.freeze()).await.unwrap();

            assert!(recover_engine
                .apply_snapshot(received_snapshot, &TESTTABLES)
                .await
                .is_ok());

            let value = recover_engine.get("kv", "key").unwrap();
            assert_eq!(value, Some("value".into()));
            let value1 = recover_engine.get("lease", "lease_id").unwrap();
            assert_eq!(value1, Some("lease".into()));
            let value2 = recover_engine.get("kv", "key2").unwrap();
            assert!(value2.is_none());
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
