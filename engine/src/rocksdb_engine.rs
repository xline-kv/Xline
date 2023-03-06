use std::{
    cmp::Ordering,
    fs::{self, File},
    io::{self, Cursor, Read, Write},
    iter::repeat,
    path::{Path, PathBuf},
    sync::Arc,
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use rocksdb::{IteratorMode, Options, SstFileWriter, WriteBatchWithTransaction, WriteOptions, DB};
use serde::{Deserialize, Serialize};

use crate::{
    engine_api::{Delete, DeleteRange, Put, SnapshotApi, StorageEngine, WriteOperation},
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
    pub fn new(data_dir: &PathBuf, tables: &[&'static str]) -> Result<Self, EngineError> {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        Ok(Self {
            inner: Arc::new(
                DB::open_cf(&db_opts, data_dir, tables).map_err(|e| {
                    EngineError::UnderlyingError(format!("cannot open database: {e}"))
                })?,
            ),
        })
    }
}

/// meta of the snapshot
#[derive(Debug, Default, Serialize, Deserialize)]
struct SnapMeta {
    /// filenames and sizes of the snapshot
    files: Vec<(String, u64)>,
}

/// File info for snapshot
#[derive(Debug, Default)]
struct SnapFile {
    /// filename of the file
    filename: String,
    /// size of the file has been written
    written_size: u64,
    /// size of the file
    size: u64,
}

/// Snapshot type for `RocksDB`
#[derive(Debug, Default)]
pub struct RocksSnapshot {
    /// Snapshot meta
    meta: Option<Cursor<Vec<u8>>>,
    /// directory of the snapshot
    dir: PathBuf,
    /// files of the snapshot
    snap_files: Vec<SnapFile>,
    /// current file index
    file_index: usize,
    /// current file
    current_file: Option<File>,
}

impl RocksSnapshot {
    /// New empty `RocksSnapshot`
    fn new<P>(dir: P) -> Result<Self, EngineError>
    where
        P: Into<PathBuf>,
    {
        let dir = dir.into();
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
        }
        Ok(RocksSnapshot {
            meta: None,
            dir,
            snap_files: Vec::new(),
            file_index: 0,
            current_file: None,
        })
    }

    /// Create a new snapshot for sending
    fn new_for_sending<P>(dir: P) -> Result<RocksSnapshot, EngineError>
    where
        P: Into<PathBuf>,
    {
        let dir = dir.into();
        let files = fs::read_dir(&dir)?;
        let mut s = Self::new(dir)?;
        for file in files {
            let entry = file?;
            let filename = entry.file_name().into_string().map_err(|_e| {
                EngineError::IoError("cannot convert filename to string".to_owned())
            })?;
            let size = entry.metadata()?.len();
            s.snap_files.push(SnapFile {
                filename: filename.clone(),
                written_size: 0,
                size,
            });
        }
        s.gen_snap_meta()?;

        Ok(s)
    }

    /// Apply snapshot meta
    fn apply_snap_meta(&mut self, meta: SnapMeta) {
        self.snap_files = meta
            .files
            .into_iter()
            .map(|(filename, size)| SnapFile {
                filename,
                size,
                ..Default::default()
            })
            .collect::<Vec<_>>();
    }

    /// Generate snapshot meta
    fn gen_snap_meta(&mut self) -> Result<(), EngineError> {
        let files = SnapMeta {
            files: self
                .snap_files
                .iter()
                .map(|sf| (sf.filename.clone(), sf.size))
                .collect::<Vec<_>>(),
        };
        let meta_bytes = bincode::serialize(&files).map_err(|e| {
            EngineError::UnderlyingError(format!("cannot serialize snapshot meta: {e}"))
        })?;
        let len = meta_bytes.len().numeric_cast::<u64>();
        let mut data = Vec::new();
        data.write_all(&len.to_le_bytes())?;
        data.write_all(&meta_bytes)?;
        self.meta = Some(Cursor::new(data));
        Ok(())
    }

    /// path of current file
    fn current_file_path(&self) -> PathBuf {
        let Some(current_filename) = self.snap_files.get(self.file_index).map(|sf| &sf.filename) else {
            unreachable!("this method must be called when self.file_index < self.snap_files.len()")
        };
        self.dir.join(current_filename)
    }

    /// tmp path of current file
    fn current_file_tmp_path(&self) -> PathBuf {
        let Some(current_filename) = self.snap_files.get(self.file_index).map(|sf| &sf.filename) else {
            unreachable!("this method must be called when self.file_index < self.snap_files.len()")
        };
        self.dir.join(format!("{current_filename}.tmp"))
    }
}

impl Read for RocksSnapshot {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if let Some(ref mut meta) = self.meta {
            let n = meta.read(buf)?;
            if n == 0 {
                let _ignore = self.meta.take();
            } else {
                return Ok(n);
            }
        }

        while self.file_index < self.snap_files.len() {
            let f = if let Some(ref mut f) = self.current_file {
                f
            } else {
                let path = self.current_file_path();
                let reader = File::open(path)?;
                self.current_file = Some(reader);
                self.current_file
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("current_file must be `Some` here"))
            };
            let n = f.read(buf)?;
            if n == 0 {
                let _ignore = self.current_file.take();
                self.file_index = self.file_index.overflow_add(1);
            } else {
                return Ok(n);
            }
        }
        Ok(0)
    }
}

impl Write for RocksSnapshot {
    #[inline]
    #[allow(clippy::indexing_slicing)] // safe indexing
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let (mut next_buf, mut written_bytes) = (buf, 0);
        if self.meta.is_none() {
            let meta_len_bytes: [u8; 8] = next_buf[0..8]
                .try_into()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let meta_len = u64::from_le_bytes(meta_len_bytes);

            let meta_bytes: Vec<u8> = next_buf[8..meta_len.overflow_add(8).numeric_cast()]
                .try_into()
                .unwrap_or_else(|_e| unreachable!("infallible "));
            let meta = bincode::deserialize(&meta_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            self.apply_snap_meta(meta);

            self.meta = Some(Cursor::new(vec![]));
            written_bytes = written_bytes
                .overflow_add(meta_len.numeric_cast())
                .overflow_add(8);
            next_buf = &next_buf[meta_len.overflow_add(8).numeric_cast()..];
        }

        while self.file_index < self.snap_files.len() {
            let snap_file = &mut self.snap_files[self.file_index];
            assert!(snap_file.size != 0);
            let left = snap_file
                .size
                .overflow_sub(snap_file.written_size)
                .numeric_cast();
            let (write_len, switch, finished) = match next_buf.len().cmp(&left) {
                Ordering::Greater => (left, true, false),
                Ordering::Equal => (left, true, true),
                Ordering::Less => (next_buf.len(), false, true),
            };
            snap_file.written_size = snap_file
                .written_size
                .overflow_add(write_len.numeric_cast());
            written_bytes = written_bytes.overflow_add(write_len);

            let buffer = &next_buf[0..write_len];

            let f = if let Some(ref mut f) = self.current_file {
                f
            } else {
                let path = self.current_file_tmp_path();
                let writer = File::create(path)?;
                self.current_file = Some(writer);
                self.current_file
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("current_file must be `Some` here"))
            };
            f.write_all(buffer)?;

            if switch {
                next_buf = &next_buf[write_len..];
                let old = self.current_file.take();
                if let Some(mut old_f) = old {
                    old_f.flush()?;
                    let path = self.current_file_path();
                    let tmp_path = self.current_file_tmp_path();
                    fs::rename(tmp_path, path)?;
                }
                self.file_index = self.file_index.overflow_add(1);
            }
            if finished {
                break;
            }
        }
        Ok(written_bytes)
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(ref mut f) = self.current_file {
            f.flush()?;
        }
        Ok(())
    }
}

impl SnapshotApi for RocksSnapshot {
    #[inline]
    fn size(&self) -> u64 {
        let mut size = self.snap_files.iter().map(|f| f.size).sum::<u64>();
        if let Some(ref meta) = self.meta {
            size = size.overflow_add(meta.get_ref().len().numeric_cast::<u64>());
        }
        size
    }
}

impl StorageEngine for RocksEngine {
    type Snapshot = RocksSnapshot;

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
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, sync: bool) -> Result<(), EngineError> {
        let mut batch = WriteBatchWithTransaction::<false>::default();

        for op in wr_ops {
            match op {
                WriteOperation::Put(Put { table, key, value }) => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.put_cf(&cf, key, value);
                }
                WriteOperation::Delete(Delete { table, key }) => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    batch.delete_cf(&cf, key);
                }
                WriteOperation::DeleteRange(DeleteRange { table, from, to }) => {
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
        self.inner
            .write_opt(batch, &opt)
            .map_err(|e| EngineError::UnderlyingError(format!("{e}")))
    }

    #[inline]
    fn snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<RocksSnapshot, EngineError> {
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
        let s = RocksSnapshot::new_for_sending(path.as_ref())?;
        Ok(s)
    }

    #[inline]
    fn apply_snapshot(&self, s: RocksSnapshot, tables: &[&'static str]) -> Result<(), EngineError> {
        for cf_name in tables {
            let file_name = format!("{cf_name}.sst");
            let file_path = s.dir.join(file_name);
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
pub fn destroy(data_dir: &PathBuf) {
    #[allow(clippy::unwrap_used)]
    DB::destroy(&Options::default(), data_dir).unwrap();
}

#[cfg(test)]
mod test {
    use std::{
        iter::{repeat, zip},
        path::PathBuf,
    };

    use super::*;
    const TESTTABLES: [&'static str; 3] = ["kv", "lease", "auth"];

    #[test]
    fn write_batch_into_a_non_existing_table_should_fail() {
        let data_dir = PathBuf::from("/tmp/write_batch_into_a_non_existing_table_should_fail");
        let engine = RocksEngine::new(&data_dir, &TESTTABLES).unwrap();

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
        drop(engine);
        destroy(&data_dir);
    }

    #[test]
    fn snapshot_should_work() {
        let origin_data_dir = PathBuf::from("/tmp/snapshot_should_work_origin");
        let recover_data_dir = PathBuf::from("/tmp/snapshot_should_work_recover");
        let snapshot_dir = PathBuf::from("/tmp/snapshot");

        let engine = RocksEngine::new(&origin_data_dir, &TESTTABLES).unwrap();
        let put = WriteOperation::Put(Put::new("kv", "key".into(), "value".into()));
        assert!(engine.write_batch(vec![put], false).is_ok());

        let snapshot = engine.snapshot(&snapshot_dir, &TESTTABLES).unwrap();
        let engine_2 = RocksEngine::new(&recover_data_dir, &TESTTABLES).unwrap();
        assert!(engine_2.apply_snapshot(snapshot, &TESTTABLES).is_ok());

        let value = engine_2.get("kv", "key").unwrap();
        assert_eq!(value, Some("value".into()));

        drop(engine);
        drop(engine_2);
        destroy(&origin_data_dir);
        destroy(&recover_data_dir);
        #[allow(clippy::unwrap_used)]
        fs::remove_dir_all(&snapshot_dir).unwrap();
    }
}
