use std::{
    cmp::Ordering,
    fs::{self, File},
    io::{self, Cursor, Error as IoError, ErrorKind::Other, Read, Seek, Write},
    iter::repeat,
    path::{Path, PathBuf},
    sync::Arc,
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use rocksdb::{
    Error as RocksError, IteratorMode, Options, SstFileWriter, WriteBatchWithTransaction,
    WriteOptions, DB,
};
use serde::{Deserialize, Serialize};

use crate::{
    engine_api::{SnapshotApi, StorageEngine, WriteOperation},
    error::EngineError,
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

/// Meta data of the snapshot
#[derive(Debug, Default)]
struct Meta {
    /// Meta data
    data: Cursor<Vec<u8>>,
    /// Whether the meta is current or not
    is_current: bool,
}

impl Meta {
    /// new `Meta`
    fn new() -> Self {
        Self {
            data: Cursor::new(Vec::new()),
            is_current: true,
        }
    }

    /// length of the meta data
    fn len(&self) -> usize {
        self.data.get_ref().len()
    }
}

/// Snapshot type for `RocksDB`
#[derive(Debug, Default)]
pub struct RocksSnapshot {
    /// Snapshot meta
    meta: Meta,
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
            meta: Meta::new(),
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
                EngineError::InvalidArgument("cannot convert filename to string".to_owned())
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
        self.meta.data = Cursor::new(data);
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

        if self.meta.is_current {
            let n = self.meta.data.read(buf)?;
            if n == 0 {
                self.meta.is_current = false;
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
        if self.meta.is_current {
            let meta_len_bytes: [u8; 8] = next_buf[0..8]
                .try_into()
                .map_err(|e| io::Error::new(Other, e))?;
            let meta_len = u64::from_le_bytes(meta_len_bytes);

            let meta_bytes: Vec<u8> = next_buf[8..meta_len.overflow_add(8).numeric_cast()]
                .try_into()
                .unwrap_or_else(|_e| unreachable!("infallible "));
            let meta = bincode::deserialize(&meta_bytes).map_err(|e| io::Error::new(Other, e))?;

            self.apply_snap_meta(meta);

            self.meta.is_current = false;
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

impl Seek for RocksSnapshot {
    #[inline]
    #[allow(clippy::indexing_slicing)] // safe indexing
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let (base_pos, offset) = match pos {
            io::SeekFrom::Start(mut offset) => {
                let new_pos = offset;
                self.file_index = 0;
                self.meta.is_current = true;
                let meta_len = self.meta.len().numeric_cast();
                if offset < meta_len {
                    return self.meta.data.seek(io::SeekFrom::Start(offset));
                }
                offset = offset.overflow_sub(meta_len);
                self.meta.is_current = false;
                let mut current_file_size = self.snap_files[self.file_index].size;
                while offset > current_file_size {
                    offset = offset.overflow_sub(current_file_size);
                    self.file_index = self.file_index.overflow_add(1);
                    current_file_size = self.snap_files[self.file_index].size;
                }
                self.current_file = Some(File::open(self.current_file_path())?);
                let f = self
                    .current_file
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("current_file must be `Some` here"));
                let _ignore = f.seek(io::SeekFrom::Start(offset))?;
                return Ok(new_pos);
            }
            io::SeekFrom::End(offset) => {
                assert!(offset <= 0);
                let base = self.size().numeric_cast();
                (base, offset)
            }
            io::SeekFrom::Current(offset) => {
                let current_pos = if self.meta.is_current {
                    self.meta.data.stream_position()?
                } else {
                    let mut current_pos: u64 = self.meta.len().numeric_cast();
                    for i in 0..self.file_index {
                        current_pos = current_pos.overflow_add(self.snap_files[i].size);
                    }
                    let file_pos = self
                        .current_file
                        .as_mut()
                        .map_or(Ok(0), Seek::stream_position)?;
                    current_pos = current_pos.overflow_add(file_pos);
                    current_pos
                };
                (current_pos, offset)
            }
        };
        let Some(new_offset) = base_pos.checked_add_signed(offset) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            ));
        };
        self.seek(io::SeekFrom::Start(new_offset))
    }
}

impl SnapshotApi for RocksSnapshot {
    #[inline]
    fn size(&self) -> u64 {
        let mut size = self.snap_files.iter().map(|f| f.size).sum::<u64>();
        size = size.overflow_add(self.meta.len().numeric_cast());
        size
    }
}

impl StorageEngine for RocksEngine {
    type Snapshot = RocksSnapshot;

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

    use super::*;
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

    #[test]
    fn snapshot_should_work() {
        let origin_data_dir = PathBuf::from("/tmp/snapshot_should_work_origin");
        let recover_data_dir = PathBuf::from("/tmp/snapshot_should_work_recover");
        let snapshot_dir = PathBuf::from("/tmp/snapshot");

        let engine = RocksEngine::new(&origin_data_dir, &TESTTABLES).unwrap();
        let put = WriteOperation::new_put("kv", "key".into(), "value".into());
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
        fs::remove_dir_all(&snapshot_dir).unwrap();
    }

    #[test]
    fn test_snapshot_seek() {
        let path = PathBuf::from("/tmp/test_snapshot_seek");
        {
            fs::create_dir_all(&path).unwrap();
            (0..3).for_each(|i| {
                let mut f = File::create(path.join(i.to_string())).unwrap();
                f.write(&vec![i; 3]).unwrap();
            });
        }
        let mut snapshot = RocksSnapshot::new_for_sending(&path).unwrap();
        let mete_size = snapshot.meta.len().numeric_cast();

        let expect = snapshot.snap_files.iter().fold(Vec::new(), |mut acc, f| {
            match f.filename.as_str() {
                "0" => acc.append(&mut vec![0, 0, 0]),
                "1" => acc.append(&mut vec![1, 1, 1]),
                "2" => acc.append(&mut vec![2, 2, 2]),
                _ => unreachable!(),
            }
            acc
        });

        let mut buf = Vec::new();
        snapshot.seek(io::SeekFrom::Start(mete_size)).unwrap();
        snapshot.read_to_end(buf.as_mut()).unwrap();
        assert_eq!(buf, expect);

        buf.clear();
        snapshot.seek(io::SeekFrom::End(-5)).unwrap();
        snapshot.read_to_end(buf.as_mut()).unwrap();
        assert_eq!(buf, expect[4..]);

        buf.clear();
        snapshot.seek(io::SeekFrom::Current(-6)).unwrap();
        snapshot.read_to_end(buf.as_mut()).unwrap();
        assert_eq!(buf, expect[3..]);

        fs::remove_dir_all(&path).unwrap();
    }
}
