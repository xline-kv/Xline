use std::{
    cmp::Ordering,
    env::temp_dir,
    fs, io,
    io::{Cursor, Error as IoError, ErrorKind},
    iter::repeat,
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};

use bytes::{Buf, Bytes, BytesMut};
use clippy_utilities::{NumericCast, OverflowArithmetic};
use rocksdb::{
    Error as RocksError, IteratorMode, Options, SstFileWriter, WriteBatchWithTransaction,
    WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncWriteExt};
use tokio_util::io::read_buf;

use crate::{
    api::{
        engine_api::{StorageEngine, WriteOperation},
        snapshot_api::SnapshotApi,
    },
    error::EngineError,
};

/// Install snapshot chunk size: 64KB
const SNAPSHOT_CHUNK_SIZE: usize = 64 * 1024;

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
                "IO error" => EngineError::IoError(IoError::new(ErrorKind::Other, err_msg)),
                _ => EngineError::UnderlyingError(err_msg.to_owned()),
            }
        } else {
            EngineError::UnderlyingError(err)
        }
    }
}

/// `RocksDB` Storage Engine
#[derive(Debug)]
pub struct RocksEngine {
    /// The inner storage engine of `RocksDB`
    inner: DB,
    /// The tables of current engine
    tables: Vec<String>,
    /// The size cache of the engine
    size: AtomicU64,
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
        let db = DB::open_cf(&db_opts, data_dir, tables)?;
        let size = Self::get_db_size(&db, tables)?;
        Ok(Self {
            inner: db,
            tables: tables.iter().map(|s| (*s).to_owned()).collect(),
            size: AtomicU64::new(size),
        })
    }

    /// Get the total sst file size of all tables
    /// # WARNING
    /// This method need to flush memtable to disk. it may be slow. do not call it frequently.
    fn get_db_size<T: AsRef<str>, V: AsRef<[T]>>(db: &DB, tables: V) -> Result<u64, EngineError> {
        let mut size = 0;
        for table in tables.as_ref() {
            let cf = db
                .cf_handle(table.as_ref())
                .ok_or_else(|| EngineError::TableNotFound(table.as_ref().to_owned()))?;
            db.flush_cf(&cf)?;
            size = db
                .property_int_value_cf(&cf, rocksdb::properties::TOTAL_SST_FILES_SIZE)?
                .ok_or(EngineError::UnderlyingError(
                    "Got None when read TOTAL_SST_FILES_SIZE".to_owned(),
                ))?
                .overflow_add(size);
        }
        Ok(size)
    }

    /// Apply snapshot from file
    pub async fn apply_snapshot_from_file<P>(
        &self,
        snap_path: P,
        tables: &[&'static str],
    ) -> Result<(), EngineError>
    where
        P: AsRef<Path>,
    {
        let mut snapshot_f = tokio::fs::File::open(snap_path).await?;
        let tmp_path = temp_dir().join(format!("snapshot-{}", uuid::Uuid::new_v4()));
        let mut rocks_snapshot = RocksSnapshot::new_for_receiving(tmp_path)?;
        let mut buf = BytesMut::with_capacity(SNAPSHOT_CHUNK_SIZE);
        while let Ok(n) = read_buf(&mut snapshot_f, &mut buf).await {
            if n == 0 {
                break;
            }
            rocks_snapshot.write_all(buf.split().freeze()).await?;
        }
        self.apply_snapshot(rocks_snapshot, tables).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
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
                .iterator_cf(&cf, IteratorMode::Start)
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
        let mut size = 0;
        for op in wr_ops {
            match op {
                WriteOperation::Put { table, key, value } => {
                    let cf = self
                        .inner
                        .cf_handle(table)
                        .ok_or(EngineError::TableNotFound(table.to_owned()))?;
                    // max write data size = 2 * key + value + cf_handle_size + 1008
                    size = size
                        .overflow_add(key.len().overflow_mul(2))
                        .overflow_add(value.len())
                        .overflow_add(table.len())
                        .overflow_add(1008);
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
        let res = self.inner.write_opt(batch, &opt).map_err(EngineError::from);
        if res.is_ok() {
            _ = self
                .size
                .fetch_add(size.numeric_cast(), std::sync::atomic::Ordering::Relaxed);
        }
        res
    }

    #[inline]
    fn get_snapshot(
        &self,
        path: impl AsRef<Path>,
        tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError> {
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
        RocksSnapshot::new_for_sending(path.as_ref())
    }

    #[inline]
    async fn apply_snapshot(
        &self,
        mut snapshot: Self::Snapshot,
        tables: &[&'static str],
    ) -> Result<(), EngineError> {
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
        snapshot.clean().await?;
        Ok(())
    }

    fn estimated_file_size(&self) -> u64 {
        self.size.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get the file size of the engine. This method need to flush memtable to disk. do not call it frequently.
    fn file_size(&self) -> Result<u64, EngineError> {
        let size = Self::get_db_size(&self.inner, &self.tables)?;
        self.size.store(size, std::sync::atomic::Ordering::Relaxed);
        Ok(size)
    }
}

/// Human readable format for `RocksEngine`
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

impl SnapFile {
    /// remain size of the file
    fn remain_size(&self) -> u64 {
        self.size.overflow_sub(self.written_size)
    }
}

/// Serialized `SnapMeta`, used for reading and writing,
#[derive(Debug, Default)]
struct Meta {
    /// Meta data
    data: Cursor<Bytes>,
    /// when `is_current` is true, read from meta, otherwise read from files
    is_current: bool,
}

impl Meta {
    /// new `Meta`
    fn new() -> Self {
        Self {
            data: Cursor::new(Bytes::new()),
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
    snap_file_idx: usize,
    /// current file
    current_file: Option<File>,
}

impl RocksSnapshot {
    /// New empty `RocksSnapshot`
    fn new<P>(dir: P) -> Self
    where
        P: Into<PathBuf>,
    {
        RocksSnapshot {
            meta: Meta::new(),
            dir: dir.into(),
            snap_files: Vec::new(),
            snap_file_idx: 0,
            current_file: None,
        }
    }

    /// Create a new snapshot for receiving
    /// # Errors
    /// Return `EngineError` when create directory failed.
    #[inline]
    pub fn new_for_receiving<P>(dir: P) -> Result<Self, EngineError>
    where
        P: Into<PathBuf>,
    {
        let dir = dir.into();
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
        }
        Ok(Self::new(dir))
    }

    /// Create a new snapshot for sending
    /// # Errors
    /// Return `EngineError` when read directory failed.
    #[inline]
    pub fn new_for_sending<P>(dir: P) -> Result<Self, EngineError>
    where
        P: Into<PathBuf>,
    {
        let dir = dir.into();
        let files = fs::read_dir(&dir)?;
        let mut s = Self::new(dir);
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

    /// Get sst file path of the table
    pub(crate) fn sst_path(&self, table: &str) -> PathBuf {
        self.dir.join(table).with_extension("sst")
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
        let mut meta_data = BytesMut::with_capacity(meta_bytes.len().overflow_add(8));
        meta_data.extend_from_slice(&len.to_le_bytes());
        meta_data.extend_from_slice(&meta_bytes);
        *self.meta.data.get_mut() = meta_data.freeze();
        Ok(())
    }

    /// path of current file
    fn current_file_path(&self, tmp: bool) -> PathBuf {
        let Some(current_filename) = self.snap_files.get(self.snap_file_idx).map(|sf| &sf.filename) else {
            unreachable!("this method must be called when self.file_index < self.snap_files.len()")
        };
        let filename = if tmp {
            format!("{current_filename}.tmp")
        } else {
            current_filename.clone()
        };
        self.dir.join(filename)
    }

    /// Read data from the snapshot
    async fn read(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        if self.meta.is_current {
            let n = read_buf(&mut self.meta.data, buf).await?;
            if n == 0 {
                self.meta.is_current = false;
            } else {
                return Ok(n);
            }
        }

        while self.snap_file_idx < self.snap_files.len() {
            let mut f = if let Some(ref mut f) = self.current_file {
                f
            } else {
                let path = self.current_file_path(false);
                let reader = File::open(path).await?;
                self.current_file = Some(reader);
                self.current_file
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("current_file must be `Some` here"))
            };
            let n = read_buf(&mut f, buf).await?;
            if n == 0 {
                let _ignore = self.current_file.take();
                self.snap_file_idx = self.snap_file_idx.overflow_add(1);
            } else {
                return Ok(n);
            }
        }
        Ok(0)
    }

    /// Write snapshot data
    async fn write(&mut self, buf: &mut Bytes) -> io::Result<()> {
        if self.meta.is_current {
            if buf.len() < 8 {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "cannot read meta length from buffer",
                ));
            }
            let meta_len = buf.as_ref().get_u64_le();

            if buf.len() < meta_len.numeric_cast::<usize>().overflow_add(8) {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "cannot read meta from buffer",
                ));
            };
            let meta_data = buf.split_to(meta_len.numeric_cast::<usize>().overflow_add(8));
            #[allow(clippy::indexing_slicing)]
            let meta = bincode::deserialize(&meta_data[8..])
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

            self.apply_snap_meta(meta);
            *self.meta.data.get_mut() = meta_data;
            self.meta.is_current = false;
        }

        // the snap_file_idx has checked with while's pattern
        // written_len is calculated by next_buf.len() so it must be less than next_buf's length
        #[allow(clippy::indexing_slicing)]
        while self.snap_file_idx < self.snap_files.len() {
            let snap_file = &mut self.snap_files[self.snap_file_idx];
            if snap_file.size == 0 {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    format!("snap file {} size is 0", snap_file.filename),
                ));
            }
            let left = snap_file.remain_size().numeric_cast();
            let (write_len, switch, finished) = match buf.len().cmp(&left) {
                Ordering::Greater => (left, true, false),
                Ordering::Equal => (left, true, true),
                Ordering::Less => (buf.len(), false, true),
            };
            snap_file.written_size = snap_file
                .written_size
                .overflow_add(write_len.numeric_cast());

            let f = if let Some(ref mut f) = self.current_file {
                f
            } else {
                let path = self.current_file_path(true);
                let writer = File::create(path).await?;
                self.current_file = Some(writer);
                self.current_file
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("current_file must be `Some` here"))
            };
            let buffer = buf.split_to(write_len);
            f.write_all(&buffer).await?;

            if switch {
                let old = self.current_file.take();
                if let Some(mut old_f) = old {
                    old_f.flush().await?;
                    let path = self.current_file_path(false);
                    let tmp_path = self.current_file_path(true);
                    fs::rename(tmp_path, path)?;
                }
                self.snap_file_idx = self.snap_file_idx.overflow_add(1);
            }
            if finished {
                break;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl SnapshotApi for RocksSnapshot {
    #[inline]
    fn size(&self) -> u64 {
        self.snap_files
            .iter()
            .map(|f| f.size)
            .sum::<u64>() // sst files size
            .overflow_add(self.meta.len().numeric_cast()) // meta size
    }

    #[inline]
    fn rewind(&mut self) -> io::Result<()> {
        self.snap_file_idx = 0;
        self.meta.is_current = true;
        self.meta.data.set_position(0);
        self.current_file = None;
        Ok(())
    }

    #[inline]
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<()> {
        self.read(buf).await.map(|_n| ())
    }

    #[inline]
    async fn write_all(&mut self, mut buf: Bytes) -> std::io::Result<()> {
        let buf_size = buf.remaining();
        while buf.has_remaining() {
            let prev_rem = buf.remaining();
            self.write(&mut buf).await?;
            if prev_rem == buf.remaining() {
                let written_size = buf_size.overflow_sub(buf.remaining());
                let size: usize = self.size().numeric_cast();
                if written_size == size {
                    break;
                }
                return Err(io::ErrorKind::WriteZero.into());
            }
        }
        Ok(())
    }

    #[inline]
    async fn clean(&mut self) -> io::Result<()> {
        for snap_file in &self.snap_files {
            let path = self.dir.join(&snap_file.filename);
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::env::temp_dir;

    use test_macros::abort_on_panic;

    use super::*;

    static TEST_TABLES: [&str; 3] = ["t1", "t2", "t3"];
    #[tokio::test]
    #[abort_on_panic]
    async fn test_rocks_errors() {
        let dir = PathBuf::from("/tmp/test_rocks_errors");
        let engine_path = dir.join("engine");
        let snapshot_path = dir.join("snapshot");
        let engine = RocksEngine::new(engine_path, &TEST_TABLES).unwrap();
        let res = engine.get("not_exist", "key");
        assert!(res.is_err());
        let res = engine.get_multi("not_exist", &["key"]);
        assert!(res.is_err());
        let res = engine.get_all("not_exist");
        assert!(res.is_err());
        let res = engine.get_snapshot(snapshot_path, &["not_exist"]);
        assert!(res.is_err());
        let fake_snapshot = RocksSnapshot {
            snap_files: vec![SnapFile {
                filename: "not_exist".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let res = engine.apply_snapshot(fake_snapshot, &["not_exist"]).await;
        assert!(res.is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_engine_size() {
        let path = temp_dir().join("test_engine_size");
        let engine = RocksEngine::new(path.clone(), &TEST_TABLES).unwrap();
        let size1 = engine.file_size().unwrap();
        engine
            .write_batch(
                vec![WriteOperation::new_put(
                    "t1",
                    b"key".to_vec(),
                    b"value".to_vec(),
                )],
                true,
            )
            .unwrap();
        let size2 = engine.file_size().unwrap();
        assert!(size2 > size1);
        fs::remove_dir_all(path).unwrap();
    }
}
