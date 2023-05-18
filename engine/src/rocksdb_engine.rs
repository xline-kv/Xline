use std::{
    cmp::Ordering,
    fs, io,
    io::{Cursor, Error as IoError, ErrorKind},
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
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{
    api::{
        engine_api::{StorageEngine, WriteOperation},
        snapshot_api::SnapshotApi,
    },
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
                "IO error" => EngineError::IoError(IoError::new(ErrorKind::Other, err_msg)),
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
    data: Cursor<Vec<u8>>,
    /// when `is_current` is true, read from meta, otherwise read from files
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
        let mut data = Vec::new();
        data.extend(len.to_le_bytes());
        data.extend(meta_bytes);
        self.meta.data = Cursor::new(data);
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
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.meta.is_current {
            let n = self.meta.data.read(buf).await?;
            if n == 0 {
                self.meta.is_current = false;
            } else {
                return Ok(n);
            }
        }

        while self.snap_file_idx < self.snap_files.len() {
            let f = if let Some(ref mut f) = self.current_file {
                f
            } else {
                let path = self.current_file_path(false);
                let reader = File::open(path).await?;
                self.current_file = Some(reader);
                self.current_file
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("current_file must be `Some` here"))
            };
            let n = f.read(buf).await?;
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
    #[allow(clippy::indexing_slicing)] // safe indexing
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let (mut next_buf, mut written_bytes) = (buf, 0);
        if self.meta.is_current {
            let meta_len_bytes: [u8; 8] = next_buf[0..8]
                .try_into()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
            let meta_len = u64::from_le_bytes(meta_len_bytes);

            let meta_bytes: Vec<u8> = next_buf[8..meta_len.overflow_add(8).numeric_cast()]
                .try_into()
                .unwrap_or_else(|_e| unreachable!("infallible"));
            let meta = bincode::deserialize(&meta_bytes)
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

            self.apply_snap_meta(meta);
            let mut data = Vec::new();
            data.extend(meta_len_bytes);
            data.extend(meta_bytes);
            *self.meta.data.get_mut() = data;
            self.meta.is_current = false;
            written_bytes = written_bytes
                .overflow_add(meta_len.numeric_cast())
                .overflow_add(8);
            next_buf = &next_buf[meta_len.overflow_add(8).numeric_cast()..];
        }

        while self.snap_file_idx < self.snap_files.len() {
            let snap_file = &mut self.snap_files[self.snap_file_idx];
            assert_ne!(snap_file.size, 0);
            let left = snap_file.remain_size().numeric_cast();
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
                let path = self.current_file_path(true);
                let writer = File::create(path).await?;
                self.current_file = Some(writer);
                self.current_file
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("current_file must be `Some` here"))
            };
            f.write_all(buffer).await?;

            if switch {
                next_buf = &next_buf[write_len..];
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
        Ok(written_bytes)
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
    #[allow(clippy::indexing_slicing)] // safe indexing
    async fn read_exact(&mut self, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read(buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                }
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if buf.is_empty() {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        }
    }

    #[inline]
    #[allow(clippy::indexing_slicing)] // safe indexing
    async fn write_all(&mut self, mut buf: &[u8]) -> io::Result<()> {
        let mut written_size = 0;
        while !buf.is_empty() {
            let n = self.write(buf).await?;
            written_size = written_size.overflow_add(n);
            buf = &buf[n..];
            if n == 0 {
                let size = self.size().numeric_cast();
                if written_size == size {
                    break;
                }
                return Err(ErrorKind::WriteZero.into());
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
    use super::*;

    static TEST_TABLES: [&str; 3] = ["t1", "t2", "t3"];
    #[tokio::test]
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
}
