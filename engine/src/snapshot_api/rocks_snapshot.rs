use std::{
    cmp::Ordering,
    fs,
    io::{
        self, Cursor,
        ErrorKind::{self},
    },
    path::PathBuf,
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use super::SnapshotApi;
use crate::error::EngineError;

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
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let (mut next_buf, mut written_bytes) = (buf, 0);

        #[allow(clippy::indexing_slicing)] // length is checked when reading meta
        if self.meta.is_current {
            let Some(meta_len_slice) = next_buf.get(0..8) else {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "cannot read meta length from buffer",
                ));
            };
            let meta_len_bytes: [u8; 8] = meta_len_slice
                .try_into()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
            let meta_len = u64::from_le_bytes(meta_len_bytes);

            let Some(meta_slice) = next_buf.get(8..meta_len.overflow_add(8).numeric_cast()) else {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "cannot read meta from buffer",
                ));
            };
            let meta_bytes: Vec<u8> = meta_slice
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
    async fn read_exact(&mut self, mut buf: &mut [u8]) -> std::io::Result<()> {
        while !buf.is_empty() {
            // the return value of read function is not larger than the input buf's length.
            #[allow(clippy::indexing_slicing)]
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
            Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        }
    }

    #[inline]
    async fn write_all(&mut self, mut buf: &[u8]) -> std::io::Result<()> {
        let mut written_size = 0;
        // the return value of write function is not larger than the input buf's length.
        #[allow(clippy::indexing_slicing)]
        while !buf.is_empty() {
            let n = self.write(buf).await?;
            written_size = written_size.overflow_add(n);
            buf = &buf[n..];
            if n == 0 {
                let size = self.size().numeric_cast();
                if written_size == size {
                    break;
                }
                return Err(io::ErrorKind::WriteZero.into());
            }
        }
        Ok(())
    }

    #[inline]
    fn rewind(&mut self) -> std::io::Result<()> {
        self.snap_file_idx = 0;
        self.meta.is_current = true;
        self.meta.data.set_position(0);
        self.current_file = None;
        Ok(())
    }

    #[inline]
    async fn clean(&mut self) -> std::io::Result<()> {
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
    #[tokio::test]
    async fn test_rocks_snapshot_api() {
        let dir = PathBuf::from("/tmp/test_rocks_snapshot_api");
        let origin_path = dir.join("origin");
        let receiver_path = dir.join("receiver");
        fs::create_dir_all(&origin_path).unwrap();
        for i in 0..3 {
            let f_path = origin_path.join(i.to_string());
            let mut f = File::create(f_path).await.unwrap();
            f.write(&[0, 1, 2, 3]).await.unwrap();
            f.flush().await.unwrap();
        }
        let mut origin_snapshot = RocksSnapshot::new_for_sending(&origin_path).unwrap();

        let mut buf = vec![0; origin_snapshot.size().numeric_cast()];
        origin_snapshot.read_exact(&mut buf).await.unwrap();

        let mut receiver_snapshot = RocksSnapshot::new_for_receiving(&receiver_path).unwrap();
        receiver_snapshot.write_all(&buf).await.unwrap();

        for i in 0..3 {
            let f1_path = origin_path.join(i.to_string());
            let f2_path = receiver_path.join(i.to_string());
            let mut f1 = File::open(f1_path).await.unwrap();
            let mut f1_data = Vec::new();
            f1.read_to_end(&mut f1_data).await.unwrap();
            let mut f2 = File::open(f2_path).await.unwrap();
            let mut f2_data = Vec::new();
            f2.read_to_end(&mut f2_data).await.unwrap();
            assert_eq!(f1_data, f2_data);
        }

        fs::remove_dir_all(dir).unwrap();
    }
}
