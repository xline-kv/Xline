use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use super::{
    segment::WALSegment,
    util::{get_checksum, get_file_paths_with_ext, is_exist, parse_u64, validate_data, LockedFile},
};

/// The name of the RWAL file
const REMOVER_WAL_FILE_NAME: &str = "segments.rwal";

/// Atomic remover of segment files
///
/// The remover will firstly creates a write ahead log that stores
/// the removal information, then it will remove it after the segment
/// removal has completed.
pub(super) struct SegmentRemover {
    /// The WAL path for storing the remove information
    rwal_path: PathBuf,
}

impl SegmentRemover {
    #[allow(clippy::doc_markdown)] // False positive for ASCII graph
    /// Recover from existing RWAL
    ///
    /// * RWAL layout
    ///
    ///  |----------+----------+-----+----------+-------------------|
    ///  | record 0 | record 1 | ... | record n | checksum (sha256) |
    ///  |----------+----------+-----+----------+-------------------|
    ///
    /// * The layout of each record
    ///
    ///  0      1      2      3      4      5      6      7      8
    ///  |------+------+------+------+------+------+------+------|
    ///  | BaseIndex                                             |
    ///  |------+------+------+------+------+------+------+------|
    ///  | SegmentID                                             |
    ///  |------+------+------+------+------+------+------+------|
    #[allow(clippy::arithmetic_side_effects)] // won't overflow
    pub(super) async fn recover(dir: impl AsRef<Path>) -> io::Result<()> {
        let wal_path = Self::rwal_path(&dir);
        if !is_exist(&wal_path) {
            return Ok(());
        }

        let mut wal = LockedFile::open_rw(wal_path.clone())?.into_async();
        let mut buf = vec![];
        let n = wal.read_to_end(&mut buf).await?;
        /// At least checksum + one record
        if n < 32 + 16 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        let checksum = buf.split_off(n - 32);
        if !validate_data(&buf, &checksum) {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        let to_remove_paths = buf.chunks_exact(16).map(|chunk| {
            let (base_index_bytes, segment_id_bytes) = chunk.split_at(8);
            let segment_id = parse_u64(segment_id_bytes);
            let base_index = parse_u64(base_index_bytes);
            let file_name = WALSegment::segment_name(segment_id, base_index);
            let mut path = PathBuf::from(dir.as_ref());
            path.push(file_name);
            path
        });

        Self::start_remove(to_remove_paths, wal_path)
    }

    /// Creates a new removal
    pub(super) async fn new_removal(
        dir: impl AsRef<Path>,
        segments: impl Iterator<Item = &WALSegment> + Clone,
    ) -> io::Result<()> {
        let wal_path = Self::rwal_path(&dir);

        // We ignore the existing RWAL file if we proceed a new removal
        if is_exist(&wal_path) {
            Self::remove_rwal(&wal_path)?;
        }

        //let file_name = WALSegment::segment_name(segment_id, base_index);
        let mut wal_data: Vec<_> = segments
            .clone()
            .flat_map(|s| {
                s.base_index()
                    .to_le_bytes()
                    .into_iter()
                    .chain(s.id().to_le_bytes().into_iter())
            })
            .collect();
        wal_data.append(&mut get_checksum(&wal_data).to_vec());

        let mut wal = LockedFile::open_rw(&wal_path)?.into_async();
        wal.write_all(&wal_data).await?;

        let to_remove_paths = segments.map(|s| {
            let mut path = PathBuf::from(dir.as_ref());
            path.push(WALSegment::segment_name(s.id(), s.base_index()));
            path
        });

        Self::start_remove(to_remove_paths, wal_path)
    }

    /// Start to remove the segment files
    fn start_remove(
        to_remove_paths: impl Iterator<Item = impl AsRef<Path>> + Clone,
        wal_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        for path in to_remove_paths.clone() {
            let _ignore = std::fs::metadata(path.as_ref())?;
            std::fs::remove_file(path.as_ref())?;
        }

        // Check if all record have been removed from fs
        for path in to_remove_paths {
            if is_exist(path.as_ref()) {
                return Err(io::Error::from(io::ErrorKind::Other));
            }
        }

        // If the removals are successful, remove the wal
        Self::remove_rwal(wal_path)
    }

    /// Remove the RWAL
    fn remove_rwal(wal_path: impl AsRef<Path>) -> io::Result<()> {
        std::fs::remove_file(wal_path.as_ref())?;
        if is_exist(wal_path.as_ref()) {
            return Err(io::Error::from(io::ErrorKind::Other));
        }
        Ok(())
    }

    /// Get the path of the RWAL
    fn rwal_path(dir: impl AsRef<Path>) -> PathBuf {
        let mut wal_path = PathBuf::from(dir.as_ref());
        wal_path.push(REMOVER_WAL_FILE_NAME);
        wal_path
    }
}

#[cfg(test)]
mod tests {
    use futures::future::join_all;

    use super::*;

    #[tokio::test]
    async fn wal_removal_is_ok() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir_path = PathBuf::from(temp_dir.path());

        let mut segments = vec![];
        let mut file_paths = vec![];
        for i in 0..10 {
            let mut file_path = dir_path.clone();
            file_path.push(format!("{i}.tmp"));
            let lfile = LockedFile::open_rw(&file_path).unwrap();
            segments.push(WALSegment::create(lfile, i + 1, i, 0).await.unwrap());
            file_paths.push(file_path);
        }

        SegmentRemover::new_removal(&dir_path, segments.iter()).await;

        assert!(
            file_paths.into_iter().all(|p| !is_exist(p)),
            "wal segment files partially removed"
        );
    }
}
