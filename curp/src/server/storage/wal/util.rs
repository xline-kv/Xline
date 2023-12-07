use std::{
    fs::{File as StdFile, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use sha2::{digest::Output, Digest, Sha256};
use tokio::fs::File as TokioFile;

/// File that is exclusively locked
#[derive(Debug)]
pub(super) struct LockedFile {
    /// The inner std file
    file: Option<StdFile>,
    /// The path of the file
    path: PathBuf,
}

impl LockedFile {
    /// Opens the file in read and append mode
    pub(super) fn open_rw(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        file.try_lock_exclusive()?;

        Ok(Self {
            file: Some(file),
            path: path.as_ref().into(),
        })
    }

    /// Pre-allocates the file
    pub(super) fn preallocate(&mut self, size: u64) -> io::Result<()> {
        if size == 0 {
            return Ok(());
        }

        self.file().allocate(size)
    }

    /// Gets the path of this file
    pub(super) fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Renames the current file
    ///
    /// We will discard this file if the rename has failed
    pub(super) fn rename(mut self, new_name: impl AsRef<Path>) -> io::Result<Self> {
        let mut new_path = parent_dir(&self.path);
        new_path.push(new_name.as_ref());
        std::fs::rename(&self.path, &new_path)?;
        sync_parent_dir(&new_path)?;

        Ok(Self {
            file: self.file.take(),
            path: PathBuf::from(new_name.as_ref()),
        })
    }

    /// Converts self to std file
    pub(super) fn into_std(self) -> StdFile {
        let mut this = std::mem::ManuallyDrop::new(self);
        this.file
            .take()
            .unwrap_or_else(|| unreachable!("File should always exist after creation"))
    }

    /// Converts self to tokio file
    pub(super) fn into_async(self) -> TokioFile {
        TokioFile::from_std(self.into_std())
    }

    /// Gets the file wrapped inside an `Option`
    fn file(&mut self) -> &mut StdFile {
        self.file
            .as_mut()
            .unwrap_or_else(|| unreachable!("File should always exist after creation"))
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        if self.file.is_some() && is_exist(self.path()) {
            let _ignore = std::fs::remove_file(self.path());
        }
    }
}

/// Gets the all files with the extension under the given folder
pub(super) fn get_file_paths_with_ext(
    dir: impl AsRef<Path>,
    ext: &str,
) -> io::Result<Vec<PathBuf>> {
    let mut files = vec![];
    for result in std::fs::read_dir(dir)? {
        let file = result?;
        if let Some(filename) = file.file_name().to_str() {
            if filename.ends_with(ext) {
                files.push(file.path());
            }
        }
    }
    Ok(files)
}

/// Gets the parent dir
pub(super) fn parent_dir(dir: impl AsRef<Path>) -> PathBuf {
    let mut parent = PathBuf::from(dir.as_ref());
    let _ignore = parent.pop();
    parent
}

/// Fsyncs the parent directory
pub(super) fn sync_parent_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    let parent_dir = parent_dir(&dir);
    let parent = std::fs::File::open(parent_dir)?;
    parent.sync_all()?;

    Ok(())
}

/// Gets the checksum of the slice, we use Sha256 as the hash function
pub(super) fn get_checksum(data: &[u8]) -> Output<Sha256> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize()
}

/// Validates the the data with the given checksum
pub(super) fn validate_data(data: &[u8], checksum: &[u8]) -> bool {
    AsRef::<[u8]>::as_ref(&get_checksum(data)) == checksum
}

/// Checks whether the file exist
pub(super) fn is_exist(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path).is_ok()
}

/// Parses a u64 from u8 slice
pub(super) fn parse_u64(bytes_le: &[u8]) -> u64 {
    assert_eq!(bytes_le.len(), 8, "The slice passed should be 8 bytes long");
    u64::from_le_bytes(
        bytes_le
            .try_into()
            .unwrap_or_else(|_| unreachable!("This conversion should always exist")),
    )
}

#[cfg(test)]
mod tests {
    use std::{io::Read, process::Command};

    use super::*;

    #[test]
    fn file_rename_is_ok() {
        let mut tempdir = tempfile::tempdir().unwrap();
        let mut path = PathBuf::from(tempdir.path());
        path.push("file.test");
        let lfile = LockedFile::open_rw(&path).unwrap();
        let new_name = "new_name.test";
        let mut new_path = parent_dir(&path);
        new_path.push(new_name);
        lfile.rename(new_name);
        assert!(!is_exist(path));
        assert!(is_exist(new_path));
    }

    #[test]
    #[allow(clippy::verbose_file_reads)] // false positive
    fn file_open_is_exclusive() {
        let mut tempdir = tempfile::tempdir().unwrap();
        let mut path = PathBuf::from(tempdir.path());
        path.push("file.test");
        let mut lfile = LockedFile::open_rw(&path).unwrap();
        assert!(
            LockedFile::open_rw(&path).is_err(),
            "acquire lock should failed"
        );
    }

    #[test]
    fn get_file_paths_with_ext_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let num_paths = 10;
        let paths_create: Vec<_> = (0..num_paths)
            .map(|i| {
                let mut path = PathBuf::from(dir.path());
                path.push(format!("{i}.test"));
                std::fs::File::create(&path).unwrap();
                path
            })
            .collect();
        let mut paths = get_file_paths_with_ext(dir.path(), ".test").unwrap();
        paths.sort();
        assert_eq!(paths.len(), num_paths);
        assert!(paths
            .into_iter()
            .zip(paths_create.into_iter())
            .all(|(x, y)| x.as_path() == y.as_path()));
    }
}
