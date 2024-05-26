/// Framed
pub mod framed;

/// File pipeline
pub mod pipeline;

use std::{
    fs::{File as StdFile, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use sha2::{digest::Output, Digest};

/// File that is exclusively locked
#[derive(Debug)]
pub struct LockedFile {
    /// The inner std file
    file: Option<StdFile>,
    /// The path of the file
    path: PathBuf,
}

impl LockedFile {
    /// Opens the file in read and append mode
    ///
    /// # Errors
    ///
    /// This function will return an error if file operations fail.
    #[inline]
    pub fn open_rw(path: impl AsRef<Path>) -> io::Result<Self> {
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
    ///
    /// # Errors
    ///
    /// This function will return an error if file operations fail.
    #[inline]
    pub fn preallocate(&mut self, size: u64) -> io::Result<()> {
        if size == 0 {
            return Ok(());
        }

        self.file().allocate(size)
    }

    /// Gets the path of this file
    #[inline]
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Renames the current file
    ///
    /// We will discard this file if the rename has failed
    ///
    /// # Errors
    ///
    /// This function will return an error if file operations fail.
    #[inline]
    pub fn rename(mut self, new_name: impl AsRef<Path>) -> io::Result<Self> {
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
    #[inline]
    #[must_use]
    pub fn into_std(self) -> StdFile {
        let mut this = std::mem::ManuallyDrop::new(self);
        this.file
            .take()
            .unwrap_or_else(|| unreachable!("File should always exist after creation"))
    }

    /// Gets the file wrapped inside an `Option`
    fn file(&mut self) -> &mut StdFile {
        self.file
            .as_mut()
            .unwrap_or_else(|| unreachable!("File should always exist after creation"))
    }
}

impl Drop for LockedFile {
    #[inline]
    fn drop(&mut self) {
        if self.file.is_some() && is_exist(self.path()) {
            let _ignore = std::fs::remove_file(self.path());
        }
    }
}

/// Gets the all files with the extension under the given folder.
///
/// # Errors
///
/// This function will return an error if failed to read the given directory.
#[inline]
pub fn get_file_paths_with_ext<A: AsRef<Path>>(dir: A, ext: &str) -> io::Result<Vec<PathBuf>> {
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
#[inline]
pub fn parent_dir<A: AsRef<Path>>(dir: A) -> PathBuf {
    let mut parent = PathBuf::from(dir.as_ref());
    let _ignore = parent.pop();
    parent
}

/// Fsyncs the parent directory
///
/// # Errors
///
/// This function will return an error if directory operations fail.
#[inline]
pub fn sync_parent_dir<A: AsRef<Path>>(dir: A) -> io::Result<()> {
    let parent_dir = parent_dir(&dir);
    let parent = std::fs::File::open(parent_dir)?;
    parent.sync_all()?;

    Ok(())
}

/// Gets the checksum of the slice, we use Sha256 as the hash function
#[inline]
#[must_use]
pub fn get_checksum<H: Digest>(data: &[u8]) -> Output<H> {
    let mut hasher = H::new();
    hasher.update(data);
    hasher.finalize()
}

/// Validates the the data with the given checksum
#[inline]
#[must_use]
pub fn validate_data<H: Digest>(data: &[u8], checksum: &[u8]) -> bool {
    AsRef::<[u8]>::as_ref(&get_checksum::<H>(data)) == checksum
}

/// Checks whether the file exist
#[inline]
#[must_use]
pub fn is_exist<A: AsRef<Path>>(path: A) -> bool {
    std::fs::metadata(path).is_ok()
}

/// Parses a u64 from u8 slice
///
/// # Panics
///
/// Panics if `bytes_le` is not exactly 8 bytes long
#[inline]
#[must_use]
pub fn parse_u64(bytes_le: &[u8]) -> u64 {
    assert_eq!(bytes_le.len(), 8, "The slice passed should be 8 bytes long");
    u64::from_le_bytes(
        bytes_le
            .try_into()
            .unwrap_or_else(|_| unreachable!("This conversion should always exist")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_rename_is_ok() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut path = PathBuf::from(tempdir.path());
        path.push("file.test");
        let lfile = LockedFile::open_rw(&path).unwrap();
        let new_name = "new_name.test";
        let mut new_path = parent_dir(&path);
        new_path.push(new_name);
        lfile.rename(new_name).unwrap();
        assert!(!is_exist(path));
        assert!(is_exist(new_path));
    }

    #[test]
    #[allow(clippy::verbose_file_reads)] // false positive
    fn file_open_is_exclusive() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut path = PathBuf::from(tempdir.path());
        path.push("file.test");
        let _lfile = LockedFile::open_rw(&path).unwrap();
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
