#![allow(
    clippy::unnecessary_wraps,
    unreachable_pub,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing
)]

use std::{
    collections::HashMap,
    ffi::OsString,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use parking_lot::Mutex;

/// The mock file system
static FS: OnceLock<Mutex<Fs>> = OnceLock::new();

/// Performs an operation on the global `Fs` instance
fn with_fs<F, R>(op: F) -> R
where
    F: FnOnce(&mut Fs) -> R,
{
    let fs = FS.get_or_init(|| Mutex::new(Fs::default()));
    op(&mut fs.lock())
}

/// The mock file system
///
/// TODO: This currently doesn't create a tree structure for directories.
/// Future improvements could address this.
#[derive(Default)]
struct Fs {
    /// All files
    entries: HashMap<PathBuf, File>,
}

impl Fs {
    /// Creates a file
    fn create_file(&mut self, path: PathBuf, file: File) -> io::Result<()> {
        self.parent_should_exist(&path)?;
        #[allow(clippy::pattern_type_mismatch)]
        if self.entries.get(&path).is_some_and(File::is_dir) {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        let _ignore = self.entries.insert(path, file);

        Ok(())
    }

    /// Opens a file
    fn open_file(&self, path: &PathBuf) -> io::Result<File> {
        self.parent_should_exist(path)?;
        self.entries
            .get(path)
            .map(File::clone)
            .ok_or(io::Error::from(io::ErrorKind::NotFound))
    }

    /// Checks if the parent directory exists
    fn parent_should_exist(&self, path: &Path) -> io::Result<()> {
        let err = io::Error::from(io::ErrorKind::NotFound);
        let Some(parent) = path.parent() else {
            return Err(err);
        };
        self.entries.contains_key(parent).then_some(()).ok_or(err)
    }
}

/// An object providing access to an open file on the filesystem.
#[derive(Debug)]
pub struct File {
    /// Inner
    inner: Arc<Mutex<Inner>>,
    /// File offset
    offset: usize,
    /// The file type
    ftype: Ftype,
}

/// File data
#[derive(Debug, Default)]
struct Inner {
    /// Buffered data
    buffer: Vec<u8>,
    /// Synced data
    synced: Vec<u8>,
    /// Metadata of the file
    metadata: Metadata,
    /// Indicate the file is locked
    is_locked: bool,
}

/// The type of the file
#[derive(Clone, Copy, Debug)]
enum Ftype {
    /// A file
    File,
    /// A directory
    Dir,
}

impl File {
    /// Attempts to open a file in read-only mode.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<File> {
        open_file(path, false)
    }

    /// Opens a file in write-only mode.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<File> {
        open_file(path, true)
    }

    /// Attempts to sync all OS-internal metadata to disk.
    pub fn sync_all(&self) -> io::Result<()> {
        self.sync_data()
    }

    /// This function is similar to [`sync_all`], except that it might not
    /// synchronize file metadata to the filesystem.
    pub fn sync_data(&self) -> io::Result<()> {
        let mut inner = self.inner.lock();
        let buffer = inner.buffer.drain(..).collect::<Vec<_>>();
        inner.synced.extend(buffer);
        Ok(())
    }

    /// Queries metadata about the underlying file.
    pub fn metadata(&self) -> io::Result<Metadata> {
        Ok(self.inner.lock().metadata.clone())
    }

    /// Creates a new file
    fn new_file() -> Self {
        Self {
            inner: Arc::default(),
            offset: 0,
            ftype: Ftype::File,
        }
    }

    /// Creates a new directory
    fn new_dir() -> Self {
        Self {
            inner: Arc::default(),
            offset: 0,
            ftype: Ftype::Dir,
        }
    }

    /// Clone the file pointer
    fn clone(&self) -> Self {
        File {
            inner: Arc::clone(&self.inner),
            offset: 0,
            ftype: self.ftype,
        }
    }

    /// Returns the type of this file
    fn is_file(&self) -> bool {
        matches!(self.ftype, Ftype::File)
    }

    /// Returns the type of this file
    fn is_dir(&self) -> bool {
        matches!(self.ftype, Ftype::Dir)
    }

    /// Returns the type of this file
    fn ftype(&self) -> Ftype {
        self.ftype
    }
}

impl Drop for File {
    fn drop(&mut self) {
        self.inner.lock().is_locked = false;
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let inner = self.inner.lock();
        let total = inner.synced.len() + inner.buffer.len();
        if self.offset == total {
            return Ok(0);
        }
        let to_copy = (total - self.offset).min(buf.len());
        let tmp: Vec<_> = inner
            .synced
            .iter()
            .chain(inner.buffer.iter())
            .skip(self.offset)
            .take(to_copy)
            .copied()
            .collect();
        buf[..to_copy].copy_from_slice(&tmp);
        self.offset += to_copy;
        Ok(to_copy)
    }
}

impl Write for File {
    #[allow(clippy::as_conversions)]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut inner = self.inner.lock();
        inner.buffer.extend_from_slice(buf);
        self.offset += buf.len();
        if self.offset as u64 > inner.metadata.len {
            inner.metadata.len = self.offset as u64;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[allow(clippy::unimplemented)]
impl fs2::FileExt for File {
    fn duplicate(&self) -> io::Result<std::fs::File> {
        unimplemented!();
    }

    fn allocated_size(&self) -> io::Result<u64> {
        unimplemented!();
    }

    #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
    fn allocate(&self, len: u64) -> io::Result<()> {
        let mut inner = self.inner.lock();
        inner.metadata.len += len;

        Ok(())
    }

    fn lock_shared(&self) -> io::Result<()> {
        unimplemented!();
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        unimplemented!();
    }

    fn try_lock_shared(&self) -> io::Result<()> {
        unimplemented!();
    }

    fn try_lock_exclusive(&self) -> io::Result<()> {
        let mut inner = self.inner.lock();
        if inner.is_locked {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
        inner.is_locked = true;
        Ok(())
    }

    fn unlock(&self) -> io::Result<()> {
        unimplemented!();
    }
}

/// Iterator over the entries in a directory.
pub struct ReadDir {
    /// Entries
    entries: Box<dyn Iterator<Item = PathBuf>>,
}

impl Iterator for ReadDir {
    type Item = io::Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(DirEntry).map(Ok)
    }
}

/// Entries returned by the [`ReadDir`] iterator.
pub struct DirEntry(PathBuf);

impl DirEntry {
    /// Returns the full path to the file that this entry represents.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.0.clone()
    }

    /// Returns the file name of this directory entry without any
    /// leading path component(s).
    #[must_use]
    pub fn file_name(&self) -> OsString {
        #[allow(clippy::unwrap_used)]
        self.0.file_name().unwrap().to_os_string()
    }
}

/// Metadata information about a file.
#[derive(Debug, Default, Clone)]
pub struct Metadata {
    /// The length of the file
    len: u64,
}

impl Metadata {
    /// Returns the size of the file, in bytes, this metadata is for.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }
}

/// Options and flags which can be used to configure how a file is opened.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    /// Open mode
    mode: u8,
}

impl OpenOptions {
    /// Creates a blank new set of options ready for configuration.
    #[must_use]
    pub fn new() -> Self {
        OpenOptions { mode: 0 }
    }

    /// Sets the option for read access.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.mode |= 1;
        self
    }

    /// Sets the option for write access.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.mode |= 1 << 1;
        self
    }

    /// Sets the option to create a new file, or open it if it already exists.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.mode |= 1 << 2;
        self
    }

    /// Opens a file at `path` with the options specified by `self`.
    pub fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        open_file(path, (self.mode >> 2 & 1) != 0)
    }
}

/// Recursively create a directory and all of its parent components if they
/// are missing.
pub fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut path = path.as_ref().to_path_buf();
    assert!(path.is_absolute(), "relative path not supported yet");
    with_fs(|fs| {
        while {
            let _ignore = fs.entries.entry(path.clone()).or_insert_with(File::new_dir);
            path.pop()
        } {}
    });
    Ok(())
}

/// Rename a file or directory to a new name, replacing the original file if
/// `to` already exists.
pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<()> {
    let from = from.as_ref().to_path_buf();
    let to = to.as_ref().to_path_buf();
    with_fs(|fs| {
        let f = fs
            .entries
            .remove(&from)
            .ok_or(io::Error::from(io::ErrorKind::NotFound))?;
        let _ignore = fs.entries.insert(to, f);
        Ok(())
    })
}

/// Removes a file from the filesystem.
pub fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref().to_path_buf();
    with_fs(|fs| fs.entries.remove(&path).map(|_| ()))
        .ok_or(io::Error::from(io::ErrorKind::NotFound))
}

/// Returns an iterator over the entries within a directory.
pub fn read_dir<P: AsRef<Path>>(path: P) -> io::Result<ReadDir> {
    let entries = path.as_ref();
    let entries = with_fs(|fs| {
        fs.entries
            .keys()
            .filter(|p| p.parent().is_some_and(|pp| pp == entries))
            .map(PathBuf::clone)
            .collect::<Vec<_>>()
    });
    Ok(ReadDir {
        entries: Box::new(entries.into_iter()),
    })
}

/// Given a path, query the file system to get information about a file,
/// directory, etc.
pub fn metadata<P: AsRef<Path>>(path: P) -> io::Result<Metadata> {
    let path = path.as_ref().to_path_buf();
    with_fs(|fs| fs.entries.get(&path).map(File::metadata))
        .ok_or(io::Error::from(io::ErrorKind::NotFound))?
}

/// Adds a new file to the fs
fn open_file(path: impl AsRef<Path>, create: bool) -> io::Result<File> {
    let path = path.as_ref().to_path_buf();
    with_fs(|fs| {
        if create {
            if let Ok(file) = fs.open_file(&path) {
                return Ok(file);
            }
            let file = File::new_file();
            fs.create_file(path, file.clone())?;
            Ok(file)
        } else {
            fs.open_file(&path)
        }
    })
}

#[cfg(test)]
mod test {
    use fs2::FileExt;

    use super::*;
    use std::fs;

    #[test]
    #[allow(clippy::verbose_file_reads)]
    fn file_read_write_is_ok() {
        create_dir_all("/tmp").unwrap();
        let path = PathBuf::from("/tmp/test_file_read_write_is_ok");
        let mut mock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        mock_file.write_all(&[1, 2, 3]).unwrap();
        file.write_all(&[1, 2, 3]).unwrap();

        assert_eq!(file.read_to_end(&mut Vec::new()).unwrap(), 0);
        assert_eq!(mock_file.read_to_end(&mut Vec::new()).unwrap(), 0);

        let mut mock_file = OpenOptions::new().read(true).open(&path).unwrap();
        let mut file = fs::OpenOptions::new().read(true).open(&path).unwrap();
        let mut buf0 = Vec::new();
        let mut buf1 = Vec::new();
        file.read_to_end(&mut buf0).unwrap();
        mock_file.read_to_end(&mut buf1).unwrap();
        assert_eq!(buf0, buf1);

        assert_eq!(
            metadata(&path).unwrap().len(),
            fs::metadata(&path).unwrap().len()
        );

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn read_dir_is_ok() {
        create_dir_all("/a/b/c").unwrap();
        File::create("/a/b/1").unwrap();
        File::create("/a/b/2").unwrap();
        let names: Vec<_> = read_dir("/a/b")
            .unwrap()
            .into_iter()
            .flatten()
            .map(|d| d.file_name().into_string().unwrap())
            .collect();
        assert_eq!(names.len(), 3);
        names.contains(&"c".to_string());
        names.contains(&"1".to_string());
        names.contains(&"2".to_string());
    }

    #[test]
    fn dir_rename_is_ok() {
        create_dir_all("/a/b/c").unwrap();
        File::create("/a/b/1").unwrap();
        rename("/a/b/c", "/a/b/d").unwrap();
        rename("/a/b/1", "/a/b/3").unwrap();

        let names: Vec<_> = read_dir("/a/b")
            .unwrap()
            .into_iter()
            .flatten()
            .map(|d| d.file_name().into_string().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        names.contains(&"d".to_string());
        names.contains(&"3".to_string());
    }

    #[test]
    fn detect_file_exist_is_ok() {
        assert!(metadata("/a/b").is_err());
    }

    #[test]
    fn open_files_only_when_parent_dir_exists() {
        assert!(File::create("/a/1").is_err());
        assert!(File::open("/a/1").is_err());
        assert!(fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/a/1")
            .is_err());
    }

    #[test]
    fn file_size_is_corrent() {
        create_dir_all("/a/b").unwrap();
        let mut file = File::create("/a/b/1").unwrap();
        assert_eq!(file.metadata().unwrap().len(), 0);
        file.allocate(128);
        assert_eq!(file.metadata().unwrap().len(), 128);
        file.write_all(&[0; 256]).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 256);
        file.sync_all();
        assert_eq!(file.metadata().unwrap().len(), 256);
        file.allocate(128);
        assert_eq!(file.metadata().unwrap().len(), 384);
    }
}
