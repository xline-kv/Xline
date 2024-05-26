use std::path::{Path, PathBuf};

/// Size in bytes per segment, default is 64MiB
const DEFAULT_INSERT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Size in bytes per segment, default is 1MiB
const DEFAULT_REMOVE_SEGMENT_SIZE: u64 = 1024 * 1024;

/// The config for WAL
#[derive(Debug, Clone)]
pub(crate) struct WALConfig {
    /// The path of insert wal directory
    pub(super) insert_dir: PathBuf,
    /// The path of remove wal directory
    pub(super) remove_dir: PathBuf,
    /// The maximum size of this segment
    ///
    /// NOTE: This is a soft limit, the actual size may larger than this
    pub(super) max_insert_segment_size: u64,
    /// The maximum size of this segment
    ///
    /// NOTE: This is a soft limit, the actual size may larger than this
    pub(super) max_remove_segment_size: u64,
}

impl WALConfig {
    #[allow(unused)]
    /// Creates a new `WALConfig`
    pub(crate) fn new(dir: impl AsRef<Path>) -> Self {
        let mut insert_dir: PathBuf = dir.as_ref().into();
        let mut remove_dir: PathBuf = dir.as_ref().into();
        insert_dir.push("insert");
        remove_dir.push("remove");
        Self {
            insert_dir,
            remove_dir,
            max_insert_segment_size: DEFAULT_INSERT_SEGMENT_SIZE,
            max_remove_segment_size: DEFAULT_REMOVE_SEGMENT_SIZE,
        }
    }

    /// Sets the `max_insert_segment_size`
    pub(crate) fn with_max_insert_segment_size(self, size: u64) -> Self {
        Self {
            insert_dir: self.insert_dir,
            remove_dir: self.remove_dir,
            max_remove_segment_size: self.max_remove_segment_size,
            max_insert_segment_size: size,
        }
    }

    /// Sets the `max_remove_segment_size`
    pub(crate) fn with_max_remove_segment_size(self, size: u64) -> Self {
        Self {
            insert_dir: self.insert_dir,
            remove_dir: self.remove_dir,
            max_insert_segment_size: self.max_insert_segment_size,
            max_remove_segment_size: size,
        }
    }
}
