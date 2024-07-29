use std::path::{Path, PathBuf};

/// Size in bytes per segment, default is 64MiB
const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// The config for WAL
#[derive(Debug, Clone)]
pub(crate) enum WALConfig {
    /// Persistent implementation
    Persistent(PersistentConfig),
    /// Mock memory implementation
    Memory,
}

/// The config for persistent WAL
#[derive(Debug, Clone)]
pub(crate) struct PersistentConfig {
    /// The path of this config
    pub(super) dir: PathBuf,
    /// The maximum size of this segment
    ///
    /// NOTE: This is a soft limit, the actual size may larger than this
    pub(super) max_segment_size: u64,
}

impl WALConfig {
    /// Creates a new `WALConfig`
    pub(crate) fn new(dir: impl AsRef<Path>) -> Self {
        Self::Persistent(PersistentConfig {
            dir: dir.as_ref().into(),
            max_segment_size: DEFAULT_SEGMENT_SIZE,
        })
    }

    /// Creates a new memory `WALConfig`
    pub(crate) fn new_memory() -> Self {
        Self::Memory
    }

    /// Sets the `max_segment_size`
    pub(crate) fn with_max_segment_size(self, size: u64) -> Self {
        match self {
            Self::Persistent(PersistentConfig {
                dir,
                max_segment_size,
            }) => Self::Persistent(PersistentConfig {
                dir,
                max_segment_size: size,
            }),
            Self::Memory => Self::Memory,
        }
    }
}
