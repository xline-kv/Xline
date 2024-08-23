#![allow(unused)] // TODO: remove this until used

/// The WAL codec
pub(super) mod codec;

/// The config for `WALStorage`
pub(super) mod config;

/// WAL errors
mod error;

/// File pipeline
mod pipeline;

/// Remover of the segment file
mod remover;

/// WAL segment
mod segment;

/// WAL test utils
#[cfg(test)]
mod test_util;

/// WAL storage tests
#[cfg(test)]
mod tests;

/// File utils
mod util;

/// Framed
mod framed;

/// Mock WAL storage
mod mock;

/// WAL storage
mod storage;

use std::io;

use codec::DataFrame;
use config::WALConfig;
use curp_external_api::LogIndex;
use serde::{de::DeserializeOwned, Serialize};

use crate::log_entry::LogEntry;

/// The wal file extension
const WAL_FILE_EXT: &str = ".wal";

/// Operations of a WAL storage
pub(crate) trait WALStorageOps<C> {
    /// Recover from the given directory if there's any segments
    fn recover(&mut self) -> io::Result<Vec<LogEntry<C>>>;

    /// Send frames with fsync
    fn send_sync(&mut self, item: Vec<DataFrame<'_, C>>) -> io::Result<()>;

    /// Tuncate all the logs whose index is less than or equal to `compact_index`
    ///
    /// `compact_index` should be the smallest index required in CURP
    fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()>;

    /// Tuncate all the logs whose index is greater than `max_index`
    fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()>;
}

/// The WAL Storage
#[derive(Debug)]
pub(crate) enum WALStorage<C> {
    /// Persistent storage
    Persistent(storage::WALStorage<C>),
    /// Mock memory storage
    Memory(mock::WALStorage<C>),
}

impl<C> WALStorage<C> {
    /// Creates a new `WALStorage`
    pub(crate) fn new(config: WALConfig) -> io::Result<Self> {
        Ok(match config {
            WALConfig::Persistent(conf) => Self::Persistent(storage::WALStorage::new(conf)?),
            WALConfig::Memory => Self::Memory(mock::WALStorage::new()),
        })
    }
}

impl<C> WALStorageOps<C> for WALStorage<C>
where
    C: Serialize + DeserializeOwned + std::fmt::Debug + Clone,
{
    fn recover(&mut self) -> io::Result<Vec<LogEntry<C>>> {
        match *self {
            WALStorage::Persistent(ref mut s) => s.recover(),
            WALStorage::Memory(ref mut s) => s.recover(),
        }
    }

    fn send_sync(&mut self, item: Vec<DataFrame<'_, C>>) -> io::Result<()> {
        match *self {
            WALStorage::Persistent(ref mut s) => s.send_sync(item),
            WALStorage::Memory(ref mut s) => s.send_sync(item),
        }
    }

    fn truncate_head(&mut self, compact_index: LogIndex) -> io::Result<()> {
        match *self {
            WALStorage::Persistent(ref mut s) => s.truncate_head(compact_index),
            WALStorage::Memory(ref mut s) => s.truncate_head(compact_index),
        }
    }

    fn truncate_tail(&mut self, max_index: LogIndex) -> io::Result<()> {
        match *self {
            WALStorage::Persistent(ref mut s) => s.truncate_tail(max_index),
            WALStorage::Memory(ref mut s) => s.truncate_tail(max_index),
        }
    }
}
