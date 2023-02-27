use std::error::Error;

use async_trait::async_trait;

use crate::{cmd::Command, log_entry::LogEntry, message::ServerId};

/// Curp storage api
#[async_trait]
pub(super) trait StorageApi: Send + Sync {
    /// Command
    type Command: Command;

    /// Put `voted_for` in storage, must be flushed on disk before returning
    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), Box<dyn Error>>;

    /// Put log entries in storage
    async fn put_log_entry(&self, entry: LogEntry<Self::Command>) -> Result<(), Box<dyn Error>>;

    /// Recover from persisted storage
    /// Return `voted_for` and all log entries
    async fn recover(
        &self,
    ) -> Result<(Option<(u64, ServerId)>, Vec<LogEntry<Self::Command>>), Box<dyn Error>>;
}

/// `RocksDB` storage implementation
pub(super) mod rocksdb;
