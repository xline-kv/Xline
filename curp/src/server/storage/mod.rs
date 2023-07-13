use async_trait::async_trait;
use engine::EngineError;
use thiserror::Error;

use crate::{cmd::Command, log_entry::LogEntry, ServerId};

/// Storage layer error
#[derive(Error, Debug)]
pub(super) enum StorageError {
    /// Serialize or deserialize error
    #[error("bincode error, {0}")]
    Bincode(#[from] bincode::Error),
    /// Rocksdb error
    #[error("internal error, {0}")]
    Internal(#[from] EngineError),
}

/// Curp storage api
#[async_trait]
pub(super) trait StorageApi: Send + Sync {
    /// Command
    type Command: Command;

    /// Put `voted_for` in storage, must be flushed on disk before returning
    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError>;

    /// Put log entries in storage
    async fn put_log_entry(&self, entry: LogEntry<Self::Command>) -> Result<(), StorageError>;

    /// Recover from persisted storage
    /// Return `voted_for` and all log entries
    async fn recover(
        &self,
    ) -> Result<(Option<(u64, ServerId)>, Vec<LogEntry<Self::Command>>), StorageError>;
}

/// CURP `DB` storage implementation
pub(super) mod db;
