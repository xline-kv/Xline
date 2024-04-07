use async_trait::async_trait;
use engine::EngineError;
use thiserror::Error;

use crate::{
    cmd::Command,
    log_entry::LogEntry,
    members::{ClusterInfo, ServerId},
    rpc::Member,
};

/// Storage layer error
#[derive(Error, Debug)]
#[allow(clippy::module_name_repetitions)]
#[non_exhaustive]
pub enum StorageError {
    /// Serialize or deserialize error
    #[error("codec error, {0}")]
    Codec(String),
    /// Rocksdb error
    #[error("internal error, {0}")]
    Internal(#[from] EngineError),
}

impl From<bincode::Error> for StorageError {
    #[inline]
    fn from(e: bincode::Error) -> Self {
        Self::Codec(e.to_string())
    }
}

impl From<prost::DecodeError> for StorageError {
    #[inline]
    fn from(e: prost::DecodeError) -> Self {
        Self::Codec(e.to_string())
    }
}

/// Curp storage api
#[async_trait]
#[allow(clippy::module_name_repetitions)]
pub trait StorageApi: Send + Sync {
    /// Command
    type Command: Command;

    /// Put `voted_for` into storage, must be flushed on disk before returning
    ///
    /// # Errors
    /// Return `StorageError` when it failed to store the `voted_for` info to underlying database.
    async fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError>;

    /// Put `Member` into storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to store the member info to underlying database.
    fn put_member(&self, member: &Member) -> Result<(), StorageError>;

    /// Remove `Member` from storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to remove the member info from underlying database.
    fn remove_member(&self, id: ServerId) -> Result<(), StorageError>;

    /// Put `ClusterInfo` into storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to store the cluster info to underlying database.
    fn put_cluster_info(&self, cluster_info: &ClusterInfo) -> Result<(), StorageError>;

    /// Recover `ClusterInfo` from storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to recover the cluster info from underlying database.
    fn recover_cluster_info(&self) -> Result<Option<ClusterInfo>, StorageError>;

    /// Put log entries in storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to store the given log entry info to underlying database.
    async fn put_log_entry(&self, entry: &LogEntry<Self::Command>) -> Result<(), StorageError>;

    /// Recover from persisted storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to recover from underlying database. Otherwise, return recovered `voted_for` and all log entries
    async fn recover(
        &self,
    ) -> Result<(Option<(u64, ServerId)>, Vec<LogEntry<Self::Command>>), StorageError>;
}

/// CURP `DB` storage implementation
pub(super) mod db;

/// CURP WAL storage implementation
pub(super) mod wal;
