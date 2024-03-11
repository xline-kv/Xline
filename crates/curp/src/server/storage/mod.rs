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
    #[error("rocksdb error, {0}")]
    RocksDB(#[from] EngineError),
    /// WAL error
    #[error("wal error, {0}")]
    WAL(#[from] std::io::Error),
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

/// Vote info
pub(crate) type VoteInfo = (u64, ServerId);
/// Recovered data
pub(crate) type RecoverData<C> = (Option<VoteInfo>, Vec<LogEntry<C>>);

/// Curp storage api
#[allow(clippy::module_name_repetitions)]
pub trait StorageApi: Send + Sync {
    /// Command
    type Command: Command;

    /// Put `voted_for` into storage, must be flushed on disk before returning
    ///
    /// # Errors
    /// Return `StorageError` when it failed to store the `voted_for` info to underlying database.
    fn flush_voted_for(&self, term: u64, voted_for: ServerId) -> Result<(), StorageError>;

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
    /// Return `StorageError` when it failed to store the log entries to underlying database.
    fn put_log_entries(&self, entry: &[&LogEntry<Self::Command>]) -> Result<(), StorageError>;

    /// Recover from persisted storage
    /// Return `voted_for` and all log entries
    ///
    /// # Errors
    /// Return `StorageError` when it failed to recover the log entries and vote info from underlying database.
    fn recover(&self) -> Result<RecoverData<Self::Command>, StorageError>;
}

/// CURP `DB` storage implementation
pub(super) mod db;

/// CURP WAL storage implementation
pub(super) mod wal;
