use engine::EngineError;
use thiserror::Error;

use crate::{cmd::Command, log_entry::LogEntry, member::MembershipState, members::ServerId};

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
/// Speculative pool version
pub(crate) type SpVersion = u64;
/// Recovered data
pub(crate) type RecoverData<C> = (Option<VoteInfo>, Vec<LogEntry<C>>, SpVersion);

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

    /// Put membership into the persisted storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to store the membership to underlying database.
    fn put_membership(
        &self,
        node_id: u64,
        membership: &MembershipState,
    ) -> Result<(), StorageError>;

    /// Recovers membership from the persisted storage
    ///
    /// # Errors
    /// Return `StorageError` when it failed to recover the membership from underlying database.
    fn recover_membership(&self) -> Result<Option<(u64, MembershipState)>, StorageError>;
}

/// CURP `DB` storage implementation
pub(super) mod db;

/// CURP WAL storage implementation
pub(super) mod wal;
