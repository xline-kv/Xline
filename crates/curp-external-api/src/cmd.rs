use std::{fmt::Debug, hash::Hash};

use async_trait::async_trait;
use engine::Snapshot;
use prost::DecodeError;
use serde::{de::DeserializeOwned, Serialize};

use crate::{InflightId, LogIndex};

/// Private
mod pri {
    use super::{Debug, DeserializeOwned, Serialize};

    /// Trait bound for thread safety
    #[allow(unreachable_pub)]
    pub trait ThreadSafe: Debug + Send + Sync + 'static {}

    /// Trait bound for serializable
    #[allow(unreachable_pub)]
    pub trait Serializable: ThreadSafe + Clone + Serialize + DeserializeOwned {}
}

impl<T> pri::ThreadSafe for T where T: Debug + Send + Sync + 'static {}

impl<T> pri::Serializable for T where T: pri::ThreadSafe + Clone + Serialize + DeserializeOwned {}

/// Command to execute on the server side
#[async_trait]
pub trait Command: pri::Serializable + ConflictCheck + PbCodec {
    /// Error type
    type Error: pri::Serializable + PbCodec + std::error::Error + Clone;

    /// K (key) is used to tell confliction
    ///
    /// The key can be a single key or a key range
    type K: pri::Serializable + Eq + Hash + ConflictCheck;

    /// Prepare result
    type PR: pri::Serializable;

    /// Execution result
    type ER: pri::Serializable + PbCodec;

    /// After_sync result
    type ASR: pri::Serializable + PbCodec;

    /// Get keys of the command
    fn keys(&self) -> Vec<Self::K>;

    /// Returns `true` if the command is read-only
    fn is_read_only(&self) -> bool;

    /// Execute the command according to the executor
    ///
    /// # Errors
    ///
    /// Return `Self::Error` when `CommandExecutor::execute` goes wrong
    #[inline]
    fn execute<E>(&self, e: &E) -> Result<Self::ER, Self::Error>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::execute(e, self)
    }
}

/// Check conflict of two keys
pub trait ConflictCheck {
    /// check if this keys conflicts with the `other` key
    ///
    /// Return：
    ///     - True: conflict
    ///     - False: not conflict
    fn is_conflict(&self, other: &Self) -> bool;
}

impl ConflictCheck for String {
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        self == other
    }
}

impl ConflictCheck for u32 {
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        self == other
    }
}

/// Command executor which actually executes the command.
///
/// It is usually defined by the protocol user.
#[async_trait]
pub trait CommandExecutor<C>: pri::ThreadSafe
where
    C: Command,
{
    /// Execute the command
    ///
    /// # Errors
    ///
    /// This function may return an error if there is a problem executing the
    /// command.
    fn execute(&self, cmd: &C) -> Result<C::ER, C::Error>;

    /// Execute the read-only command
    ///
    /// # Errors
    ///
    /// This function may return an error if there is a problem executing the
    /// command.
    fn execute_ro(&self, cmd: &C) -> Result<(C::ER, C::ASR), C::Error>;

    /// Batch execute the after_sync callback
    ///
    /// This `highest_index` means the last log index of the `cmds`
    fn after_sync(
        &self,
        cmds: Vec<AfterSyncCmd<'_, C>>,
        // might be `None` if it's a speculative execution
        highest_index: Option<LogIndex>,
    ) -> Vec<Result<AfterSyncOk<C>, C::Error>>;

    /// Set the index of the last log entry that has been successfully applied
    /// to the command executor
    ///
    /// # Errors
    ///
    /// Returns an error if setting the last applied log entry fails.
    fn set_last_applied(&self, index: LogIndex) -> Result<(), C::Error>;

    /// Get the index of the last log entry that has been successfully applied
    /// to the command executor
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval of the last applied log entry fails.
    fn last_applied(&self) -> Result<LogIndex, C::Error>;

    /// Take a snapshot
    ///
    /// # Errors
    ///
    /// This function may return an error if there is a problem taking a
    /// snapshot.
    async fn snapshot(&self) -> Result<Snapshot, C::Error>;

    /// Reset the command executor using the snapshot or to the initial state if
    /// None
    ///
    /// # Errors
    ///
    /// This function may return an error if there is a problem resetting the
    /// command executor.
    async fn reset(&self, snapshot: Option<(Snapshot, LogIndex)>) -> Result<(), C::Error>;

    /// Trigger the barrier of the given trigger id (based on propose id) and
    /// log index.
    fn trigger(&self, id: InflightId);
}

/// Codec for encoding and decoding data into/from the Protobuf format
pub trait PbCodec: Sized {
    /// Encode
    fn encode(&self) -> Vec<u8>;
    /// Decode
    ///
    /// # Errors
    ///
    /// Returns an error if decode failed
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError>;
}

/// Serialize error used in `PbSerialize`
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PbSerializeError {
    /// Error during decode phase
    #[error("rpc decode error: {0}")]
    RpcDecode(DecodeError),
    /// Error if the required field is empty
    #[error("field is empty after decoded")]
    EmptyField,
}

impl From<DecodeError> for PbSerializeError {
    #[inline]
    fn from(err: DecodeError) -> Self {
        PbSerializeError::RpcDecode(err)
    }
}

#[allow(clippy::module_name_repetitions)]
/// After sync command type
#[derive(Debug)]
pub struct AfterSyncCmd<'a, C> {
    /// The command
    cmd: &'a C,
    /// Whether the command needs to be executed in after sync stage
    to_execute: bool,
}

impl<'a, C> AfterSyncCmd<'a, C> {
    /// Creates a new `AfterSyncCmd`
    #[inline]
    pub fn new(cmd: &'a C, to_execute: bool) -> Self {
        Self { cmd, to_execute }
    }

    /// Gets the command
    #[inline]
    #[must_use]
    pub fn cmd(&self) -> &'a C {
        self.cmd
    }

    /// Convert self into parts
    #[inline]
    #[must_use]
    pub fn into_parts(&'a self) -> (&'a C, bool) {
        (self.cmd, self.to_execute)
    }
}

/// Ok type of the after sync result
#[derive(Debug)]
pub struct AfterSyncOk<C: Command> {
    /// After Sync Result
    asr: C::ASR,
    /// Optional Execution Result
    er_opt: Option<C::ER>,
}

impl<C: Command> AfterSyncOk<C> {
    /// Creates a new [`AfterSyncOk<C>`].
    #[inline]
    pub fn new(asr: C::ASR, er_opt: Option<C::ER>) -> Self {
        Self { asr, er_opt }
    }

    /// Decomposes `AfterSyncOk` into its constituent parts.
    #[inline]
    pub fn into_parts(self) -> (C::ASR, Option<C::ER>) {
        let Self { asr, er_opt } = self;
        (asr, er_opt)
    }
}
