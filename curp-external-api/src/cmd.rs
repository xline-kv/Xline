use std::{fmt::Display, hash::Hash};

use async_trait::async_trait;
use engine::Snapshot;
use prost::DecodeError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::LogIndex;

/// Command to execute on the server side
#[async_trait]
pub trait Command:
    Sync + Send + DeserializeOwned + Serialize + std::fmt::Debug + Clone + ConflictCheck + PbCodec
{
    /// Error type
    type Error: Send + Sync + Clone + std::error::Error + Serialize + DeserializeOwned + PbCodec;

    /// K (key) is used to tell confliction
    /// The key can be a single key or a key range
    type K: std::fmt::Debug
        + Eq
        + Hash
        + Send
        + Sync
        + Clone
        + Serialize
        + DeserializeOwned
        + ConflictCheck;

    /// Prepare result
    type PR: std::fmt::Debug + Send + Sync + Clone + Serialize + DeserializeOwned;

    /// Execution result
    type ER: std::fmt::Debug + Send + Sync + Clone + Serialize + DeserializeOwned + PbCodec;

    /// After_sync result
    type ASR: std::fmt::Debug + Send + Sync + Clone + Serialize + DeserializeOwned + PbCodec;

    /// Get keys of the command
    fn keys(&self) -> &[Self::K];

    /// Get propose id
    fn id(&self) -> ProposeId;

    /// Prepare the command
    #[inline]
    fn prepare<E>(&self, e: &E, index: LogIndex) -> Result<Self::PR, Self::Error>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::prepare(e, self, index)
    }

    /// Execute the command according to the executor
    #[inline]
    async fn execute<E>(&self, e: &E, index: LogIndex) -> Result<Self::ER, Self::Error>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::execute(e, self, index).await
    }

    /// Execute the command after_sync callback
    #[inline]
    async fn after_sync<E>(
        &self,
        e: &E,
        index: LogIndex,
        prepare_res: Self::PR,
    ) -> Result<Self::ASR, Self::Error>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::after_sync(e, self, index, prepare_res).await
    }
}

/// Command Id wrapper, which is used to identify a command
/// The underlying data is a tuple of (`client_id`, `seq_num`)
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd, Default,
)]
#[allow(clippy::exhaustive_structs)] // It is exhaustive
pub struct ProposeId(pub u64, pub u64);

impl Display for ProposeId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.0, self.1)
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
/// It is usually defined by the protocol user.
#[async_trait]
pub trait CommandExecutor<C>: Sync + Send + std::fmt::Debug
where
    C: Command,
{
    /// Prepare the command
    fn prepare(&self, cmd: &C, index: LogIndex) -> Result<C::PR, C::Error>;

    /// Commit the prepare
    fn prepare_commit(&self, cmd: &C, index: LogIndex) -> Result<C::PR, C::Error>;

    /// Reset the prepare
    fn prepare_reset(&self);

    /// Execute the command
    async fn execute(&self, cmd: &C, index: LogIndex) -> Result<C::ER, C::Error>;

    /// Execute the after_sync callback
    async fn after_sync(
        &self,
        cmd: &C,
        index: LogIndex,
        prepare_res: C::PR,
    ) -> Result<C::ASR, C::Error>;

    /// Set the index of the last log entry that has been successfully applied to the command executor
    fn set_last_applied(&self, index: LogIndex) -> Result<(), C::Error>;

    /// Index of the last log entry that has been successfully applied to the command executor
    fn last_applied(&self) -> Result<LogIndex, C::Error>;

    /// Take a snapshot
    async fn snapshot(&self) -> Result<Snapshot, C::Error>;

    /// Reset the command executor using the snapshot or to the initial state if None
    async fn reset(&self, snapshot: Option<(Snapshot, LogIndex)>) -> Result<(), C::Error>;
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
