use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::error::ExecuteError;
use std::hash::Hash;

/// Command to execute on the server side
#[async_trait]
pub trait Command:
    Sized + Sync + Send + DeserializeOwned + Serialize + std::fmt::Debug + Clone + ConflictCheck
{
    /// K (key) is used to tell confliction
    /// The key can be a single key or a key range
    type K: Eq + Hash + Send + Sync + Clone + Serialize + DeserializeOwned + ConflictCheck;

    /// Execution result
    type ER: std::fmt::Debug + Send + Sync + Clone + Serialize + DeserializeOwned;

    /// Get keys of the command
    fn keys(&self) -> &[Self::K];

    /// Get propose id
    fn id(&self) -> &ProposeId;

    /// Execute the command according to the executor
    #[inline]
    async fn execute<E>(&self, e: &E) -> Result<Self::ER, ExecuteError>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::execute(e, self).await
    }
}

/// Command Id wrapper, abstracting underlying implementation
#[allow(clippy::module_name_repetitions)] // the name is ok even with repetitions
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ProposeId(String);

impl ProposeId {
    /// Create a new propose id
    #[inline]
    #[must_use]
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

/// Check conflict of two keys
pub trait ConflictCheck {
    /// check if this keys conflicts with the `other` key
    ///
    /// Return：
    ///     - True: conflict
    ///     - False: not conflit
    fn is_conflict(&self, other: &Self) -> bool;
}

impl ConflictCheck for String {
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        self == other
    }
}

/// Command executor which actually executes the command.
/// It usually defined by the protocol user.
#[async_trait]
pub trait CommandExecutor<C>: Sync + Send + Clone + std::fmt::Debug
where
    C: Command,
{
    /// Execute the command
    async fn execute(&self, cmd: &C) -> Result<C::ER, ExecuteError>;
}
