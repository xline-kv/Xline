use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::ExecuteError;
use std::hash::Hash;

/// Command to execute on the server side
#[async_trait]
pub trait Command: Sized + Sync + Send + DeserializeOwned + Serialize + Clone {
    /// K is used to tell confliction
    type K: Eq + Hash + Send + Sync + Clone + Serialize + DeserializeOwned;

    /// Execution result
    type ER: std::fmt::Debug + Send + Sync + Clone + Serialize + DeserializeOwned;

    /// Get keys of the command
    fn keys(&self) -> &[Self::K];

    /// Execute the command according to the executor
    #[inline]
    async fn execute<E>(&self, e: &E) -> Result<Self::ER, ExecuteError>
    where
        E: CommandExecutor<Self> + Send + Sync,
    {
        <E as CommandExecutor<Self>>::execute(e, self).await
    }
}

/// Command executor which actually executes the command.
/// It usually defined by the protocol user.
#[async_trait]
pub trait CommandExecutor<C>: Sync + Send + Clone
where
    C: Command,
{
    /// Create a new command executor
    fn new() -> Self;
    /// Execute the command
    async fn execute(&self, cmd: &C) -> Result<C::ER, ExecuteError>;
}
