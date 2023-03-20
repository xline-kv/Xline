use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Log Index
pub type LogIndex = u64;

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LogEntry<C> {
    /// Term
    pub(crate) term: u64,
    /// Index
    pub(crate) index: LogIndex,
    /// The Command
    pub(crate) cmd: Arc<C>,
}

impl<C> LogEntry<C> {
    /// Create a new `LogEntry`
    pub(super) fn new(index: LogIndex, term: u64, cmd: Arc<C>) -> Self {
        Self { term, index, cmd }
    }
}
