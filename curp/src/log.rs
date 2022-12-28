use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{cmd::Command, message::TermNum};

/// Log entry
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct LogEntry<C> {
    /// Term id
    term: TermNum,
    /// Commands
    cmds: Arc<[Arc<C>]>,
}

impl<C: Command> LogEntry<C> {
    /// Create a new `LogEntry`
    pub(crate) fn new(term: TermNum, cmds: &[Arc<C>]) -> Self {
        Self {
            term,
            cmds: cmds.into(),
        }
    }

    /// Get term id
    pub(crate) fn term(&self) -> TermNum {
        self.term
    }

    /// Get command in the entry
    pub(crate) fn cmds(&self) -> &[Arc<C>] {
        &self.cmds
    }
}
