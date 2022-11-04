use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{cmd::Command, message::TermNum};

/// Log entry status
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) enum EntryStatus {
    /// The entry has not synced
    #[allow(dead_code)]
    UnSynced,
    /// The entry has been synced to the majority to the
    #[allow(dead_code)]
    Synced,
}

/// Log entry
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct LogEntry<C: Command> {
    /// Term id
    #[allow(dead_code)]
    pub(crate) term: TermNum,
    /// Commands
    #[allow(dead_code)]
    pub(crate) cmds: Arc<[Arc<C>]>,
    /// Log entry status
    #[allow(dead_code)]
    pub(crate) status: EntryStatus,
}

// struct CmdVisitor;
// impl<'de, C: Command> Visitor<'de> for CmdVisitor {
//     type Value = LogEntry<C>;
//
//     fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//         formatter.write_str("LogEntry")
//     }
// }
//
// impl<'de, C: Command> de::Deserialize<'de> for LogEntry<C> {
//     fn deserialize<D>(deserializer: D) -> Result<Self, dyn de::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         deserializer.deserialize_map(CmdVisitor)
//     }
// }

impl<C: Command> LogEntry<C> {
    /// Create a new `LogEntry`
    pub(crate) fn new(term: TermNum, cmds: &[Arc<C>], status: EntryStatus) -> Self {
        Self {
            term,
            cmds: cmds.into(),
            status,
        }
    }

    /// Get term id
    #[allow(dead_code)]
    pub(crate) fn term(&self) -> TermNum {
        self.term
    }

    /// Get command in the entry
    #[allow(dead_code)]
    pub(crate) fn cmds(&self) -> &[Arc<C>] {
        &self.cmds
    }

    /// Get status in the entry
    #[allow(dead_code)]
    pub(crate) fn status(&self) -> &EntryStatus {
        &self.status
    }

    /// Set entry status
    #[allow(dead_code)]
    pub(crate) fn set_status(&mut self, status: EntryStatus) {
        self.status = status;
    }
}
