use std::sync::Arc;

use curp_external_api::{
    cmd::{Command, ProposeId},
    LogIndex,
};
use serde::{Deserialize, Serialize};

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LogEntry<C> {
    /// Term
    pub(crate) term: u64,
    /// Index
    pub(crate) index: LogIndex,
    /// Entry data
    pub(crate) entry_data: EntryData<C>,
}

/// Entry data of a `LogEntry`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum EntryData<C> {
    /// `Command` entry
    Command(Arc<C>),
    /// `ConfChange` entry
    ConfChange(ProposeId),
    /// `Shutdown` entry
    Shutdown,
}

impl<C> LogEntry<C>
where
    C: Command,
{
    /// Create a new `LogEntry` of a `Command`
    pub(super) fn new_cmd(index: LogIndex, term: u64, cmd: Arc<C>) -> Self {
        Self {
            term,
            index,
            entry_data: EntryData::Command(cmd),
        }
    }

    /// Create a new `LogEntry` of `Shutdown`
    pub(super) fn new_shutdown(index: LogIndex, term: u64) -> Self {
        Self {
            term,
            index,
            entry_data: EntryData::Shutdown,
        }
    }

    /// Create a new `LogEntry` of `ConfChange`
    #[allow(dead_code)] // TODO: remove this when we implement conf change
    pub(super) fn new_conf_change(index: LogIndex, term: u64, id: ProposeId) -> Self {
        Self {
            term,
            index,
            entry_data: EntryData::ConfChange(id),
        }
    }

    /// Get the id of the entry
    pub(super) fn id(&self) -> &ProposeId {
        match self.entry_data {
            EntryData::Command(ref cmd) => cmd.id(),
            EntryData::ConfChange(ref id) => id,
            EntryData::Shutdown => {
                unreachable!(
                    "LogEntry::id() should not be called on {:?} entry",
                    self.entry_data
                );
            }
        }
    }
}
