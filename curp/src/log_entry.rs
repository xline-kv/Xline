use std::sync::Arc;

use curp_external_api::{
    cmd::{Command, ProposeId},
    LogIndex,
};
use serde::{Deserialize, Serialize};

use crate::{members::ServerId, rpc::ConfChangeEntry, server::PoolEntry, PublishRequest};

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
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
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum EntryData<C> {
    /// Empty entry
    Empty(ProposeId),
    /// `Command` entry
    Command(Arc<C>),
    /// `ConfChange` entry
    ConfChange(Box<ConfChangeEntry>), // Box to fix variant_size_differences
    /// `Shutdown` entry
    Shutdown(ProposeId),
    /// `SetName` entry
    SetName(ProposeId, ServerId, String),
}

impl<C> From<ConfChangeEntry> for EntryData<C> {
    fn from(conf_change: ConfChangeEntry) -> Self {
        EntryData::ConfChange(Box::new(conf_change))
    }
}

impl<C> From<Arc<C>> for EntryData<C> {
    fn from(cmd: Arc<C>) -> Self {
        EntryData::Command(cmd)
    }
}

impl<C> From<PoolEntry<C>> for EntryData<C> {
    fn from(value: PoolEntry<C>) -> Self {
        match value {
            PoolEntry::Command(cmd) => EntryData::Command(cmd),
            PoolEntry::ConfChange(conf_change) => EntryData::ConfChange(Box::new(conf_change)),
        }
    }
}

impl<C> From<PublishRequest> for EntryData<C> {
    fn from(value: PublishRequest) -> Self {
        EntryData::SetName(value.id(), value.node_id, value.name)
    }
}

impl<C> LogEntry<C>
where
    C: Command,
{
    /// Create a new `LogEntry`
    pub(super) fn new(index: LogIndex, term: u64, entry_data: impl Into<EntryData<C>>) -> Self {
        Self {
            term,
            index,
            entry_data: entry_data.into(),
        }
    }

    /// Get the id of the entry
    pub(super) fn id(&self) -> ProposeId {
        match self.entry_data {
            EntryData::Command(ref cmd) => cmd.id(),
            EntryData::ConfChange(ref e) => e.id(),
            EntryData::Shutdown(id) | EntryData::Empty(id) | EntryData::SetName(id, _, _) => id,
        }
    }
}
