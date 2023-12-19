use std::sync::Arc;

use curp_external_api::{cmd::Command, LogIndex};
use serde::{Deserialize, Serialize};

use crate::{
    members::ServerId,
    rpc::{ConfChange, PoolEntryInner, ProposeId},
    PublishRequest,
};

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct LogEntry<C> {
    /// Term
    pub(crate) term: u64,
    /// Index
    pub(crate) index: LogIndex,
    /// Propose id
    pub(crate) propose_id: ProposeId,
    /// Entry data
    pub(crate) entry_data: EntryData<C>,
}

/// Entry data of a `LogEntry`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum EntryData<C> {
    /// Empty entry
    Empty,
    /// `Command` entry
    Command(Arc<C>),
    /// `ConfChange` entry
    ConfChange(Vec<ConfChange>), // Box to fix variant_size_differences
    /// `Shutdown` entry
    Shutdown,
    /// `SetName` entry
    SetName(ServerId, String),
}

impl<C> From<Arc<C>> for EntryData<C> {
    fn from(cmd: Arc<C>) -> Self {
        EntryData::Command(cmd)
    }
}

impl<C> From<Vec<ConfChange>> for EntryData<C> {
    fn from(value: Vec<ConfChange>) -> Self {
        Self::ConfChange(value)
    }
}

impl<C> From<PoolEntryInner<C>> for EntryData<C> {
    fn from(value: PoolEntryInner<C>) -> Self {
        match value {
            PoolEntryInner::Command(cmd) => EntryData::Command(cmd),
            PoolEntryInner::ConfChange(conf_change) => EntryData::ConfChange(conf_change),
        }
    }
}

impl<C> From<PublishRequest> for EntryData<C> {
    fn from(value: PublishRequest) -> Self {
        EntryData::SetName(value.node_id, value.name)
    }
}

impl<C> LogEntry<C>
where
    C: Command,
{
    /// Create a new `LogEntry`
    pub(super) fn new(
        index: LogIndex,
        term: u64,
        propose_id: ProposeId,
        entry_data: impl Into<EntryData<C>>,
    ) -> Self {
        Self {
            term,
            index,
            propose_id,
            entry_data: entry_data.into(),
        }
    }

    /// Get the trigger id of this log entry
    pub(super) fn trigger_id(&self) -> u64 {
        // TODO: Will this significantly increase the probability of collision?
        self.propose_id.0 ^ self.propose_id.1
    }
}
