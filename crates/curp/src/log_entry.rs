use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use curp_external_api::{cmd::Command, InflightId, LogIndex};
use serde::{Deserialize, Serialize};

use crate::{
    members::ServerId,
    rpc::{ConfChange, PoolEntryInner, ProposeId, PublishRequest},
};

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub struct LogEntry<C> {
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
    ConfChange(Vec<ConfChange>),
    /// `Shutdown` entry
    Shutdown,
    /// `SetNodeState` entry
    SetNodeState(ServerId, String, Vec<String>),
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
        EntryData::SetNodeState(value.node_id, value.name, value.client_urls)
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

    /// Get the inflight id of this log entry
    pub(super) fn inflight_id(&self) -> InflightId {
        propose_id_to_inflight_id(self.propose_id)
    }
}

/// Propose id to inflight id
pub(super) fn propose_id_to_inflight_id(id: ProposeId) -> InflightId {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.0.hash(&mut hasher);
    id.1.hash(&mut hasher);
    hasher.finish()
}
