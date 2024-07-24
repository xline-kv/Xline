use std::hash::{Hash, Hasher};

use curp_external_api::{cmd::Command, InflightId, LogIndex};
use serde::{Deserialize, Serialize};

use crate::rpc::ProposeId;

pub(crate) use entry_data::EntryData;

/// Definition of different entry data types
mod entry_data;

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

impl<C> AsRef<LogEntry<C>> for LogEntry<C> {
    fn as_ref(&self) -> &LogEntry<C> {
        self
    }
}

/// Propose id to inflight id
pub(super) fn propose_id_to_inflight_id(id: ProposeId) -> InflightId {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.0.hash(&mut hasher);
    id.1.hash(&mut hasher);
    let inflight = hasher.finish();
    tracing::debug!("convert: {id:?} to {inflight:?}");
    inflight
}
