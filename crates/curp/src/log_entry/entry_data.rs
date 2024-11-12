use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::member::Membership;
use crate::server::conflict::spec_pool_new::SpecPoolRepl;

#[allow(variant_size_differences)] // The `Membership` won't be too large
/// Entry data of a `LogEntry`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) enum EntryData<C> {
    /// Empty entry
    Empty,
    /// `Command` entry
    Command(Arc<C>),
    /// `Shutdown` entry
    Shutdown,
    /// `Member` entry
    Member(Membership),
    /// Speculative pool replication entry
    SpecPoolReplication(SpecPoolRepl),
}

impl<C> From<Arc<C>> for EntryData<C> {
    fn from(cmd: Arc<C>) -> Self {
        EntryData::Command(cmd)
    }
}

impl<C> From<Membership> for EntryData<C> {
    fn from(value: Membership) -> Self {
        EntryData::Member(value)
    }
}

impl<C> From<SpecPoolRepl> for EntryData<C> {
    fn from(value: SpecPoolRepl) -> Self {
        EntryData::SpecPoolReplication(value)
    }
}
