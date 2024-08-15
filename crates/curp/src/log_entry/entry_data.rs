use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::member::Membership;
use crate::members::ServerId;
use crate::rpc::PublishRequest;

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
    /// `SetNodeState` entry
    SetNodeState(ServerId, String, Vec<String>),
    /// `Member` entry
    Member(Membership),
}

impl<C> From<Arc<C>> for EntryData<C> {
    fn from(cmd: Arc<C>) -> Self {
        EntryData::Command(cmd)
    }
}

impl<C> From<PublishRequest> for EntryData<C> {
    fn from(value: PublishRequest) -> Self {
        EntryData::SetNodeState(value.node_id, value.name, value.client_urls)
    }
}

impl<C> From<Membership> for EntryData<C> {
    fn from(value: Membership) -> Self {
        EntryData::Member(value)
    }
}
