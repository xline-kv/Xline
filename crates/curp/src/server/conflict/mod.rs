#![allow(unused)]
#![allow(unreachable_pub)]

/// Speculative pool
pub(crate) mod spec_pool_new;

/// Uncommitted pool
pub(crate) mod uncommitted_pool;

#[cfg(test)]
mod tests;

use std::{ops::Deref, sync::Arc};

use crate::rpc::{ConfChange, PoolEntry, PoolEntryInner, ProposeId};

// TODO: relpace `PoolEntry` with this
/// Entry stored in conflict pools
pub(super) enum SplitEntry<C> {
    /// A command entry
    Command(CommandEntry<C>),
    /// A conf change entry
    ConfChange(ConfChangeEntry),
}

impl<C> From<PoolEntry<C>> for SplitEntry<C> {
    fn from(entry: PoolEntry<C>) -> Self {
        match entry.inner {
            PoolEntryInner::Command(c) => SplitEntry::Command(CommandEntry {
                id: entry.id,
                cmd: c,
            }),
            PoolEntryInner::ConfChange(c) => SplitEntry::ConfChange(ConfChangeEntry {
                id: entry.id,
                conf_change: c,
            }),
        }
    }
}

/// Command entry type
#[derive(Debug)]
pub struct CommandEntry<C> {
    /// The propose id
    id: ProposeId,
    /// The command
    cmd: Arc<C>,
}

impl<C> Clone for CommandEntry<C> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            cmd: Arc::clone(&self.cmd),
        }
    }
}

impl<C> Deref for CommandEntry<C> {
    type Target = C;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}

impl<C> AsRef<C> for CommandEntry<C> {
    #[inline]
    fn as_ref(&self) -> &C {
        self.cmd.as_ref()
    }
}

impl<C> From<CommandEntry<C>> for PoolEntry<C> {
    fn from(entry: CommandEntry<C>) -> Self {
        PoolEntry {
            id: entry.id,
            inner: PoolEntryInner::Command(entry.cmd),
        }
    }
}

/// Conf change entry type
#[derive(Clone, PartialEq)]
pub(super) struct ConfChangeEntry {
    /// The propose id
    id: ProposeId,
    /// The conf change entry
    conf_change: Vec<ConfChange>,
}

impl<C> From<ConfChangeEntry> for PoolEntry<C> {
    fn from(entry: ConfChangeEntry) -> Self {
        PoolEntry {
            id: entry.id,
            inner: PoolEntryInner::ConfChange(entry.conf_change),
        }
    }
}
