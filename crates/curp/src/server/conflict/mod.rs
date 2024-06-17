/// Speculative pool
pub(crate) mod spec_pool_new;

/// Uncommitted pool
pub(crate) mod uncommitted_pool;

#[cfg(test)]
mod tests;

/// Conflict pool used in tests
#[doc(hidden)]
pub mod test_pools;

use std::{ops::Deref, sync::Arc};

use crate::rpc::{ConfChange, PoolEntry, PoolEntryInner, ProposeId};

// TODO: relpace `PoolEntry` with this
/// Entry stored in conflict pools
pub(super) enum ConflictPoolEntry<C> {
    /// A command entry
    Command(CommandEntry<C>),
    /// A conf change entry
    ConfChange(ConfChangeEntry),
}

impl<C> From<PoolEntry<C>> for ConflictPoolEntry<C> {
    fn from(entry: PoolEntry<C>) -> Self {
        match entry.inner {
            PoolEntryInner::Command(c) => ConflictPoolEntry::Command(CommandEntry {
                id: entry.id,
                cmd: c,
            }),
            PoolEntryInner::ConfChange(c) => ConflictPoolEntry::ConfChange(ConfChangeEntry {
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

impl<C> CommandEntry<C> {
    /// Creates a new `CommandEntry`
    #[inline]
    pub fn new(id: ProposeId, cmd: Arc<C>) -> Self {
        Self { id, cmd }
    }
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

impl<C> std::hash::Hash for CommandEntry<C> {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<C> PartialEq for CommandEntry<C> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<C> Eq for CommandEntry<C> {}

impl<C> PartialOrd for CommandEntry<C> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<C> Ord for CommandEntry<C> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
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
