use curp_external_api::conflict::{ConflictPoolOp, UncommittedPoolOp};

use super::{CommandEntry, ConfChangeEntry, ConflictPoolEntry};
use crate::rpc::PoolEntry;

/// An uncommitted pool object
pub type UcpObject<C> = Box<dyn UncommittedPoolOp<Entry = CommandEntry<C>> + Send + 'static>;

/// Union type of `UncommittedPool` objects
pub(crate) struct UncommittedPool<C> {
    /// Command uncommitted pools
    command_ucps: Vec<UcpObject<C>>,
    /// Conf change uncommitted pools
    conf_change_ucp: ConfChangeUcp,
}

impl<C> UncommittedPool<C> {
    /// Creates a new `UncomPool`
    pub(crate) fn new(command_ucps: Vec<UcpObject<C>>) -> Self {
        Self {
            command_ucps,
            conf_change_ucp: ConfChangeUcp::default(),
        }
    }

    /// Insert an entry into the pool
    pub(crate) fn insert(&mut self, entry: PoolEntry<C>) -> bool {
        let mut conflict = false;

        conflict |= !self.conf_change_ucp.is_empty();

        match ConflictPoolEntry::from(entry) {
            ConflictPoolEntry::Command(c) => {
                for cucp in &mut self.command_ucps {
                    conflict |= cucp.insert(c.clone());
                }
            }
            ConflictPoolEntry::ConfChange(c) => {
                let _ignore = self.conf_change_ucp.insert(c);
                conflict |= !self
                    .command_ucps
                    .iter()
                    .map(AsRef::as_ref)
                    .all(ConflictPoolOp::is_empty);
            }
        }

        conflict
    }

    /// Removes an entry from the pool
    pub(crate) fn remove(&mut self, entry: PoolEntry<C>) {
        match ConflictPoolEntry::from(entry) {
            ConflictPoolEntry::Command(c) => {
                for cucp in &mut self.command_ucps {
                    cucp.remove(c.clone());
                }
            }
            ConflictPoolEntry::ConfChange(c) => {
                self.conf_change_ucp.remove(c);
            }
        }
    }

    /// Returns all entries in the pool that conflict with the given entry
    pub(crate) fn all_conflict(&self, entry: PoolEntry<C>) -> Vec<PoolEntry<C>> {
        match ConflictPoolEntry::from(entry) {
            // A command entry conflict with other conflict entries plus all conf change entries
            ConflictPoolEntry::Command(ref c) => self
                .conf_change_ucp
                .all()
                .into_iter()
                .map(Into::into)
                .chain(
                    self.command_ucps
                        .iter()
                        .flat_map(|p| p.all_conflict(c))
                        .map(Into::into),
                )
                .collect(),
            // A conf change entry conflict with all other entries
            ConflictPoolEntry::ConfChange(_) => self
                .conf_change_ucp
                .all()
                .into_iter()
                .map(Into::into)
                .chain(
                    self.command_ucps
                        .iter()
                        .map(AsRef::as_ref)
                        .flat_map(ConflictPoolOp::all)
                        .map(Into::into),
                )
                .collect(),
        }
    }

    #[cfg(test)]
    /// Gets all entries in the pool
    pub(crate) fn all(&self) -> Vec<PoolEntry<C>> {
        let mut entries = Vec::new();
        for csp in &self.command_ucps {
            entries.extend(csp.all().into_iter().map(Into::into));
        }
        entries.extend(self.conf_change_ucp.all().into_iter().map(Into::into));
        entries
    }

    #[cfg(test)]
    /// Returns `true` if the pool is empty
    pub(crate) fn is_empty(&self) -> bool {
        self.command_ucps.iter().all(|ucp| ucp.is_empty()) && self.conf_change_ucp.is_empty()
    }

    /// Clears all entries in the pool
    pub(crate) fn clear(&mut self) {
        for ucp in &mut self.command_ucps {
            ucp.clear();
        }
        self.conf_change_ucp.clear();
    }
}

/// Conf change uncommitted pool
#[derive(Default)]
struct ConfChangeUcp {
    /// entry count
    conf_changes: Vec<ConfChangeEntry>,
}

impl ConflictPoolOp for ConfChangeUcp {
    type Entry = ConfChangeEntry;

    fn is_empty(&self) -> bool {
        self.conf_changes.is_empty()
    }

    fn remove(&mut self, entry: Self::Entry) {
        if let Some(pos) = self.conf_changes.iter().position(|x| *x == entry) {
            let _ignore = self.conf_changes.remove(pos);
        }
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.conf_changes.clone()
    }

    fn clear(&mut self) {
        self.conf_changes.clear();
    }

    fn len(&self) -> usize {
        self.conf_changes.len()
    }
}

impl UncommittedPoolOp for ConfChangeUcp {
    fn insert(&mut self, entry: Self::Entry) -> bool {
        let conflict = !self.conf_changes.is_empty();
        self.conf_changes.push(entry);
        conflict
    }

    fn all_conflict(&self, _entry: &Self::Entry) -> Vec<Self::Entry> {
        self.conf_changes.clone()
    }
}
