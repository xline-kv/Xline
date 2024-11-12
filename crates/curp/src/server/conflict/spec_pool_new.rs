use std::collections::{HashMap, HashSet};

use curp_external_api::conflict::SpeculativePoolOp;
use serde::{Deserialize, Serialize};

use crate::rpc::{PoolEntry, ProposeId};

/// A speculative pool object
pub type SpObject<C> = Box<dyn SpeculativePoolOp<Entry = PoolEntry<C>> + Send + 'static>;

/// Union type of `SpeculativePool` objects
pub(crate) struct SpeculativePool<C> {
    /// Command speculative pools
    command_sps: Vec<SpObject<C>>,
    /// propose id to entry mapping
    entries: HashMap<ProposeId, PoolEntry<C>>,
    /// Current version
    version: u64,
}

impl<C> SpeculativePool<C> {
    /// Creates a new pool
    pub(crate) fn new(command_sps: Vec<SpObject<C>>, version: u64) -> Self {
        Self {
            command_sps,
            entries: HashMap::new(),
            version,
        }
    }

    /// Inserts an entry into the pool
    #[allow(clippy::needless_pass_by_value)] // we need to consume the entry
    pub(crate) fn insert(&mut self, entry: PoolEntry<C>) -> Option<PoolEntry<C>> {
        for csp in &mut self.command_sps {
            if let Some(e) = csp.insert_if_not_conflict(entry.clone()) {
                return Some(e);
            }
        }

        let _ignore = self.entries.insert(entry.id, entry);

        None
    }

    /// Removes an entry from the pool
    pub(crate) fn remove(&mut self, entry: &PoolEntry<C>) {
        for csp in &mut self.command_sps {
            csp.remove(entry);
        }

        let _ignore = self.entries.remove(&entry.id);
    }

    /// Removes an entry from the pool by it's propose id
    pub(crate) fn remove_by_id(&mut self, id: &ProposeId) {
        if let Some(entry) = self.entries.remove(id) {
            for csp in &mut self.command_sps {
                csp.remove(&entry);
            }
        }
    }

    /// Returns all entries in the pool
    pub(crate) fn all(&self) -> Vec<PoolEntry<C>> {
        self.all_ref().map(PoolEntry::clone).collect()
    }

    /// Returns all entry refs in the pool
    pub(crate) fn all_ref(&self) -> impl Iterator<Item = &PoolEntry<C>> {
        self.entries.values()
    }

    /// Returns all entry refs in the pool
    pub(crate) fn all_ids(&self) -> impl Iterator<Item = &ProposeId> {
        self.entries.keys()
    }

    /// Returns the number of entries in the pool
    #[allow(clippy::arithmetic_side_effects)] // Pool sizes can't overflow a `usize`
    pub(crate) fn len(&self) -> usize {
        self.command_sps
            .iter()
            .fold(0, |sum, pool| sum + pool.len())
    }

    /// Performs garbage collection on the spec pool with given entries from the leader
    ///
    /// Removes entries from the pool that are not present in the provided `leader_entries`
    pub(crate) fn gc(&mut self, leader_entry_ids: &HashSet<ProposeId>, version: u64) {
        debug_assert!(version > self.version, "invalid version: {version}");
        self.version = version;
        let to_remove: Vec<_> = self
            .entries
            .keys()
            .filter(|id| !leader_entry_ids.contains(id))
            .copied()
            .collect();
        for id in to_remove {
            self.remove_by_id(&id);
        }
    }

    /// Returns the current version
    pub(crate) fn version(&self) -> u64 {
        self.version
    }
}

/// A Speculative Pool log entry
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SpecPoolRepl {
    /// The version of this entry
    version: u64,
    /// Propose ids of the leader's speculative pool entries
    ids: HashSet<ProposeId>,
}

impl SpecPoolRepl {
    /// Creates a new `SpecPoolEntry`
    pub(crate) fn new(version: u64, ids: HashSet<ProposeId>) -> Self {
        Self { version, ids }
    }

    /// Returns the version of this entry
    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    /// Returns the propose ids
    pub(crate) fn ids(&self) -> &HashSet<ProposeId> {
        &self.ids
    }
}
