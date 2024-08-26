//! An uncommitted pool is used to store unsynced commands.
//! CURP requires that a master will only execute client operations
//! speculatively, if that operation is commutative with every other unsynced
//! operation.

use std::{
    collections::{hash_map, HashMap},
    sync::Arc,
};

use curp::rpc::PoolEntry;
use curp_external_api::conflict::{ConflictPoolOp, EntryId, UncommittedPoolOp};
use itertools::Itertools;
use utils::interval_map::{Interval, IntervalMap};
use xlineapi::{command::Command, interval::BytesAffine};

use crate::storage::lease_store::LeaseCollection;

use super::{all_leases, intervals, is_exclusive_cmd};

/// Uncommitted pool for KV commands.
#[derive(Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct KvUncomPool {
    /// Interval map for keys overlap detection
    map: IntervalMap<BytesAffine, Commands>,
    /// Lease collection
    lease_collection: Arc<LeaseCollection>,
    /// Id to intervals map
    ///
    /// NOTE: To avoid potential side-effects from the `LeaseCollection`, we
    /// store The lookup results from `LeaseCollection` during entry insert
    /// and use these result in entry remove.
    intervals:
        HashMap<<<Self as ConflictPoolOp>::Entry as EntryId>::Id, Vec<Interval<BytesAffine>>>,
}

impl KvUncomPool {
    /// Creates a new [`KvUncomPool`].
    pub(crate) fn new(lease_collection: Arc<LeaseCollection>) -> Self {
        Self {
            map: IntervalMap::new(),
            lease_collection,
            intervals: HashMap::new(),
        }
    }
}

impl ConflictPoolOp for KvUncomPool {
    type Entry = PoolEntry<Command>;

    fn remove(&mut self, entry: &Self::Entry) {
        for interval in self.intervals.remove(&entry.id()).into_iter().flatten() {
            if self
                .map
                .get_mut(&interval)
                .map_or(false, |m| m.remove_cmd(entry))
            {
                let _ignore = self.map.remove(&interval);
            }
        }
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.map
            .iter()
            .flat_map(|(_, v)| v.all())
            .unique()
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn clear(&mut self) {
        self.map.clear();
    }

    fn len(&self) -> usize {
        self.map.iter().flat_map(|(_, v)| v.all()).unique().count()
    }
}

impl UncommittedPoolOp for KvUncomPool {
    fn insert(&mut self, entry: Self::Entry) -> bool {
        let intervals = intervals(&self.lease_collection, &entry);
        let _ignore = self.intervals.insert(entry.id(), intervals.clone());
        let conflict = intervals.iter().any(|i| self.map.overlaps(i));
        for interval in intervals {
            let e = self.map.entry(interval).or_insert(Commands::default());
            e.push_cmd(entry.clone());
        }
        conflict
    }

    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry> {
        let intervals = intervals(&self.lease_collection, entry);
        intervals
            .into_iter()
            .flat_map(|i| self.map.find_all_overlap(&i))
            .flat_map(|(_, v)| v.all())
            .unique()
            .collect()
    }
}

/// Lease uncommitted pool
#[derive(Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct LeaseUncomPool {
    /// Stores leases in the pool
    leases: HashMap<i64, Commands>,
    /// Lease collection
    lease_collection: Arc<LeaseCollection>,
    /// Id to lease ids map
    ///
    /// NOTE: To avoid potential side-effects from the `LeaseCollection`, we
    /// store The lookup results from `LeaseCollection` during entry insert
    /// and use these result in entry remove.
    ids: HashMap<<<Self as ConflictPoolOp>::Entry as EntryId>::Id, Vec<i64>>,
}

impl LeaseUncomPool {
    /// Creates a new [`LeaseUncomPool`].
    pub(crate) fn new(lease_collection: Arc<LeaseCollection>) -> Self {
        Self {
            leases: HashMap::new(),
            lease_collection,
            ids: HashMap::new(),
        }
    }
}

impl ConflictPoolOp for LeaseUncomPool {
    type Entry = PoolEntry<Command>;

    fn remove(&mut self, entry: &Self::Entry) {
        for id in self.ids.remove(&entry.id()).into_iter().flatten() {
            if let hash_map::Entry::Occupied(mut e) = self.leases.entry(id) {
                if e.get_mut().remove_cmd(entry) {
                    let _ignore = e.remove_entry();
                }
            }
        }
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.leases
            .iter()
            .flat_map(|(_, v)| v.all())
            .unique()
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    fn clear(&mut self) {
        self.leases.clear();
    }

    fn len(&self) -> usize {
        self.leases
            .iter()
            .flat_map(|(_, v)| v.all())
            .unique()
            .count()
    }
}

impl UncommittedPoolOp for LeaseUncomPool {
    fn insert(&mut self, entry: Self::Entry) -> bool {
        let mut conflict = false;
        let ids = all_leases(&self.lease_collection, &entry);
        let _ignore = self.ids.insert(entry.id(), ids.clone());
        for id in ids {
            match self.leases.entry(id) {
                hash_map::Entry::Occupied(mut e) => {
                    e.get_mut().push_cmd(entry.clone());
                    conflict = true;
                }
                hash_map::Entry::Vacant(v) => {
                    let e = v.insert(Commands::default());
                    e.push_cmd(entry.clone());
                }
            }
        }
        conflict
    }

    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry> {
        let ids = all_leases(&self.lease_collection, entry);
        ids.into_iter()
            .flat_map(|id| self.leases.get(&id).map(Commands::all).unwrap_or_default())
            .collect()
    }
}

/// Uncommitted pool for commands that conflict with all other commands.
#[derive(Debug, Default)]
pub(crate) struct ExclusiveUncomPool {
    /// All commands in the pool
    conflicts: Commands,
}

impl ConflictPoolOp for ExclusiveUncomPool {
    type Entry = PoolEntry<Command>;

    fn all(&self) -> Vec<Self::Entry> {
        self.conflicts.all()
    }

    fn is_empty(&self) -> bool {
        self.conflicts.is_empty()
    }

    fn remove(&mut self, entry: &Self::Entry) {
        if is_exclusive_cmd(entry) {
            let _ignore = self.conflicts.remove_cmd(entry);
        }
    }

    fn clear(&mut self) {
        self.conflicts.clear();
    }

    fn len(&self) -> usize {
        self.conflicts.len()
    }
}

impl UncommittedPoolOp for ExclusiveUncomPool {
    fn insert(&mut self, entry: Self::Entry) -> bool {
        let mut conflict = !self.conflicts.is_empty();
        if is_exclusive_cmd(&entry) {
            self.conflicts.push_cmd(entry);
            // Always returns conflict when the command conflicts with all other commands
            conflict = true;
        }
        conflict
    }

    fn all_conflict(&self, _entry: &Self::Entry) -> Vec<Self::Entry> {
        self.conflicts.all()
    }
}

/// Value stored in uncommitted pool
#[derive(Debug, Default)]
struct Commands {
    /// The commands correspond to the key
    ///
    /// As we may need to insert multiple commands with the same
    /// set of keys, we store a vector of commands as the value.
    cmds: Vec<PoolEntry<Command>>,
}

impl Commands {
    /// Appends a cmd to the value
    fn push_cmd(&mut self, cmd: PoolEntry<Command>) {
        self.cmds.push(cmd);
    }

    /// Removes a cmd from the value
    ///
    /// Returns `true` if the value is empty
    fn remove_cmd(&mut self, cmd: &PoolEntry<Command>) -> bool {
        let Some(idx) = self.cmds.iter().position(|c| c == cmd) else {
            return self.is_empty();
        };
        let _ignore = self.cmds.swap_remove(idx);
        self.is_empty()
    }

    /// Checks if the value is empty
    fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }

    /// Gets all commands
    fn all(&self) -> Vec<PoolEntry<Command>> {
        self.cmds.clone()
    }

    /// Clears all commands
    fn clear(&mut self) {
        self.cmds.clear();
    }

    /// Returns the number of commands
    fn len(&self) -> usize {
        self.cmds.len()
    }
}
