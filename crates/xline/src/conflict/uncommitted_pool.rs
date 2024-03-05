use std::collections::{hash_map, HashMap};

use curp::server::conflict::CommandEntry;
use curp_external_api::conflict::UncommittedPool;
use itertools::Itertools;
use utils::interval_map::IntervalMap;
use xlineapi::{
    command::{get_lease_ids, Command},
    interval::BytesAffine,
};

use super::{filter_kv, intervals, is_xor_cmd};

/// Uncommitted pool for KV commands.
#[derive(Debug, Default)]
pub(crate) struct KvUncomPool {
    /// Interval map for keys overlap detection
    map: IntervalMap<BytesAffine, Commands>,
}

impl UncommittedPool for KvUncomPool {
    type Entry = CommandEntry<Command>;

    fn insert(&mut self, entry: Self::Entry) -> bool {
        let Some(entry) = filter_kv(entry) else {
            return false;
        };

        let intervals = intervals(&entry);
        let conflict = intervals.iter().any(|i| self.map.overlap(i));
        for interval in intervals {
            let e = self.map.entry(interval).or_insert(Commands::default());
            e.push_cmd(entry.clone());
        }
        conflict
    }

    fn remove(&mut self, entry: Self::Entry) {
        let Some(entry) = filter_kv(entry) else {
            return;
        };
        let intervals = intervals(&entry);
        for interval in intervals {
            if self
                .map
                .get_mut(&interval)
                .map_or(false, |m| m.remove_cmd(&entry))
            {
                let _ignore = self.map.remove(&interval);
            }
        }
    }

    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry> {
        let Some(entry) = filter_kv(entry) else {
            return vec![];
        };
        let intervals = intervals(entry);
        intervals
            .into_iter()
            .flat_map(|i| self.map.find_all_overlap(&i))
            .flat_map(|(_, v)| v.all())
            .unique()
            .collect()
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
        self.map.len()
    }
}

/// Lease uncommitted pool
#[derive(Debug, Default)]
pub(crate) struct LeaseUncomPool {
    /// Stores leases in the pool
    leases: HashMap<i64, Commands>,
}

impl UncommittedPool for LeaseUncomPool {
    type Entry = CommandEntry<Command>;

    // TODO: fix lease leases command
    fn insert(&mut self, entry: Self::Entry) -> bool {
        let mut conflict = false;
        let ids = get_lease_ids(entry.request());
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

    fn remove(&mut self, entry: Self::Entry) {
        let ids = get_lease_ids(entry.request());
        for id in ids {
            if let hash_map::Entry::Occupied(mut e) = self.leases.entry(id) {
                if e.get_mut().remove_cmd(&entry) {
                    let _ignore = e.remove_entry();
                }
            }
        }
    }

    fn all_conflict(&self, entry: &Self::Entry) -> Vec<Self::Entry> {
        let ids = get_lease_ids(entry.request());
        ids.into_iter()
            .flat_map(|id| self.leases.get(&id).map(Commands::all).unwrap_or_default())
            .collect()
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
        self.leases.len()
    }
}

/// Uncommitted pool for commands that conflict with all other commands.
#[derive(Debug, Default)]
pub(crate) struct XorUncomPool {
    /// All commands in the pool
    conflicts: Commands,
}

impl UncommittedPool for XorUncomPool {
    type Entry = CommandEntry<Command>;

    fn insert(&mut self, entry: Self::Entry) -> bool {
        let mut conflict = !self.conflicts.is_empty();
        if is_xor_cmd(&entry) {
            self.conflicts.push_cmd(entry);
            // Always returns conflict when the command conflicts with all other commands
            conflict = true;
        }
        conflict
    }

    fn all_conflict(&self, _entry: &Self::Entry) -> Vec<Self::Entry> {
        self.conflicts.all()
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.conflicts.all()
    }

    fn is_empty(&self) -> bool {
        self.conflicts.is_empty()
    }

    fn remove(&mut self, entry: Self::Entry) {
        if is_xor_cmd(&entry) {
            let _ignore = self.conflicts.remove_cmd(&entry);
        }
    }

    fn clear(&mut self) {
        self.conflicts.clear();
    }

    fn len(&self) -> usize {
        self.conflicts.len()
    }
}

/// Value stored in uncommitted pool
#[derive(Debug, Default)]
struct Commands {
    /// The commands correspond to the key
    ///
    /// As we may need to insert multiple commands with the same
    /// set of keys, we store a vector of commands as the value.
    cmds: Vec<CommandEntry<Command>>,
}

impl Commands {
    /// Appends a cmd to the value
    fn push_cmd(&mut self, cmd: CommandEntry<Command>) {
        self.cmds.push(cmd);
    }

    /// Removes a cmd from the value
    ///
    /// Returns `true` if the value is empty
    fn remove_cmd(&mut self, cmd: &CommandEntry<Command>) -> bool {
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
    fn all(&self) -> Vec<CommandEntry<Command>> {
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
