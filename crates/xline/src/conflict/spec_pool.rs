//! A speculative pool(witness) is used to store commands that are speculatively executed.
//! CURP requires that a witness only accepts and saves an operation if it is commutative
//! with every other operation currently stored by that witness

use std::collections::HashMap;

use curp::server::conflict::CommandEntry;
use curp_external_api::conflict::{ConflictPoolOp, SpeculativePoolOp};
use utils::interval_map::IntervalMap;
use xlineapi::{
    command::{get_lease_ids, Command},
    interval::BytesAffine,
};

use super::{filter_kv, intervals, is_exclusive_cmd};

/// Speculative pool for KV commands.
#[derive(Debug, Default)]
pub(crate) struct KvSpecPool {
    /// Interval map for keys overlap detection
    map: IntervalMap<BytesAffine, CommandEntry<Command>>,
}

impl ConflictPoolOp for KvSpecPool {
    type Entry = CommandEntry<Command>;

    fn remove(&mut self, entry: Self::Entry) {
        let Some(entry) = filter_kv(entry) else {
            return;
        };

        for interval in intervals(&entry) {
            let _ignore = self.map.remove(&interval);
        }
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.map.iter().map(|(_, v)| v).map(Clone::clone).collect()
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

impl SpeculativePoolOp for KvSpecPool {
    fn insert_if_not_conflict(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
        let entry = filter_kv(entry)?;

        let intervals = intervals(&entry);
        if intervals.iter().any(|i| self.map.overlap(i)) {
            return Some(entry);
        }
        for interval in intervals {
            let _ignore = self.map.insert(interval, entry.clone());
        }
        None
    }
}

/// Speculative pool for Lease commands.
#[derive(Debug, Default)]
pub(crate) struct LeaseSpecPool {
    /// Stores leases in the pool
    leases: HashMap<i64, CommandEntry<Command>>,
}

impl ConflictPoolOp for LeaseSpecPool {
    type Entry = CommandEntry<Command>;

    fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    fn remove(&mut self, entry: Self::Entry) {
        let ids = get_lease_ids(entry.request());
        for id in ids {
            let _ignore = self.leases.remove(&id);
        }
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.leases.values().cloned().collect()
    }

    fn clear(&mut self) {
        self.leases.clear();
    }

    fn len(&self) -> usize {
        self.leases.len()
    }
}

impl SpeculativePoolOp for LeaseSpecPool {
    fn insert_if_not_conflict(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
        let ids = get_lease_ids(entry.request());
        for id in ids.clone() {
            if self.leases.contains_key(&id) {
                return Some(entry);
            }
        }
        for id in ids {
            let _ignore = self.leases.insert(id, entry.clone());
        }
        None
    }
}

/// Speculative pool for commands that conflict with all other commands.
#[derive(Debug, Default)]
pub(crate) struct ExclusiveSpecPool {
    /// Stores the command
    conflict: Option<CommandEntry<Command>>,
}

impl ConflictPoolOp for ExclusiveSpecPool {
    type Entry = CommandEntry<Command>;

    fn is_empty(&self) -> bool {
        self.conflict.is_none()
    }

    fn remove(&mut self, entry: Self::Entry) {
        if is_exclusive_cmd(&entry) {
            self.conflict = None;
        }
    }

    fn all(&self) -> Vec<Self::Entry> {
        self.conflict.iter().cloned().collect()
    }

    fn clear(&mut self) {
        self.conflict = None;
    }

    fn len(&self) -> usize {
        self.conflict.iter().count()
    }
}

impl SpeculativePoolOp for ExclusiveSpecPool {
    fn insert_if_not_conflict(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
        if self.conflict.is_some() {
            return Some(entry);
        }

        if is_exclusive_cmd(&entry) {
            self.conflict = Some(entry.clone());
            // Always returns conflict when the command conflicts with all other commands
            return Some(entry);
        }
        None
    }
}
