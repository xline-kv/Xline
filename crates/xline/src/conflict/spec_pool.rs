use std::collections::HashMap;

use curp::server::conflict::CommandEntry;
use curp_external_api::conflict::SpeculativePool;
use utils::interval_map::IntervalMap;
use xlineapi::{
    command::{get_lease_ids, Command},
    interval::BytesAffine,
};

use super::{filter_kv, intervals, is_xor_cmd};

/// Speculative pool for KV commands.
#[derive(Debug, Default)]
pub(crate) struct KvSpecPool {
    /// Interval map for keys overlap detection
    map: IntervalMap<BytesAffine, CommandEntry<Command>>,
}

impl SpeculativePool for KvSpecPool {
    type Entry = CommandEntry<Command>;

    fn insert(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
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

/// Speculative pool for Lease commands.
#[derive(Debug, Default)]
pub(crate) struct LeaseSpecPool {
    /// Stores leases in the pool
    leases: HashMap<i64, CommandEntry<Command>>,
}

impl SpeculativePool for LeaseSpecPool {
    type Entry = CommandEntry<Command>;

    // TODO: fix lease leases command
    fn insert(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
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

/// Speculative pool for commands that conflict with all other commands.
#[derive(Debug, Default)]
pub(crate) struct XorSpecPool {
    /// Stores the command
    conflict: Option<CommandEntry<Command>>,
}

impl SpeculativePool for XorSpecPool {
    type Entry = CommandEntry<Command>;

    fn insert(&mut self, entry: Self::Entry) -> Option<Self::Entry> {
        if self.conflict.is_some() {
            return Some(entry);
        }

        if is_xor_cmd(&entry) {
            self.conflict = Some(entry.clone());
            // Always returns conflict when the command conflicts with all other commands
            return Some(entry);
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.conflict.is_none()
    }

    fn remove(&mut self, entry: Self::Entry) {
        if is_xor_cmd(&entry) {
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
