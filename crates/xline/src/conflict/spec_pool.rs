//! A speculative pool(witness) is used to store commands that are speculatively
//! executed. CURP requires that a witness only accepts and saves an operation
//! if it is commutative with every other operation currently stored by that
//! witness

use std::{collections::HashMap, sync::Arc};

use curp::rpc::PoolEntry;
use curp_external_api::conflict::{ConflictPoolOp, EntryId, SpeculativePoolOp};
use utils::interval_map::{Interval, IntervalMap};
use xlineapi::{command::Command, interval::BytesAffine};

use crate::storage::lease_store::LeaseCollection;

use super::{all_leases, intervals, is_exclusive_cmd};

/// Speculative pool for KV commands.
#[derive(Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct KvSpecPool {
    /// Interval map for keys overlap detection
    map: IntervalMap<BytesAffine, PoolEntry<Command>>,
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

impl KvSpecPool {
    /// Creates a new [`KvSpecPool`].
    pub(crate) fn new(lease_collection: Arc<LeaseCollection>) -> Self {
        Self {
            map: IntervalMap::new(),
            lease_collection,
            intervals: HashMap::new(),
        }
    }
}

impl ConflictPoolOp for KvSpecPool {
    type Entry = PoolEntry<Command>;

    fn remove(&mut self, entry: &Self::Entry) {
        for interval in self.intervals.remove(&entry.id()).into_iter().flatten() {
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
        let intervals = intervals(&self.lease_collection, &entry);
        if intervals.iter().any(|i| self.map.overlaps(i)) {
            return Some(entry);
        }
        assert!(
            self.intervals
                .insert(entry.id(), intervals.clone())
                .is_none(),
            "duplicate entry id"
        );
        for interval in intervals {
            let _ignore = self.map.insert(interval, entry.clone());
        }
        None
    }
}

/// Speculative pool for Lease commands.
#[derive(Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct LeaseSpecPool {
    /// Stores leases in the pool
    leases: HashMap<i64, PoolEntry<Command>>,
    /// Lease collection
    lease_collection: Arc<LeaseCollection>,
    /// Id to lease ids map
    ///
    /// NOTE: To avoid potential side-effects from the `LeaseCollection`, we
    /// store The lookup results from `LeaseCollection` during entry insert
    /// and use these result in entry remove.
    ids: HashMap<<<Self as ConflictPoolOp>::Entry as EntryId>::Id, Vec<i64>>,
}

impl LeaseSpecPool {
    /// Creates a new [`LeaseSpecPool`].
    pub(crate) fn new(lease_collection: Arc<LeaseCollection>) -> Self {
        Self {
            leases: HashMap::new(),
            lease_collection,
            ids: HashMap::new(),
        }
    }
}

impl ConflictPoolOp for LeaseSpecPool {
    type Entry = PoolEntry<Command>;

    fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }

    fn remove(&mut self, entry: &Self::Entry) {
        for id in self.ids.remove(&entry.id()).into_iter().flatten() {
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
        let ids = all_leases(&self.lease_collection, &entry);
        for id in &ids {
            if self.leases.contains_key(id) {
                return Some(entry);
            }
        }
        assert!(
            self.ids.insert(entry.id(), ids.clone()).is_none(),
            "duplicate entry id"
        );
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
    conflict: Option<PoolEntry<Command>>,
}

impl ConflictPoolOp for ExclusiveSpecPool {
    type Entry = PoolEntry<Command>;

    fn is_empty(&self) -> bool {
        self.conflict.is_none()
    }

    fn remove(&mut self, entry: &Self::Entry) {
        if is_exclusive_cmd(entry) {
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
