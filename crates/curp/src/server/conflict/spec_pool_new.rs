use std::{collections::HashMap, sync::Arc};

use curp_external_api::conflict::SpeculativePoolOp;
use parking_lot::Mutex;

use crate::rpc::{PoolEntry, ProposeId};

/// Ref to `SpeculativePool`
pub(crate) type SpeculativePoolRef<C> = Arc<Mutex<SpeculativePool<C>>>;

/// A speculative pool object
pub type SpObject<C> = Box<dyn SpeculativePoolOp<Entry = PoolEntry<C>> + Send + 'static>;

/// Union type of `SpeculativePool` objects
pub(crate) struct SpeculativePool<C> {
    /// Command speculative pools
    command_sps: Vec<SpObject<C>>,
    /// propose id to entry mapping
    entries: HashMap<ProposeId, PoolEntry<C>>,
}

impl<C> SpeculativePool<C> {
    /// Creates a new pool
    pub(crate) fn new(command_sps: Vec<SpObject<C>>) -> Self {
        Self {
            command_sps,
            entries: HashMap::new(),
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
        let mut entries = Vec::new();
        for csp in &self.command_sps {
            entries.extend(csp.all().into_iter().map(Into::into));
        }
        entries
    }

    /// Returns the number of entries in the pool
    #[allow(clippy::arithmetic_side_effects)] // Pool sizes can't overflow a `usize`
    pub(crate) fn len(&self) -> usize {
        self.command_sps
            .iter()
            .fold(0, |sum, pool| sum + pool.len())
    }
}
