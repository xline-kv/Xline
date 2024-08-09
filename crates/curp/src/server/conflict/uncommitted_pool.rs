use curp_external_api::conflict::UncommittedPoolOp;

use crate::rpc::PoolEntry;

/// An uncommitted pool object
pub type UcpObject<C> = Box<dyn UncommittedPoolOp<Entry = PoolEntry<C>> + Send + 'static>;

/// Union type of `UncommittedPool` objects
pub(crate) struct UncommittedPool<C> {
    /// Command uncommitted pools
    command_ucps: Vec<UcpObject<C>>,
}

impl<C> UncommittedPool<C> {
    /// Creates a new `UncomPool`
    pub(crate) fn new(command_ucps: Vec<UcpObject<C>>) -> Self {
        Self { command_ucps }
    }

    /// Insert an entry into the pool
    pub(crate) fn insert(&mut self, entry: &PoolEntry<C>) -> bool {
        let mut conflict = false;

        for cucp in &mut self.command_ucps {
            conflict |= cucp.insert(entry.clone());
        }

        conflict
    }

    /// Removes an entry from the pool
    pub(crate) fn remove(&mut self, entry: &PoolEntry<C>) {
        for cucp in &mut self.command_ucps {
            cucp.remove(entry);
        }
    }

    /// Returns all entries in the pool that conflict with the given entry
    pub(crate) fn all_conflict(&self, entry: &PoolEntry<C>) -> Vec<PoolEntry<C>> {
        self.command_ucps
            .iter()
            .flat_map(|p| p.all_conflict(entry))
            .collect()
    }

    #[cfg(test)]
    /// Gets all entries in the pool
    pub(crate) fn all(&self) -> Vec<PoolEntry<C>> {
        let mut entries = Vec::new();
        for csp in &self.command_ucps {
            entries.extend(csp.all().into_iter());
        }
        entries
    }

    #[cfg(test)]
    /// Returns `true` if the pool is empty
    pub(crate) fn is_empty(&self) -> bool {
        self.command_ucps.iter().all(|ucp| ucp.is_empty())
    }

    /// Clears all entries in the pool
    pub(crate) fn clear(&mut self) {
        for ucp in &mut self.command_ucps {
            ucp.clear();
        }
    }
}
