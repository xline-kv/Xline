use std::{collections::HashMap, sync::Arc};

use curp_external_api::cmd::ConflictCheck;
use parking_lot::Mutex;
use tracing::{debug, warn};

use crate::{
    cmd::Command,
    rpc::{PoolEntry, ProposeId},
};

/// A reference to the speculative pool
pub(super) type SpecPoolRef<C> = Arc<Mutex<SpeculativePool<C>>>;

/// The speculative pool that stores commands that might be executed speculatively
#[derive(Debug)]
pub(super) struct SpeculativePool<C> {
    /// Store
    pub(super) pool: HashMap<ProposeId, PoolEntry<C>>,
}

impl<C: Command> SpeculativePool<C> {
    /// Create a new speculative pool
    pub(super) fn new() -> Self {
        Self {
            pool: HashMap::new(),
        }
    }

    /// Push a new command into spec pool if it has no conflict. Return Some if it conflicts with spec pool or the cmd is already in the pool.
    pub(super) fn insert(&mut self, entry: PoolEntry<C>) -> Option<PoolEntry<C>> {
        if self.has_conflict_with(&entry) {
            Some(entry)
        } else {
            let id = entry.id;
            let result = self.pool.insert(id, entry);
            if result.is_none() {
                debug!("insert cmd({id}) into spec pool");
            } else {
                warn!("cmd {id:?} is inserted into spec pool twice");
            }
            result
        }
    }

    /// Check whether the command pool has conflict with the new command
    fn has_conflict_with(&self, cmd: &PoolEntry<C>) -> bool {
        self.pool.values().any(|spec_cmd| spec_cmd.is_conflict(cmd))
    }

    /// Remove the command from spec pool
    pub(super) fn remove(&mut self, cmd_id: &ProposeId) {
        if self.pool.remove(cmd_id).is_some() {
            debug!("cmd({cmd_id}) is removed from spec pool");
        } else {
            // this happens when a cmd was not added to the spec pool because of conflict
            // or the fast round proposal never arrived at the server
            debug!("cmd({cmd_id}) is not in spec pool");
        };
    }
}
