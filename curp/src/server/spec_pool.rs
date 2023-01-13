use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use tracing::debug;

use crate::cmd::{Command, ProposeId};

/// A reference to the speculative pool
pub(super) type SpecPoolRef<C> = Arc<Mutex<SpeculativePool<C>>>;

/// The speculative pool that stores commands that might be executed speculatively
#[derive(Debug)]
pub(super) struct SpeculativePool<C> {
    /// Store
    pub(super) pool: HashMap<ProposeId, Arc<C>>,
}

impl<C: Command + 'static> SpeculativePool<C> {
    /// Create a new speculative pool
    pub(super) fn new() -> Self {
        Self {
            pool: HashMap::new(),
        }
    }

    /// Push a new command into spec pool if it has no conflict
    pub(super) fn try_insert(&mut self, cmd: Arc<C>) -> bool {
        if self.has_conflict_with(cmd.as_ref()) {
            false
        } else {
            debug!("insert cmd {:?} to spec pool", cmd.id());
            assert!(
                self.pool.insert(cmd.id().clone(), cmd).is_none(),
                "a cmd should be inserted to spec pool twice"
            );
            true
        }
    }

    /// Check whether the command pool has conflict with the new command
    fn has_conflict_with(&self, cmd: &C) -> bool {
        self.pool.values().any(|spec_cmd| spec_cmd.is_conflict(cmd))
    }

    /// Try to remove the command from spec pool
    pub(super) fn try_remove(&mut self, cmd_id: &ProposeId) {
        if self.pool.remove(cmd_id).is_some() {
            debug!("Cmd {:?} is removed from spec pool", cmd_id);
        };
    }
}
