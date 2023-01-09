use std::{collections::HashMap, sync::Arc};

use indexmap::IndexSet;
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
    /// Store the ids of commands that have completed backend syncing, but not in the local speculative pool. It'll prevent the late arrived commands.
    pub(super) ready: IndexSet<ProposeId>,
}

impl<C: Command + 'static> SpeculativePool<C> {
    /// Create a new speculative pool
    pub(super) fn new() -> Self {
        Self {
            pool: HashMap::new(),
            ready: IndexSet::new(),
        }
    }

    /// Push a new command into spec pool if it has not been marked ready
    pub(super) fn insert(&mut self, cmd: Arc<C>) {
        if !self.ready.contains(cmd.id()) {
            debug!("insert cmd {:?} to spec pool", cmd.id());
            let _ignored = self.pool.insert(cmd.id().clone(), cmd).is_none();
        }
    }

    /// Check whether the command pool has conflict with the new command
    pub(super) fn has_conflict_with(&self, cmd: &C) -> bool {
        self.pool.values().any(|spec_cmd| spec_cmd.is_conflict(cmd))
    }

    /// Try to remove the command from spec pool and mark it ready.
    /// There could be no such command in the following situations:
    /// * When the proposal arrived, the command conflicted with speculative pool and was not stored in it.
    /// * The command has committed. But the fast round proposal has not arrived at the client or failed to arrive.
    /// To prevent the server from returning error in the second situation when the fast proposal finally arrives, we mark the command ready.
    pub(super) fn mark_ready(&mut self, cmd_id: &ProposeId) {
        debug!("remove cmd {:?} from spec pool", cmd_id);
        if self.pool.remove(cmd_id).is_none() {
            debug!("Cmd {:?} is marked ready", cmd_id);
            assert!(
                self.ready.insert(cmd_id.clone()),
                "Cmd {:?} is already in ready pool",
                cmd_id
            );
        };
    }
}
