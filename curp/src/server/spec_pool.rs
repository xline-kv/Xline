use std::collections::{HashMap, VecDeque};

use tokio::time::Instant;
use tracing::debug;

use crate::cmd::{Command, ProposeId};

/// The speculative pool that stores commands that might be executed speculatively
#[derive(Debug)]
pub(crate) struct SpeculativePool<C> {
    /// Store
    pub(crate) pool: VecDeque<C>,
    /// Store the ids of commands that have completed backend syncing, but not in the local speculative pool. It'll prevent the late arrived commands.
    pub(crate) ready: HashMap<ProposeId, Instant>,
}

impl<C: Command + 'static> SpeculativePool<C> {
    /// Create a new speculative pool
    pub(crate) fn new() -> Self {
        Self {
            pool: VecDeque::new(),
            ready: HashMap::new(),
        }
    }

    /// Push a new command into spec pool if it has not been marked ready
    pub(crate) fn push(&mut self, cmd: C) {
        if self.ready.remove(cmd.id()).is_none() {
            debug!("insert cmd {:?} to spec pool", cmd.id());
            self.pool.push_back(cmd);
        }
    }

    /// Check whether the command pool has conflict with the new command
    pub(crate) fn has_conflict_with(&self, cmd: &C) -> bool {
        self.pool.iter().any(|spec_cmd| spec_cmd.is_conflict(cmd))
    }

    /// Try to remove the command from spec pool and mark it ready.
    /// There could be no such command in the following situations:
    /// * When the proposal arrived, the command conflicted with speculative pool and was not stored in it.
    /// * The command has committed. But the fast round proposal has not arrived at the client or failed to arrive.
    /// To prevent the server from returning error in the second situation when the fast proposal finally arrives, we mark the command ready.
    pub(crate) fn mark_ready(&mut self, cmd_id: &ProposeId) {
        debug!("remove cmd {:?} from spec pool", cmd_id);
        if self
            .pool
            .iter()
            .position(|s| s.id() == cmd_id)
            .and_then(|index| self.pool.swap_remove_back(index))
            .is_none()
        {
            debug!("Cmd {:?} is marked ready", cmd_id);
            assert!(
                self.ready.insert(cmd_id.clone(), Instant::now()).is_none(),
                "Cmd {:?} is already in ready pool",
                cmd_id
            );
        };
    }
}
