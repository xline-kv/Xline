use std::{ops::Not, sync::Arc};

use parking_lot::Mutex;

use crate::{
    cmd::{Command, CommandExecutor},
    message::{Propose, ProposeError, ProposeResponse},
    util::MutexMap,
};

/// The server that handles client request and server consensus protocol
#[derive(Debug, Clone)]
pub struct Server<C: Command, CE: CommandExecutor<C>> {
    /// The speculative cmd pool
    spec: Arc<Mutex<Vec<C>>>,
    /// Command executor
    cmd_exeutor: Arc<CE>,
}

impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Default for Server<C, CE> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[allow(missing_docs)]
#[madsim::service]
impl<C: 'static + Command, CE: 'static + CommandExecutor<C>> Server<C, CE> {
    /// Create a new server instance
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            spec: Arc::new(Mutex::new(Vec::new())),
            cmd_exeutor: Arc::new(CE::new()),
        }
    }

    /// Check if the `cmd` conflict with any conflicts in the speculative cmd pool
    fn add_spec(&self, cmd: &C) -> bool {
        self.spec.map(|mut spec| {
            let can_insert = spec
                .iter()
                .map(|spec_cmd| spec_cmd.is_conflict(cmd))
                .any(|has| has)
                .not();

            if can_insert {
                spec.push(cmd.clone());
            }

            can_insert
        })
    }

    /// Propose request handler
    #[rpc]
    async fn propose(&self, p: Propose<C>) -> ProposeResponse<C> {
        if self.add_spec(p.cmd()) {
            p.cmd()
                .execute(self.cmd_exeutor.as_ref())
                .await
                .map_or_else(
                    |err| ProposeResponse::Error(ProposeError::ExecutionError(err.to_string())),
                    |er| ProposeResponse::ReturnValue(er),
                )
        } else {
            ProposeResponse::Error(ProposeError::KeyConflict)
        }
    }
}
