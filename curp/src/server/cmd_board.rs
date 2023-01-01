use std::collections::HashMap;

use event_listener::Event;
use indexmap::IndexMap;

use crate::{cmd::ProposeId, rpc::WaitSyncedResponse};

/// Command board is a buffer to store command execution result for `wait_synced` requests
// TODO: GC
pub(super) struct CommandBoard {
    /// Stores all notifiers for wait_synced requests
    pub(super) notifiers: HashMap<ProposeId, Event>,
    /// Stores all command states
    pub(super) cmd_states: IndexMap<ProposeId, CmdState>,
}

impl CommandBoard {
    /// Create an empty command board
    pub(super) fn new() -> Self {
        Self {
            notifiers: HashMap::new(),
            cmd_states: IndexMap::new(),
        }
    }

    /// Release notifiers
    pub(super) fn release_notifiers(&mut self) {
        self.notifiers
            .drain()
            .for_each(|(_, event)| event.notify(usize::MAX));
    }
}

/// The state of a command in cmd watch board
/// (`EarlyArrive` -> ) `Execute` -> `AfterSync` -> `FinalResponse`
// TODO: this struct might be removed. We don't need to store whether the command needs execution after sync in one place. We can attach it to SyncMessage.
#[derive(Debug)]
pub(super) enum CmdState {
    /// Request for cmd sync result arrives earlier than the cmd itself
    EarlyArrive,
    /// Command still needs execute
    Execute,
    /// Command still needs not execute
    AfterSync,
    /// Command gotten the final result
    FinalResponse(Result<WaitSyncedResponse, bincode::Error>),
}
