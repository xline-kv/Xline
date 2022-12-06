use std::collections::HashMap;

use event_listener::Event;

use crate::{cmd::ProposeId, rpc::WaitSyncedResponse};

/// Command board is a buffer to store command execution result for `wait_synced` requests
// TODO: GC
pub(crate) struct CommandBoard {
    /// Stores all notifiers for wait_synced requests
    pub(crate) notifiers: HashMap<ProposeId, Event>,
    /// Stores all command states
    pub(crate) cmd_states: HashMap<ProposeId, CmdState>,
}

impl CommandBoard {
    /// Create an empty command board
    pub(crate) fn new() -> Self {
        Self {
            notifiers: HashMap::new(),
            cmd_states: HashMap::new(),
        }
    }
}

/// The state of a command in cmd watch board
/// (`EarlyArrive` -> ) `Execute` -> `AfterSync` -> `FinalResponse`
// TODO: this struct might me removed. We don't need to store whether the command needs execution after sync in one place. We can attach it to SyncMessage.
#[derive(Debug)]
pub(crate) enum CmdState {
    /// Request for cmd sync result arrives earlier than the cmd itself
    EarlyArrive,
    /// Command still needs execute
    Execute,
    /// Command still needs not execute
    AfterSync,
    /// Command gotten the final result
    FinalResponse(Result<WaitSyncedResponse, bincode::Error>),
}
