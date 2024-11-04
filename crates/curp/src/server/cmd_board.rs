use std::sync::Arc;

use event_listener::{Event, EventListener};
use parking_lot::RwLock;

/// Ref to the cmd board
pub(super) type CmdBoardRef = Arc<RwLock<CommandBoard>>;

/// Command board is a buffer to track cmd states and store notifiers for requests that need to wait for a cmd
#[derive(Debug)]
pub(super) struct CommandBoard {
    /// Store the shutdown notifier
    shutdown_notifier: Event,
}

impl CommandBoard {
    /// Create an empty command board
    pub(super) fn new() -> Self {
        Self {
            shutdown_notifier: Event::new(),
        }
    }

    /// Get a listener for shutdown
    fn shutdown_listener(&mut self) -> EventListener {
        self.shutdown_notifier.listen()
    }

    /// Notify `shutdown` requests
    pub(super) fn notify_shutdown(&mut self) {
        let _ignore = self.shutdown_notifier.notify(usize::MAX);
    }

    /// Wait for an execution result
    pub(super) async fn wait_for_shutdown_synced(cb: &CmdBoardRef) {
        let listener = cb.write().shutdown_listener();
        listener.await;
    }
}
