use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use event_listener::Event;
use indexmap::IndexMap;
use parking_lot::RwLock;

use crate::{
    cmd::{Command, ProposeId},
    error::ExecuteError,
};

/// Ref to the cmd board
pub(super) type CmdBoardRef<C> = Arc<RwLock<CommandBoard<C>>>;

/// Command board is a buffer to track cmd states and store notifiers for requests that need to wait for a cmd
pub(super) struct CommandBoard<C: Command> {
    /// Store all notifiers for wait_synced requests
    pub(super) notifiers: HashMap<ProposeId, Event>,
    /// Whether the cmd needs execution when after sync
    pub(super) needs_exe: HashSet<ProposeId>,
    /// Store all execution results
    pub(super) er_buffer: IndexMap<ProposeId, Result<C::ER, ExecuteError>>,
    /// Store all after sync results
    pub(super) asr_buffer: IndexMap<ProposeId, Result<C::ASR, ExecuteError>>,
}

impl<C: Command> CommandBoard<C> {
    /// Create an empty command board
    pub(super) fn new() -> Self {
        Self {
            notifiers: HashMap::new(),
            needs_exe: HashSet::new(),
            er_buffer: IndexMap::new(),
            asr_buffer: IndexMap::new(),
        }
    }

    /// Release notifiers
    pub(super) fn release_notifiers(&mut self) {
        self.notifiers
            .drain()
            .for_each(|(_, event)| event.notify(usize::MAX));
    }

    /// Clear
    pub(super) fn clear(&mut self) {
        self.needs_exe.clear();
        self.er_buffer.clear();
        self.asr_buffer.clear();
        self.release_notifiers();
    }

    /// Insert er to internal buffer
    pub(super) fn insert_er(&mut self, id: &ProposeId, er: Result<C::ER, ExecuteError>) {
        let er_ok = er.is_ok();
        assert!(
            self.er_buffer.insert(id.clone(), er).is_none(),
            "er should not be inserted twice"
        );

        if !er_ok {
            self.notify(id);
        }
    }

    /// Insert er to internal buffer
    pub(super) fn insert_asr(&mut self, id: &ProposeId, asr: Result<C::ASR, ExecuteError>) {
        assert!(
            self.asr_buffer.insert(id.clone(), asr).is_none(),
            "er should not be inserted twice"
        );

        self.notify(id);
    }

    /// Notify `wait_synced` requests
    pub(super) fn notify(&self, id: &ProposeId) {
        if let Some(notifier) = self.notifiers.get(id) {
            notifier.notify(usize::MAX);
        }
    }
}
