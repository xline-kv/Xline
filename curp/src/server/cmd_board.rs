use std::{collections::HashMap, sync::Arc};

use event_listener::{Event, EventListener};
use indexmap::{IndexMap, IndexSet};
use parking_lot::RwLock;
use utils::parking_lot_lock::RwLockMap;

use crate::cmd::{Command, ProposeId};

/// Ref to the cmd board
pub(super) type CmdBoardRef<C> = Arc<RwLock<CommandBoard<C>>>;

/// Command board is a buffer to track cmd states and store notifiers for requests that need to wait for a cmd
#[derive(Debug)]
pub(super) struct CommandBoard<C: Command> {
    /// Store all notifiers for execution results
    er_notifiers: HashMap<ProposeId, Event>,
    /// Store all notifiers for after sync results
    asr_notifiers: HashMap<ProposeId, Event>,
    /// The cmd has been received before, this is used for dedup
    pub(super) sync: IndexSet<ProposeId>,
    /// Store all execution results
    pub(super) er_buffer: IndexMap<ProposeId, Result<C::ER, String>>,
    /// Store all after sync results
    pub(super) asr_buffer: IndexMap<ProposeId, Result<C::ASR, String>>,
}

impl<C: Command> CommandBoard<C> {
    /// Create an empty command board
    pub(super) fn new() -> Self {
        Self {
            er_notifiers: HashMap::new(),
            asr_notifiers: HashMap::new(),
            sync: IndexSet::new(),
            er_buffer: IndexMap::new(),
            asr_buffer: IndexMap::new(),
        }
    }

    /// Release notifiers
    pub(super) fn release_notifiers(&mut self) {
        self.er_notifiers
            .drain()
            .for_each(|(_, event)| event.notify(usize::MAX));
        self.asr_notifiers
            .drain()
            .for_each(|(_, event)| event.notify(usize::MAX));
    }

    /// Clear
    pub(super) fn clear(&mut self) {
        self.er_buffer.clear();
        self.asr_buffer.clear();
        self.release_notifiers();
    }

    /// Insert er to internal buffer
    pub(super) fn insert_er(&mut self, id: &ProposeId, er: Result<C::ER, String>) {
        let er_ok = er.is_ok();
        assert!(
            self.er_buffer.insert(id.clone(), er).is_none(),
            "er should not be inserted twice"
        );

        self.notify_er(id);

        // wait_synced response is also ready when execution fails
        if !er_ok {
            self.notify_asr(id);
        }
    }

    /// Insert er to internal buffer
    pub(super) fn insert_asr(&mut self, id: &ProposeId, asr: Result<C::ASR, String>) {
        assert!(
            self.asr_buffer.insert(id.clone(), asr).is_none(),
            "er should not be inserted twice"
        );

        self.notify_asr(id);
    }

    /// Get a listener for execution result
    fn er_listener(&mut self, id: &ProposeId) -> EventListener {
        let event = self
            .er_notifiers
            .entry(id.clone())
            .or_insert_with(Event::new);
        let listener = event.listen();
        if self.er_buffer.contains_key(id) {
            event.notify(usize::MAX);
        }
        listener
    }

    /// Get a listener for after sync result
    fn asr_listener(&mut self, id: &ProposeId) -> EventListener {
        let event = self
            .asr_notifiers
            .entry(id.clone())
            .or_insert_with(Event::new);
        let listener = event.listen();
        if self.asr_buffer.contains_key(id) {
            event.notify(usize::MAX);
        }
        listener
    }

    /// Notify execution results
    fn notify_er(&mut self, id: &ProposeId) {
        if let Some(notifier) = self.er_notifiers.remove(id) {
            notifier.notify(usize::MAX);
        }
    }

    /// Notify `wait_synced` requests
    fn notify_asr(&mut self, id: &ProposeId) {
        if let Some(notifier) = self.asr_notifiers.remove(id) {
            notifier.notify(usize::MAX);
        }
    }

    /// Wait for an execution result
    pub(super) async fn wait_for_er(cb: &CmdBoardRef<C>, id: &ProposeId) -> Result<C::ER, String> {
        loop {
            if let Some(er) = cb.map_read(|cb_r| cb_r.er_buffer.get(id).cloned()) {
                return er;
            }
            let listener = cb.write().er_listener(id);
            listener.await;
        }
    }

    /// Wait for an after sync result
    pub(super) async fn wait_for_er_asr(
        cb: &CmdBoardRef<C>,
        id: &ProposeId,
    ) -> (Result<C::ER, String>, Option<Result<C::ASR, String>>) {
        loop {
            {
                let cb_r = cb.read();
                match (cb_r.er_buffer.get(id), cb_r.asr_buffer.get(id)) {
                    (Some(er), None) if er.is_err() => return (er.clone(), None),
                    (Some(er), Some(asr)) => return (er.clone(), Some(asr.clone())),
                    _ => {}
                }
            }
            let listener = cb.write().asr_listener(id);
            listener.await;
        }
    }
}
