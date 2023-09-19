#![allow(unused)]

use std::{collections::HashMap, collections::HashSet, sync::Arc};

use event_listener::{Event, EventListener};
use indexmap::{IndexMap, IndexSet};
use parking_lot::RwLock;
use utils::parking_lot_lock::RwLockMap;

use crate::cmd::{Command, ProposeId};

/// Ref to the cmd board
pub(super) type CmdBoardRef<C> = Arc<RwLock<CommandBoard<C>>>;

/// Track results for a command
#[derive(Debug, Default)]
pub(super) struct ResultTracker {
    /// First incomplete seq num, it will be advanced by client
    first_incomplete: u64,
    /// inflight seq nums proposed by the client
    seq_nums: HashSet<u64>,
}

impl ResultTracker {
    /// Check duplicated
    fn filter_dup(&mut self, seq_num: u64) -> bool {
        if seq_num < self.first_incomplete {
            return true;
        }
        !self.seq_nums.insert(seq_num)
    }

    /// Advance `last_incomplete` to `seq_num`
    fn advance_confirm_to(&mut self, seq_num: u64) {
        // The ACK request may out-of-order, resulting in the server's
        // first_incomplete fallback, which would be very weird, so here the maximum value is taken.
        if seq_num <= self.first_incomplete {
            return;
        }
        self.first_incomplete = seq_num;
        self.seq_nums
            .retain(|in_process| *in_process >= self.first_incomplete);
    }
}

/// Command board is a buffer to track cmd states and store notifiers for requests that need to wait for a cmd
#[derive(Debug)]
pub(super) struct CommandBoard<C: Command> {
    /// Store all notifiers for execution results
    er_notifiers: HashMap<ProposeId, Event>,
    /// Store all notifiers for after sync results
    asr_notifiers: HashMap<ProposeId, Event>,
    /// Store the shutdown notifier
    shutdown_notifier: Event,
    /// The result trackers track all cmd, this is used for dedup
    trackers: HashMap<String, ResultTracker>,
    /// Store all execution results
    pub(super) er_buffer: IndexMap<ProposeId, Result<C::ER, C::Error>>,
    /// Store all after sync results
    pub(super) asr_buffer: IndexMap<ProposeId, Result<C::ASR, C::Error>>,
}

impl<C: Command> CommandBoard<C> {
    /// Create an empty command board
    pub(super) fn new() -> Self {
        Self {
            er_notifiers: HashMap::new(),
            asr_notifiers: HashMap::new(),
            shutdown_notifier: Event::new(),
            trackers: HashMap::new(),
            er_buffer: IndexMap::new(),
            asr_buffer: IndexMap::new(),
        }
    }

    /// filter duplication, return true if duplicated
    pub(super) fn filter_dup(&mut self, client_id: &str, seq_num: u64) -> bool {
        let tracker = self
            .trackers
            .entry(client_id.to_owned())
            .or_insert_with(ResultTracker::default);
        tracker.filter_dup(seq_num)
    }

    /// Process ack for a request
    pub(super) fn process_ack(&mut self, client_id: &str, first_incomplete: u64) {
        let tracker = self
            .trackers
            .entry(client_id.to_owned())
            .or_insert_with(ResultTracker::default);
        tracker.advance_confirm_to(first_incomplete);
    }

    /// Remove client result tracker from trackers if it is expired
    pub(super) fn client_expired(&mut self, client_id: &str) {
        let _ig = self.trackers.remove(client_id);
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

    /// Clear, called when leader retires
    pub(super) fn clear(&mut self) {
        self.er_buffer.clear();
        self.asr_buffer.clear();
        self.trackers.clear();
        self.release_notifiers();
    }

    /// Insert er to internal buffer
    pub(super) fn insert_er(&mut self, id: &ProposeId, er: Result<C::ER, C::Error>) {
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
    pub(super) fn insert_asr(&mut self, id: &ProposeId, asr: Result<C::ASR, C::Error>) {
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

    /// Get a listener for shutdown
    fn shutdown_listener(&mut self) -> EventListener {
        self.shutdown_notifier.listen()
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

    /// Notify `shutdown` requests
    pub(super) fn notify_shutdown(&mut self) {
        self.shutdown_notifier.notify(usize::MAX);
    }

    /// Wait for an execution result
    pub(super) async fn wait_for_er(
        cb: &CmdBoardRef<C>,
        id: &ProposeId,
    ) -> Result<C::ER, C::Error> {
        loop {
            if let Some(er) = cb.map_read(|cb_r| cb_r.er_buffer.get(id).cloned()) {
                return er;
            }
            let listener = cb.write().er_listener(id);
            listener.await;
        }
    }

    /// Wait for an execution result
    pub(super) async fn wait_for_shutdown_synced(cb: &CmdBoardRef<C>) {
        let listener = cb.write().shutdown_listener();
        listener.await;
    }

    /// Wait for an after sync result
    pub(super) async fn wait_for_er_asr(
        cb: &CmdBoardRef<C>,
        id: &ProposeId,
    ) -> (Result<C::ER, C::Error>, Option<Result<C::ASR, C::Error>>) {
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
