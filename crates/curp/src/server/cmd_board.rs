#![allow(unused)] // TODO remove

use std::{collections::HashMap, sync::Arc};

use event_listener::{Event, EventListener};
use indexmap::{IndexMap, IndexSet};
use parking_lot::RwLock;
use utils::parking_lot_lock::RwLockMap;

use crate::{cmd::Command, rpc::ProposeId, tracker::Tracker};

/// Ref to the cmd board
pub(super) type CmdBoardRef<C> = Arc<RwLock<CommandBoard<C>>>;

/// Command board is a buffer to track cmd states and store notifiers for requests that need to wait for a cmd
#[derive(Debug)]
pub(super) struct CommandBoard<C: Command> {
    /// Store all notifiers for execution results
    er_notifiers: HashMap<ProposeId, Event>,
    /// Store all notifiers for after sync results
    asr_notifiers: HashMap<ProposeId, Event>,
    /// Store the shutdown notifier
    shutdown_notifier: Event,
    /// Store all notifiers for conf change results
    conf_notifier: HashMap<ProposeId, Event>,
    /// The result trackers track all cmd, this is used for dedup
    pub(super) trackers: HashMap<u64, Tracker>,
    /// Store all conf change propose ids
    pub(super) conf_buffer: IndexSet<ProposeId>,
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
            conf_notifier: HashMap::new(),
            conf_buffer: IndexSet::new(),
        }
    }

    /// Get the tracker for a client id
    pub(super) fn tracker(&mut self, client_id: u64) -> &mut Tracker {
        self.trackers.entry(client_id).or_default()
    }

    /// Remove client result tracker from trackers if it is expired
    pub(super) fn client_expired(&mut self, client_id: u64) {
        let _ig = self.trackers.remove(&client_id);
    }

    /// Release notifiers
    pub(super) fn release_notifiers(&mut self) {
        self.er_notifiers.drain().for_each(|(_, event)| {
            let _ignore = event.notify(usize::MAX);
        });
        self.asr_notifiers.drain().for_each(|(_, event)| {
            let _ignore = event.notify(usize::MAX);
        });
    }

    /// Clear, called when leader retires
    pub(super) fn clear(&mut self) {
        self.er_buffer.clear();
        self.asr_buffer.clear();
        self.trackers.clear();
        self.release_notifiers();
    }

    /// Insert er to internal buffer
    pub(super) fn insert_er(&mut self, id: ProposeId, er: Result<C::ER, C::Error>) {
        let er_ok = er.is_ok();
        assert!(
            self.er_buffer.insert(id, er).is_none(),
            "er should not be inserted twice"
        );

        self.notify_er(&id);

        // wait_synced response is also ready when execution fails
        if !er_ok {
            self.notify_asr(&id);
        }
    }

    /// Insert asr to internal buffer
    pub(super) fn insert_asr(&mut self, id: ProposeId, asr: Result<C::ASR, C::Error>) {
        assert!(
            self.asr_buffer.insert(id, asr).is_none(),
            "asr should not be inserted twice"
        );

        self.notify_asr(&id);
    }

    /// Insert conf change result to internal buffer
    pub(super) fn insert_conf(&mut self, id: ProposeId) {
        assert!(
            self.conf_buffer.insert(id),
            "conf should not be inserted twice"
        );

        self.notify_conf(&id);
    }

    /// Get a listener for execution result
    fn er_listener(&mut self, id: ProposeId) -> EventListener {
        let event = self.er_notifiers.entry(id).or_default();
        let listener = event.listen();
        if self.er_buffer.contains_key(&id) {
            let _ignore = event.notify(usize::MAX);
        }
        listener
    }

    /// Get a listener for shutdown
    fn shutdown_listener(&mut self) -> EventListener {
        self.shutdown_notifier.listen()
    }

    /// Get a listener for after sync result
    fn asr_listener(&mut self, id: ProposeId) -> EventListener {
        let event = self.asr_notifiers.entry(id).or_default();
        let listener = event.listen();
        if self.asr_buffer.contains_key(&id) {
            let _ignore = event.notify(usize::MAX);
        }
        listener
    }

    /// Get a listener for conf change result
    fn conf_listener(&mut self, id: ProposeId) -> EventListener {
        let event = self.conf_notifier.entry(id).or_default();
        let listener = event.listen();
        if self.conf_buffer.contains(&id) {
            let _ignore = event.notify(usize::MAX);
        }
        listener
    }

    /// Notify execution results
    fn notify_er(&mut self, id: &ProposeId) {
        if let Some(notifier) = self.er_notifiers.remove(id) {
            let _ignore = notifier.notify(usize::MAX);
        }
    }

    /// Notify `wait_synced` requests
    fn notify_asr(&mut self, id: &ProposeId) {
        if let Some(notifier) = self.asr_notifiers.remove(id) {
            let _ignore = notifier.notify(usize::MAX);
        }
    }

    /// Notify `shutdown` requests
    pub(super) fn notify_shutdown(&mut self) {
        let _ignore = self.shutdown_notifier.notify(usize::MAX);
    }

    /// Notify `wait_synced` requests
    fn notify_conf(&mut self, id: &ProposeId) {
        if let Some(notifier) = self.conf_notifier.remove(id) {
            let _ignore = notifier.notify(usize::MAX);
        }
    }

    /// Wait for an execution result
    pub(super) async fn wait_for_er(cb: &CmdBoardRef<C>, id: ProposeId) -> Result<C::ER, C::Error> {
        loop {
            if let Some(er) = cb.map_read(|cb_r| cb_r.er_buffer.get(&id).cloned()) {
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
        id: ProposeId,
    ) -> (Result<C::ER, C::Error>, Result<C::ASR, C::Error>) {
        loop {
            {
                let cb_r = cb.read();
                if let (Some(er), Some(asr)) = (cb_r.er_buffer.get(&id), cb_r.asr_buffer.get(&id)) {
                    return (er.clone(), asr.clone());
                }
            }
            let listener = cb.write().asr_listener(id);
            listener.await;
        }
    }

    /// Wait for an conf change result
    pub(super) async fn wait_for_conf(cb: &CmdBoardRef<C>, id: ProposeId) {
        loop {
            if let Some(_ccr) = cb.map_read(|cb_r| cb_r.conf_buffer.get(&id).copied()) {
                return;
            }
            let listener = cb.write().conf_listener(id);
            listener.await;
        }
    }
}
