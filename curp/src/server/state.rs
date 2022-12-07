use std::{collections::HashMap, sync::Arc};

use event_listener::Event;
use parking_lot::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::debug;

use crate::{
    cmd::Command,
    log::LogEntry,
    message::{ServerId, TermNum},
};

use super::{cmd_board::CommandBoard, ServerRole};

/// State of the server
pub(crate) struct State<C: Command + 'static> {
    /// Id of the server
    id: ServerId,
    /// Id of the leader. None if in election state.
    pub(crate) leader_id: Option<ServerId>,
    /// Role of the server
    role: ServerRole,
    /// Current term
    pub(crate) term: TermNum,
    /// Consensus log
    pub(crate) log: Vec<LogEntry<C>>,
    /// Candidate id that received vote in current term
    pub(crate) voted_for: Option<String>,
    /// Votes received in the election
    pub(crate) votes_received: usize,
    /// Index of highest log entry known to be committed
    pub(crate) commit_index: usize,
    /// Index of highest log entry applied to state machine
    pub(crate) last_applied: usize,
    /// For each server, index of the next log entry to send to that server
    // TODO: this should be indexed by server id and changed into a vec for efficiency
    pub(crate) next_index: HashMap<ServerId, usize>,
    /// For each server, index of highest log entry known to be replicated on server
    pub(crate) match_index: HashMap<ServerId, usize>,
    /// Other server ids and addresses
    pub(crate) others: HashMap<ServerId, String>,
    /// Trigger when server role changes
    pub(crate) role_trigger: Arc<Event>,
    /// Trigger when there might be some logs to commit
    pub(crate) commit_trigger: Arc<Event>,
    /// Trigger when a new leader needs to calibrate its followers
    pub(crate) calibrate_trigger: Arc<Event>,
    // TODO: clean up the board when the size is too large
    /// Cmd watch board for tracking the cmd sync results
    pub(crate) cmd_board: Arc<Mutex<CommandBoard>>,
    /// Last time a rpc is received.
    pub(crate) last_rpc_time: Arc<RwLock<Instant>>,
}

impl<C: Command + 'static> State<C> {
    /// Init server state
    pub(crate) fn new(
        id: ServerId,
        role: ServerRole,
        others: HashMap<ServerId, String>,
        cmd_board: Arc<Mutex<CommandBoard>>,
        last_rpc_time: Arc<RwLock<Instant>>,
    ) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for other in others.keys() {
            assert!(next_index.insert(other.clone(), 1).is_none());
            assert!(match_index.insert(other.clone(), 0).is_none());
        }
        Self {
            id,
            leader_id: None,
            role,
            term: 0,
            log: vec![LogEntry::new(0, &[])], // a fake log[0] will simplify the boundary check significantly
            voted_for: None,
            votes_received: 0,
            commit_index: 0,
            last_applied: 0,
            next_index,
            match_index,
            others,
            role_trigger: Arc::new(Event::new()),
            commit_trigger: Arc::new(Event::new()),
            calibrate_trigger: Arc::new(Event::new()),
            cmd_board,
            last_rpc_time,
        }
    }

    /// Is leader?
    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.role, ServerRole::Leader)
    }

    /// Last log index
    #[allow(clippy::integer_arithmetic)] // log.len() >= 1 because we have a fake log[0]
    pub(crate) fn last_log_index(&self) -> usize {
        self.log.len() - 1
    }

    /// Last log term
    #[allow(dead_code, clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0]
    pub(crate) fn last_log_term(&self) -> TermNum {
        self.log[self.log.len() - 1].term()
    }

    /// Need to commit
    pub(crate) fn need_commit(&self) -> bool {
        self.last_applied < self.commit_index
    }

    /// Update to `term`
    pub(crate) fn update_to_term(&mut self, term: TermNum) {
        debug_assert!(self.term <= term);
        self.term = term;
        self.set_role(ServerRole::Follower);
        self.voted_for = None;
        self.votes_received = 0;
        self.leader_id = None;
        debug!("updated to term {term}");
    }

    /// Set server role
    pub(crate) fn set_role(&mut self, role: ServerRole) {
        let prev_role = self.role;
        self.role = role;
        if prev_role != role {
            self.role_trigger.notify(usize::MAX);
        }
    }

    /// Get server role
    pub(crate) fn role(&self) -> ServerRole {
        self.role
    }

    /// Get role trigger
    pub(crate) fn role_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.role_trigger)
    }

    /// Get commit trigger
    pub(crate) fn commit_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.commit_trigger)
    }

    /// Get id
    pub(crate) fn id(&self) -> &ServerId {
        &self.id
    }

    /// Get `last_rpc_time`
    pub(crate) fn last_rpc_time(&self) -> Arc<RwLock<Instant>> {
        Arc::clone(&self.last_rpc_time)
    }

    /// Get `cmd_board`
    pub(crate) fn cmd_board(&self) -> Arc<Mutex<CommandBoard>> {
        Arc::clone(&self.cmd_board)
    }
}
