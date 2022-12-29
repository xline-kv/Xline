use std::{collections::HashMap, sync::Arc};

use event_listener::Event;
use parking_lot::RwLock;
use tokio::time::Instant;
use tracing::debug;

use super::ServerRole;
use crate::{
    cmd::Command,
    log::LogEntry,
    message::{ServerId, TermNum},
    server::cmd_board::CmdBoardRef,
};

/// State of the server
pub(super) struct State<C: Command + 'static> {
    /// Id of the server
    id: ServerId,
    /// Id of the leader. None if in election state.
    pub(super) leader_id: Option<ServerId>,
    /// Role of the server
    role: ServerRole,
    /// Current term
    pub(super) term: TermNum,
    /// Consensus log
    pub(super) log: Vec<LogEntry<C>>,
    /// Candidate id that received vote in current term
    pub(super) voted_for: Option<String>,
    /// Votes received in the election
    pub(super) votes_received: usize,
    /// Index of highest log entry known to be committed
    pub(super) commit_index: usize,
    /// Index of highest log entry applied to state machine
    pub(super) last_applied: usize,
    /// For each server, index of the next log entry to send to that server
    // TODO: this should be indexed by server id and changed into a vec for efficiency
    pub(super) next_index: HashMap<ServerId, usize>,
    /// For each server, index of highest log entry known to be replicated on server
    pub(super) match_index: HashMap<ServerId, usize>,
    /// Other server ids and addresses
    pub(super) others: HashMap<ServerId, String>,
    /// Trigger when server role changes
    pub(super) role_trigger: Arc<Event>,
    /// Trigger when there might be some logs to commit
    pub(super) commit_trigger: Arc<Event>,
    /// Trigger when a new leader needs to calibrate its followers
    pub(super) calibrate_trigger: Arc<Event>,
    /// Trigger when append_entires are sent and no heartbeat is needed for a while
    pub(super) heartbeat_reset_trigger: Arc<Event>,
    // TODO: clean up the board when the size is too large
    /// Cmd watch board for tracking the cmd sync results
    pub(super) cmd_board: CmdBoardRef<C>,
    /// Last time a rpc is received.
    pub(super) last_rpc_time: Arc<RwLock<Instant>>,
}

impl<C: Command + 'static> State<C> {
    /// Init server state
    pub(super) fn new(
        id: ServerId,
        role: ServerRole,
        others: HashMap<ServerId, String>,
        cmd_board: CmdBoardRef<C>,
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
            heartbeat_reset_trigger: Arc::new(Event::new()),
            cmd_board,
            last_rpc_time,
        }
    }

    /// Is leader?
    pub(super) fn is_leader(&self) -> bool {
        matches!(self.role, ServerRole::Leader)
    }

    /// Last log index
    #[allow(clippy::integer_arithmetic)] // log.len() >= 1 because we have a fake log[0]
    pub(super) fn last_log_index(&self) -> usize {
        self.log.len() - 1
    }

    /// Last log term
    #[allow(dead_code, clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0]
    pub(super) fn last_log_term(&self) -> TermNum {
        self.log[self.log.len() - 1].term()
    }

    /// Need to commit
    pub(super) fn need_commit(&self) -> bool {
        self.last_applied < self.commit_index
    }

    /// Update to `term`
    pub(super) fn update_to_term(&mut self, term: TermNum) {
        debug_assert!(self.term <= term);
        self.term = term;
        self.set_role(ServerRole::Follower);
        self.voted_for = None;
        self.votes_received = 0;
        self.leader_id = None;
        debug!("updated to term {term}");
    }

    /// Set server role
    pub(super) fn set_role(&mut self, role: ServerRole) {
        let prev_role = self.role;
        self.role = role;
        if prev_role != role {
            self.role_trigger.notify(usize::MAX);

            // from leader to follower
            if prev_role == ServerRole::Leader {
                self.cmd_board.write().release_notifiers();
            }
        }
    }

    /// Get server role
    pub(super) fn role(&self) -> ServerRole {
        self.role
    }

    /// Get role trigger
    pub(super) fn role_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.role_trigger)
    }

    /// Get commit trigger
    pub(super) fn commit_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.commit_trigger)
    }

    /// Get id
    pub(super) fn id(&self) -> &ServerId {
        &self.id
    }

    /// Get `last_rpc_time`
    pub(super) fn last_rpc_time(&self) -> Arc<RwLock<Instant>> {
        Arc::clone(&self.last_rpc_time)
    }

    /// Get `cmd_board`
    pub(super) fn cmd_board(&self) -> CmdBoardRef<C> {
        Arc::clone(&self.cmd_board)
    }

    /// Get heartbeat reset trigger
    pub(super) fn heartbeat_reset_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.heartbeat_reset_trigger)
    }
}
