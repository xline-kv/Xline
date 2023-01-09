use std::{collections::HashMap, sync::Arc};

use clippy_utilities::NumericCast;
use event_listener::Event;
use parking_lot::RwLock;
use tokio::{sync::broadcast, time::Instant};
use tracing::debug;

use super::ServerRole;
use crate::{
    cmd::Command,
    log::LogEntry,
    message::{ServerId, TermNum},
    server::{
        cmd_board::CmdBoardRef, cmd_execute_worker::CmdExeSenderInterface, spec_pool::SpecPoolRef,
    },
};

/// State of the server
pub(super) struct State<C: Command + 'static, ExeTx: CmdExeSenderInterface<C>> {
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
    /// Trigger when append_entries are sent and no heartbeat is needed for a while
    pub(super) heartbeat_reset_trigger: Arc<Event>,
    /// Cmd watch board for tracking the cmd sync results
    pub(super) cmd_board: CmdBoardRef<C>,
    /// Speculative pool
    pub(super) spec: SpecPoolRef<C>,
    /// Last time a rpc is received.
    pub(super) last_rpc_time: Arc<RwLock<Instant>>,
    /// Leader changes tx
    leader_tx: broadcast::Sender<Option<ServerId>>,
    /// Cmd exe sender
    pub(super) cmd_exe_tx: ExeTx,
}

impl<C: Command + 'static, ExeTx: CmdExeSenderInterface<C>> State<C, ExeTx> {
    /// Init server state
    pub(super) fn new(
        id: ServerId,
        role: ServerRole,
        others: HashMap<ServerId, String>,
        cmd_board: CmdBoardRef<C>,
        spec: SpecPoolRef<C>,
        last_rpc_time: Arc<RwLock<Instant>>,
        cmd_exe_tx: ExeTx,
    ) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for other in others.keys() {
            assert!(next_index.insert(other.clone(), 1).is_none());
            assert!(match_index.insert(other.clone(), 0).is_none());
        }
        let (tx, _) = broadcast::channel(1);
        Self {
            leader_id: matches!(role, ServerRole::Leader).then(|| id.clone()),
            id,
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
            heartbeat_reset_trigger: Arc::new(Event::new()),
            cmd_board,
            spec,
            last_rpc_time,
            leader_tx: tx,
            cmd_exe_tx,
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

    /// Do cleanup and redo logs when the leader retires
    #[allow(clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    fn leader_retires(&mut self) {
        // when a leader retires, it should wipe up speculatively executed cmds by resetting and re-executing
        self.cmd_exe_tx.send_reset();

        let mut board_w = self.cmd_board.write();
        board_w.clear();

        for i in 0..=self.commit_index {
            let log = &self.log[i];
            for cmd in log.cmds() {
                assert!(
                    board_w.needs_exe.insert(cmd.id().clone()),
                    "shouldn't insert needs_exe twice"
                );
                // FIXME: do we need to call after sync or that execution is enough?
                self.cmd_exe_tx
                    .send_after_sync(Arc::clone(cmd), i.numeric_cast());
            }
        }
        // old leader will reset time to prevent itself from starting election immediately
        *self.last_rpc_time.write() = Instant::now();
    }

    /// Update to `term`
    pub(super) fn update_to_term(&mut self, term: TermNum) {
        debug_assert!(self.term <= term);
        if self.is_leader() {
            self.leader_retires();
        }
        self.term = term;
        self.set_role(ServerRole::Follower);
        self.voted_for = None;
        self.votes_received = 0;
        if self.leader_id.is_some() {
            self.leader_id = None;
            let _ig = self.leader_tx.send(None).ok();
        }
        self.leader_id = None;
        debug!("{} updated to term {term}", self.id);
    }

    /// Set server role
    pub(super) fn set_role(&mut self, role: ServerRole) {
        let prev_role = self.role;
        self.role = role;
        if prev_role != role {
            self.role_trigger.notify(usize::MAX);
        }
    }

    /// Set leader
    pub(super) fn set_leader(&mut self, leader_id: ServerId) {
        if self.leader_id.is_none() {
            self.leader_id = Some(leader_id.clone());
            let _ig = self.leader_tx.send(Some(leader_id)).ok();
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

    /// Get channel for leader changes
    pub(super) fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.leader_tx.subscribe()
    }

    /// Get heartbeat reset trigger
    pub(super) fn heartbeat_reset_trigger(&self) -> Arc<Event> {
        Arc::clone(&self.heartbeat_reset_trigger)
    }

    /// Get a reference to speculative pool
    pub(super) fn spec(&self) -> SpecPoolRef<C> {
        Arc::clone(&self.spec)
    }
}

#[cfg_attr(test, allow(clippy::unwrap_used))]
#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    use super::*;
    use crate::{
        server::{
            cmd_board::CommandBoard, cmd_execute_worker::MockCmdExeSenderInterface,
            spec_pool::SpeculativePool,
        },
        test_utils::test_cmd::TestCommand,
    };

    #[tokio::test]
    async fn leader_broadcast() {
        let mut exe_tx = MockCmdExeSenderInterface::default();
        exe_tx.expect_send_reset().returning(|| ());
        let mut state: State<TestCommand, _> = State::new(
            "Foo".to_owned(),
            ServerRole::Leader,
            HashMap::new(),
            Arc::new(RwLock::new(CommandBoard::new())),
            Arc::new(Mutex::new(SpeculativePool::new())),
            Arc::new(RwLock::new(Instant::now())),
            exe_tx,
        );
        let mut rx = state.leader_tx.subscribe();

        state.update_to_term(1);
        assert!(rx.recv().await.unwrap().is_none());
        state.set_leader("S1".to_owned());
        assert_eq!(rx.recv().await.unwrap().unwrap().as_str(), "S1");

        // the subscriber will only receive the newest changes, otherwise it will return delay, and we can receive again
        state.update_to_term(2);
        state.set_leader("S2".to_owned());
        assert!(rx.recv().await.is_err());
        assert_eq!(rx.recv().await.unwrap().unwrap().as_str(), "S2");
    }
}
