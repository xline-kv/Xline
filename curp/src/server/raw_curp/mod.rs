//! READ THIS BEFORE YOU START WRITING CODE FOR THIS MODULE
//! To avoid deadlock, let's make some rules:
//! 1. To group similar functions, I divide Curp impl into three scope: one for utils(don't grab lock here), one for tick, one for handlers
//! 2. Lock order should be:
//!     1. self.st
//!     2. self.cst
//!     3. self.log

#![allow(clippy::similar_names)] // st, lst, cst is similar but not confusing
#![allow(clippy::integer_arithmetic)] // u64 is large enough and won't overflow

use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use clippy_utilities::NumericCast;
use event_listener::Event;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{
    debug, error,
    log::{log_enabled, Level},
};
use utils::{
    config::CurpConfig,
    parking_lot_lock::{MutexMap, RwLockMap},
};

use self::{
    log::Log,
    state::{CandidateState, LeaderState, State},
};
use super::cmd_worker::CEEventTxApi;
use crate::{
    cmd::{Command, ProposeId},
    error::ProposeError,
    log_entry::LogEntry,
    rpc::{IdSet, ReadState},
    server::{cmd_board::CmdBoardRef, spec_pool::SpecPoolRef},
    snapshot::{Snapshot, SnapshotMeta},
    LogIndex, ServerId,
};

/// Curp state
mod state;

/// Curp log
mod log;

/// Uncommitted pool type
pub(super) type UncommittedPool<C> = HashMap<ProposeId, Arc<C>>;

/// Reference to uncommitted pool
type UncommittedPoolRef<C> = Arc<Mutex<UncommittedPool<C>>>;

/// The curp state machine
#[derive(Debug)]
pub(super) struct RawCurp<C: Command> {
    /// Curp state
    st: RwLock<State>,
    /// Additional leader state
    lst: LeaderState,
    /// Additional candidate state
    cst: Mutex<CandidateState<C>>,
    /// Curp logs
    log: RwLock<Log<C>>,
    /// Relevant context
    ctx: Context<C>,
}

/// Actions of syncing
pub(super) enum SyncAction<C> {
    /// Use append entires to calibrate
    AppendEntries(AppendEntries<C>),
    /// Use snapshot to calibrate
    Snapshot(oneshot::Receiver<Snapshot>),
}

/// Invoked by candidates to gather votes
#[derive(Clone)]
pub(super) struct Vote {
    /// Candidate's term
    pub(super) term: u64,
    /// Candidate's Id
    pub(super) candidate_id: ServerId,
    /// Candidate's last log index
    pub(super) last_log_index: LogIndex,
    /// Candidate's last log term
    pub(super) last_log_term: u64,
}

/// Invoked by leader to replicate log entries; also used as heartbeat
pub(super) struct AppendEntries<C> {
    /// Leader's term
    pub(super) term: u64,
    /// Leader's id
    pub(super) leader_id: ServerId,
    /// Index of log entry immediately preceding new ones
    pub(super) prev_log_index: LogIndex,
    /// Term of log entry immediately preceding new ones
    pub(super) prev_log_term: u64,
    /// Leader's commit index
    pub(super) leader_commit: LogIndex,
    /// New entries to be appended to the follower
    pub(super) entries: Vec<LogEntry<C>>,
}

/// Curp Role
#[derive(Debug, Clone, Copy, PartialEq)]
enum Role {
    /// Follower
    Follower,
    /// Candidate
    Candidate,
    /// Leader
    Leader,
}

/// Relevant context for Curp
struct Context<C: Command> {
    /// Id of the server
    id: ServerId,
    /// Other server ids
    others: HashSet<ServerId>,
    /// Config
    cfg: Arc<CurpConfig>,
    /// Cmd board for tracking the cmd sync results
    cb: CmdBoardRef<C>,
    /// Speculative pool
    sp: SpecPoolRef<C>,
    /// Uncommitted pool
    ucp: UncommittedPoolRef<C>,
    /// Tx to send leader changes
    leader_tx: broadcast::Sender<Option<ServerId>>,
    /// Election tick
    election_tick: AtomicU8,
    /// Tx to send cmds to execute and do after sync
    cmd_tx: Arc<dyn CEEventTxApi<C>>,
    /// Followers sync event trigger
    sync_events: HashMap<ServerId, Arc<Event>>,
    /// Become leader event
    leader_event: Arc<Event>,
}

impl<C: Command> Debug for Context<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("id", &self.id)
            .field("others", &self.others)
            .field("config", &self.cfg)
            .field("cb", &self.cb)
            .field("sp", &self.sp)
            .field("leader_tx", &self.leader_tx)
            .field("election_tick", &self.election_tick)
            .finish()
    }
}

// Tick
impl<C: 'static + Command> RawCurp<C> {
    /// Tick
    pub(super) fn tick_election(&self) -> Option<Vote> {
        let timeout = {
            let st_r = self.st.read();
            match st_r.role {
                Role::Follower => st_r.follower_timeout_ticks,
                Role::Candidate => st_r.candidate_timeout_ticks,
                Role::Leader => return None,
            }
        };
        let tick = self.ctx.election_tick.fetch_add(1, Ordering::AcqRel);
        if tick < timeout {
            return None;
        }
        let vote =
            self.become_candidate(&mut self.st.write(), &mut self.cst.lock(), &self.log.read());
        Some(vote)
    }
}

// Curp handlers
impl<C: 'static + Command> RawCurp<C> {
    /// Handle `propose` request
    /// Return `((leader_id, term), Ok(spec_executed))` if the proposal succeeds, `Ok(true)` if leader speculatively executed the command
    /// Return `((leader_id, term), Err(ProposeError))` if the cmd cannot be speculatively executed or is duplicated
    #[allow(clippy::type_complexity)] // it's clear
    pub(super) fn handle_propose(
        &self,
        cmd: Arc<C>,
    ) -> ((Option<ServerId>, u64), Result<bool, ProposeError>) {
        debug!("{} gets proposal for cmd({})", self.id(), cmd.id());
        let mut conflict = self
            .ctx
            .sp
            .map_lock(|mut spec_l| spec_l.insert(Arc::clone(&cmd)).is_some());

        let st_r = self.st.read();
        let info = (st_r.leader_id.clone(), st_r.term);

        // Non-leader doesn't need to sync or execute
        if st_r.role != Role::Leader {
            return (
                info,
                if conflict {
                    Err(ProposeError::KeyConflict)
                } else {
                    Ok(false)
                },
            );
        }

        if !self
            .ctx
            .cb
            .map_write(|mut cb_w| cb_w.sync.insert(cmd.id().clone()))
        {
            return (info, Err(ProposeError::Duplicated));
        }

        // leader also needs to check if the cmd conflicts un-synced commands
        conflict |= self.ctx.ucp.map_lock(|mut ucp_l| {
            let conflict_uncommitted = ucp_l.values().any(|c| c.is_conflict(cmd.as_ref()));
            assert!(
                ucp_l.insert(cmd.id().clone(), Arc::clone(&cmd)).is_none(),
                "cmd should never be inserted to uncommitted pool twice"
            );
            conflict_uncommitted
        });
        let mut log_w = self.log.write();

        let index = match log_w.push_cmd(st_r.term, Arc::clone(&cmd)) {
            Ok(index) => {
                debug!("{} gets new log[{index}]", self.id());
                index
            }
            Err(e) => {
                return (info, Err(e.into()));
            }
        };

        if !conflict {
            log_w.last_exe = index;
            self.ctx.cmd_tx.send_sp_exe(cmd, index);
        }

        self.ctx.sync_events.iter().for_each(|(id, event)| {
            let next = self.lst.get_next_index(id);
            if next > log_w.base_index && log_w.has_next_batch(next) {
                event.notify(1);
            }
        });

        (
            info,
            if conflict {
                Err(ProposeError::KeyConflict)
            } else {
                Ok(true)
            },
        )
    }

    /// Handle `append_entries`
    /// Return `Ok(term)` if succeeds
    /// Return `Err(term, hint_index)` if fails
    pub(super) fn handle_append_entries(
        &self,
        term: u64,
        leader_id: String,
        prev_log_index: LogIndex,
        prev_log_term: u64,
        entries: Vec<LogEntry<C>>,
        leader_commit: LogIndex,
    ) -> Result<u64, (u64, LogIndex)> {
        debug!(
            "{} received append_entries from {}: term({}), commit({}), prev_log_index({}), prev_log_term({}), {} entries",
            self.id(), leader_id, term, leader_commit, prev_log_index, prev_log_term, entries.len()
        );

        // validate term and set leader id
        let st_r = self.st.upgradable_read();
        match st_r.term.cmp(&term) {
            std::cmp::Ordering::Less => {
                let mut st_w = RwLockUpgradableReadGuard::upgrade(st_r);
                self.update_to_term_and_become_follower(&mut st_w, term);
                st_w.leader_id = Some(leader_id.clone());
                let _ig = self.ctx.leader_tx.send(Some(leader_id)).ok();
            }
            std::cmp::Ordering::Equal => {
                if st_r.leader_id.is_none() {
                    let mut st_w = RwLockUpgradableReadGuard::upgrade(st_r);
                    st_w.leader_id = Some(leader_id.clone());
                    let _ig = self.ctx.leader_tx.send(Some(leader_id)).ok();
                }
            }
            std::cmp::Ordering::Greater => {
                return Err((st_r.term, self.log.read().commit_index + 1))
            }
        }
        self.reset_election_tick();

        // append log entries
        let mut log_w = self.log.write();
        let append_succeeded = log_w
            .try_append_entries(entries, prev_log_index, prev_log_term)
            .is_ok();

        // update commit index
        let prev_commit_index = log_w.commit_index;
        log_w.commit_index = min(leader_commit, log_w.last_log_index());
        if prev_commit_index < log_w.commit_index {
            self.apply(&mut *log_w);
        }

        if append_succeeded {
            Ok(term)
        } else {
            debug!(
                "{} rejects append_entries, term: {term}, hint: {}",
                self.id(),
                log_w.commit_index + 1
            );
            Err((term, log_w.commit_index + 1))
        }
    }

    /// Handle `append_entries` response
    /// Return `Ok(ae_succeeded)`
    /// Return `Err(())` if self is no longer the leader
    pub(super) fn handle_append_entries_resp(
        &self,
        follower_id: &ServerId,
        last_sent_index: Option<LogIndex>, // None means the ae is a heartbeat
        term: u64,
        success: bool,
        hint_index: LogIndex,
    ) -> Result<bool, ()> {
        // validate term
        let (cur_term, cur_role) = self.st.map_read(|st_r| (st_r.term, st_r.role));
        if cur_term < term {
            let mut st_w = self.st.write();
            self.update_to_term_and_become_follower(&mut st_w, term);
            return Err(());
        }
        if cur_role != Role::Leader {
            return Err(());
        }

        if !success {
            self.lst.update_next_index(follower_id, hint_index);
            debug!(
                "{} updates follower {}'s next_index to {hint_index} because it rejects ae",
                self.id(),
                follower_id,
            );
            return Ok(false);
        }

        // if ae is a heartbeat, return
        let Some(last_sent_index) = last_sent_index else {
            return Ok(true);
        };

        self.lst.update_match_index(follower_id, last_sent_index);

        // check if commit_index needs to be updated
        if self.can_update_commit_index_to(&self.log.read(), last_sent_index, cur_term) {
            let mut log_w = self.log.write();
            if last_sent_index > log_w.commit_index {
                log_w.commit_index = last_sent_index;
                debug!("{} updates commit index to {last_sent_index}", self.id());
                self.apply(&mut *log_w);
            }
        }

        Ok(true)
    }

    /// Handle `vote`
    /// Return `Ok(term, spec_pool)` if the vote is granted
    /// Return `Err(term)` if the vote is rejected
    pub(super) fn handle_vote(
        &self,
        term: u64,
        candidate_id: ServerId,
        last_log_index: LogIndex,
        last_log_term: u64,
    ) -> Result<(u64, Vec<Arc<C>>), u64> {
        debug!(
            "{} received vote: term({}), last_log_index({}), last_log_term({}), id({})",
            self.id(),
            term,
            last_log_index,
            last_log_term,
            candidate_id
        );

        let mut st_w = self.st.write();
        let log_r = self.log.read();

        // calibrate term
        if term < st_w.term {
            return Err(st_w.term);
        }
        if term > st_w.term {
            self.update_to_term_and_become_follower(&mut st_w, term);
        }

        // check self role
        if st_w.role != Role::Follower {
            return Err(st_w.term);
        }

        // check if voted before
        if st_w
            .voted_for
            .as_ref()
            .map_or(false, |id| id != &candidate_id)
        {
            return Err(st_w.term);
        }

        // check if the candidate's log is up-to-date
        if !log_r.log_up_to_date(last_log_term, last_log_index) {
            return Err(st_w.term);
        }

        // grant the vote
        debug!("{} votes for server {}", self.id(), candidate_id);
        st_w.voted_for = Some(candidate_id);
        let self_spec_pool = self.ctx.sp.lock().pool.values().cloned().collect_vec();
        self.reset_election_tick();
        Ok((st_w.term, self_spec_pool))
    }

    /// Handle `vote` responses
    /// Return `Ok(election_ends)` if succeeds
    /// Return `Err(())` if self is no longer a candidate
    pub(super) fn handle_vote_resp(
        &self,
        id: &ServerId,
        term: u64,
        vote_granted: bool,
        spec_pool: Vec<Arc<C>>,
    ) -> Result<bool, ()> {
        let mut st_w = self.st.write();
        if st_w.term < term {
            self.update_to_term_and_become_follower(&mut st_w, term);
            return Err(());
        }
        if st_w.role != Role::Candidate {
            return Err(());
        }

        if !vote_granted {
            return Ok(false);
        }

        let mut cst_w = self.cst.lock();
        debug!("{}'s vote is granted by server {}", self.id(), id);

        cst_w.votes_received += 1;
        assert!(
            cst_w.sps.insert(id.clone(), spec_pool).is_none(),
            "a server can't vote twice"
        );

        let min_granted = self.quorum();
        if cst_w.votes_received < min_granted {
            return Ok(false);
        }

        // vote is granted by the majority of servers, can become leader
        let spec_pools = cst_w.sps.drain().collect();
        drop(cst_w);
        let mut log_w = self.log.write();

        let prev_last_log_index = log_w.last_log_index();
        self.recover_from_spec_pools(&mut st_w, &mut log_w, &spec_pools);
        let last_log_index = log_w.last_log_index();

        self.become_leader(&mut st_w);

        // update next_index for each follower
        for other in &self.ctx.others {
            self.lst.update_next_index(other, last_log_index + 1); // iter from the end to front is more likely to match the follower
        }
        if prev_last_log_index < last_log_index {
            // if some entries are recovered, sync with followers immediately
            self.ctx
                .sync_events
                .values()
                .for_each(|event| event.notify(1));
        }

        Ok(true)
    }

    /// Verify `install_snapshot` request
    pub(super) fn verify_install_snapshot(
        &self,
        term: u64,
        leader_id: ServerId,
        last_included_index: LogIndex,
        last_included_term: u64,
    ) -> bool {
        let mut st_w = self.st.write();
        if st_w.term < term {
            self.update_to_term_and_become_follower(&mut st_w, term);
            st_w.leader_id = Some(leader_id);
        }
        let log_r = self.log.read();
        // FIXME: is this correct
        let validate = log_r.last_log_index() < last_included_index
            && log_r.last_log_term() <= last_included_term;
        if validate {
            self.reset_election_tick();
        }
        validate
    }

    /// Handle `install_snapshot` resp
    /// Return Err(()) if the current node isn't a leader or current term is less than the given term
    pub(super) fn handle_snapshot_resp(
        &self,
        follower_id: &ServerId,
        meta: SnapshotMeta,
        term: u64,
    ) -> Result<(), ()> {
        // validate term
        let (cur_term, cur_role) = self.st.map_read(|st_r| (st_r.term, st_r.role));
        if cur_term < term {
            let mut st_w = self.st.write();
            self.update_to_term_and_become_follower(&mut st_w, term);
            return Err(());
        }
        if cur_role != Role::Leader {
            return Err(());
        }
        self.lst
            .update_match_index(follower_id, meta.last_included_index.numeric_cast());
        Ok(())
    }

    /// Handle `fetch_read_state`
    pub(super) fn handle_fetch_read_state(&self, cmd: &C) -> bincode::Result<ReadState> {
        let ids = self.ctx.sp.map_lock(|sp| {
            sp.pool
                .iter()
                .filter_map(|(id, c)| c.is_conflict(cmd).then_some(id.clone()))
                .collect_vec()
        });
        if ids.is_empty() {
            Ok(ReadState::CommitIndex(self.log.read().commit_index))
        } else {
            Ok(ReadState::Ids(IdSet::new(ids)?))
        }
    }
}

/// Other small public interface
impl<C: 'static + Command> RawCurp<C> {
    /// Create a new `RawCurp`
    #[allow(clippy::too_many_arguments)] // only called once
    pub(super) fn new(
        id: ServerId,
        others: HashSet<ServerId>,
        is_leader: bool,
        cmd_board: CmdBoardRef<C>,
        spec_pool: SpecPoolRef<C>,
        uncommitted_pool: UncommittedPoolRef<C>,
        cfg: Arc<CurpConfig>,
        cmd_tx: Arc<dyn CEEventTxApi<C>>,
        sync_events: HashMap<ServerId, Arc<Event>>,
        log_tx: mpsc::UnboundedSender<LogEntry<C>>,
    ) -> Self {
        let raw_curp = Self {
            st: RwLock::new(State::new(
                0,
                None,
                Role::Follower,
                None,
                cfg.follower_timeout_ticks,
                cfg.candidate_timeout_ticks,
            )),
            lst: LeaderState::new(&others),
            cst: Mutex::new(CandidateState::new()),
            log: RwLock::new(Log::new(log_tx, cfg.batch_max_size, cfg.log_entries_cap)),
            ctx: Context {
                id,
                others,
                cb: cmd_board,
                sp: spec_pool,
                ucp: uncommitted_pool,
                leader_tx: broadcast::channel(1).0,
                cfg,
                election_tick: AtomicU8::new(0),
                cmd_tx,
                sync_events,
                leader_event: Arc::new(Event::new()),
            },
        };
        if is_leader {
            let mut st_w = raw_curp.st.write();
            raw_curp.become_leader(&mut st_w);
        }
        raw_curp
    }

    /// Create a new `RawCurp`
    /// `is_leader` will only take effect when all servers start from a fresh state
    #[allow(clippy::too_many_arguments)] // only called once
    pub(super) fn recover_from(
        id: ServerId,
        others: HashSet<ServerId>,
        is_leader: bool,
        cmd_board: CmdBoardRef<C>,
        spec_pool: SpecPoolRef<C>,
        uncommitted_pool: UncommittedPoolRef<C>,
        cfg: &Arc<CurpConfig>,
        cmd_tx: Arc<dyn CEEventTxApi<C>>,
        sync_event: HashMap<ServerId, Arc<Event>>,
        log_tx: mpsc::UnboundedSender<LogEntry<C>>,
        voted_for: Option<(u64, ServerId)>,
        entries: Vec<LogEntry<C>>,
        last_applied: LogIndex,
    ) -> Self {
        let raw_curp = Self::new(
            id,
            others,
            is_leader,
            cmd_board,
            spec_pool,
            uncommitted_pool,
            Arc::clone(cfg),
            cmd_tx,
            sync_event,
            log_tx,
        );

        if let Some((term, server_id)) = voted_for {
            let mut st_w = raw_curp.st.write();
            raw_curp.update_to_term_and_become_follower(&mut st_w, term);
            st_w.voted_for = Some(server_id);
        } else if is_leader {
            // all uncommitted cmds should stay in ucp until they are executed
            raw_curp.ctx.ucp.map_lock(|mut ucp_l| {
                for e in &entries {
                    let _ig = ucp_l.insert(e.cmd.id().clone(), Arc::clone(&e.cmd));
                }
            });
        } else {
        }

        raw_curp.log.map_write(|mut log_w| {
            log_w.last_as = last_applied;
            log_w.last_exe = last_applied;
            log_w.commit_index = last_applied;
            log_w.restore_entries(entries);
        });

        raw_curp
    }

    /// Get the leader id, attached with the term
    pub(super) fn leader(&self) -> (Option<ServerId>, u64) {
        self.st.map_read(|st_r| (st_r.leader_id.clone(), st_r.term))
    }

    /// Get self's id
    pub(super) fn id(&self) -> &ServerId {
        &self.ctx.id
    }

    /// Get a rx for leader changes
    pub(super) fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.ctx.leader_tx.subscribe()
    }

    /// Get `append_entries` request for `follower_id` that contains the latest log entries
    pub(super) fn sync(&self, follower_id: &ServerId) -> Result<SyncAction<C>, ()> {
        let term = {
            let lst_r = self.st.read();
            if lst_r.role != Role::Leader {
                return Err(());
            }
            lst_r.term
        };

        let next_index = self.lst.get_next_index(follower_id);
        let log_r = self.log.read();
        if next_index <= log_r.base_index {
            // the log has already been compacted
            let entry = log_r.get(log_r.last_exe).unwrap_or_else(|| {
                unreachable!(
                    "log entry {} should not have been compacted yet, needed for snapshot",
                    log_r.last_as
                )
            });
            // TODO: buffer a local snapshot: if a follower is down for a long time,
            // the leader will take a snapshot itself every time `sync` is called in effort to
            // calibrate it. Since taking a snapshot will block the leader's execute workers, we should
            // not take snapshot so often. A better solution would be to keep a snapshot cache.
            Ok(SyncAction::Snapshot(self.ctx.cmd_tx.send_snapshot(
                SnapshotMeta {
                    last_included_index: entry.index,
                    last_included_term: entry.term,
                },
            )))
        } else {
            let (prev_log_index, prev_log_term) = log_r.get_prev_entry_info(next_index);
            let entries = log_r.get_from(next_index);
            let ae = AppendEntries {
                term,
                leader_id: self.id().clone(),
                prev_log_index,
                prev_log_term,
                leader_commit: log_r.commit_index,
                entries,
            };
            Ok(SyncAction::AppendEntries(ae))
        }
    }

    /// Get a reference to `CurpConfig`
    pub(super) fn cfg(&self) -> &CurpConfig {
        self.ctx.cfg.as_ref()
    }

    /// Is leader?
    pub(super) fn is_leader(&self) -> bool {
        self.st.read().role == Role::Leader
    }

    /// Get leader event
    pub(super) fn leader_event(&self) -> Arc<Event> {
        Arc::clone(&self.ctx.leader_event)
    }

    /// Reset log base
    pub(super) fn reset_by_snapshot(&self, meta: SnapshotMeta) {
        let mut log_w = self.log.write();
        log_w.reset_by_snapshot_meta(meta);
    }

    /// Get current term
    pub(super) fn term(&self) -> u64 {
        self.st.read().term
    }

    /// Get a reference to command board
    pub(super) fn cmd_board(&self) -> CmdBoardRef<C> {
        Arc::clone(&self.ctx.cb)
    }

    /// Get a reference to spec pool
    pub(crate) fn spec_pool(&self) -> SpecPoolRef<C> {
        Arc::clone(&self.ctx.sp)
    }

    /// Get a reference to uncommitted pool
    pub(crate) fn uncommitted_pool(&self) -> UncommittedPoolRef<C> {
        Arc::clone(&self.ctx.ucp)
    }

    /// Get sync event
    pub(super) fn sync_event(&self, id: &ServerId) -> Arc<Event> {
        Arc::clone(
            self.ctx
                .sync_events
                .get(id)
                .unwrap_or_else(|| unreachable!("server id {id} not found")),
        )
    }
}

// Utils
// Don't grab lock in the following functions(except cb or sp's lock)
impl<C: 'static + Command> RawCurp<C> {
    /// Server becomes a candidate
    fn become_candidate(&self, st: &mut State, cst: &mut CandidateState<C>, log: &Log<C>) -> Vote {
        let prev_role = st.role;
        assert!(prev_role != Role::Leader, "leader can't start election");

        st.term += 1;
        st.role = Role::Candidate;
        st.voted_for = Some(self.id().clone());
        st.leader_id = None;
        let _ig = self.ctx.leader_tx.send(None).ok();
        self.reset_election_tick();

        let self_sp = self
            .ctx
            .sp
            .map_lock(|sp| sp.pool.values().cloned().collect());
        cst.votes_received = 1;
        cst.sps = HashMap::from([(self.id().clone(), self_sp)]);

        if prev_role == Role::Follower {
            debug!("Follower {} starts election", self.id());
        } else {
            debug!("Candidate {} restarts election", self.id());
        }

        Vote {
            term: st.term,
            candidate_id: self.id().clone(),
            last_log_index: log.last_log_index(),
            last_log_term: log.last_log_term(),
        }
    }

    /// Server becomes a leader
    fn become_leader(&self, st: &mut State) {
        st.role = Role::Leader;
        st.leader_id = Some(self.id().clone());
        let _ig = self.ctx.leader_tx.send(Some(self.id().clone())).ok();
        self.ctx.leader_event.notify(usize::MAX);

        debug!("{} becomes the leader", self.id());
    }

    /// Server update self to a new term, will become a follower
    fn update_to_term_and_become_follower(&self, st: &mut State, term: u64) {
        if st.term > term {
            error!(
                "cannot update to a smaller term {term}, current term is {}",
                st.term
            );
            return;
        }
        if st.role == Role::Leader {
            self.leader_retires();
        }
        st.term = term;
        st.role = Role::Follower;
        st.voted_for = None;
        st.leader_id = None;
        let _ig = self.ctx.leader_tx.send(None).ok();
        st.randomize_timeout_ticks(); // regenerate timeout ticks
        debug!(
            "{} updates to term {term} and becomes a follower",
            self.id()
        );
    }

    /// Reset election tick
    fn reset_election_tick(&self) {
        self.ctx.election_tick.store(0, Ordering::Relaxed);
    }

    /// Check whether `commit_index` can be updated to i
    fn can_update_commit_index_to(&self, log: &Log<C>, i: LogIndex, cur_term: u64) -> bool {
        if log.commit_index >= i {
            return false;
        }

        // don't commit log from previous term
        if log.get(i).map_or(true, |entry| entry.term != cur_term) {
            return false;
        }

        let replicated_cnt: u64 = self
            .ctx
            .others
            .iter()
            .filter(|&id| self.lst.get_match_index(id) >= i)
            .count()
            .numeric_cast();
        replicated_cnt + 1 >= self.quorum()
    }

    /// Recover from all voter's spec pools
    fn recover_from_spec_pools(
        &self,
        st: &mut State,
        log: &mut Log<C>,
        spec_pools: &HashMap<ServerId, Vec<Arc<C>>>,
    ) {
        if log_enabled!(Level::Debug) {
            let debug_sps: HashMap<ServerId, String> = spec_pools
                .iter()
                .map(|(id, sp)| {
                    let sp: Vec<String> = sp.iter().map(|cmd| cmd.id().to_string()).collect();
                    (id.clone(), sp.join(","))
                })
                .collect();
            debug!("{} collected spec pools:\n{debug_sps:#?}", self.id());
        }

        let mut cmd_cnt: HashMap<ProposeId, (Arc<C>, u64)> = HashMap::new();
        for cmd in spec_pools.values().flatten().cloned() {
            let entry = cmd_cnt.entry(cmd.id().clone()).or_insert((cmd, 0));
            entry.1 += 1;
        }

        // get all possibly executed(fast path) commands
        let existing_log_ids = log.get_cmd_ids();
        let recovered_cmds = cmd_cnt
            .into_values()
            // only cmds whose cnt >= ( f + 1 ) / 2 + 1 can be recovered
            .filter_map(|(cmd, cnt)| (cnt >= self.recover_quorum()).then_some(cmd))
            // dedup in current logs
            .filter(|cmd| {
                // TODO: better dedup mechanism
                !existing_log_ids.contains(cmd.id())
            })
            .collect_vec();

        let mut cb_w = self.ctx.cb.write();
        let mut sp_l = self.ctx.sp.lock();

        let term = st.term;
        for cmd in recovered_cmds {
            let _ig_sync = cb_w.sync.insert(cmd.id().clone()); // may have been inserted before
            let _ig_spec = sp_l.insert(Arc::clone(&cmd)); // may have been inserted before
            #[allow(clippy::expect_used)]
            let index = log
                .push_cmd(term, Arc::clone(&cmd))
                .expect("cmd {cmd:?} cannot be serialized");
            debug!(
                "{} recovers speculatively executed cmd({}) in log[{}]",
                self.id(),
                cmd.id(),
                index,
            );
        }
    }

    /// Apply new logs
    fn apply(&self, log: &mut Log<C>) {
        for i in (log.last_as + 1)..=log.commit_index {
            let entry = log.get(i).unwrap_or_else(|| {
                unreachable!(
                    "system corrupted, apply log[{i}] when we only have {} log entries",
                    log.last_log_index()
                )
            });
            self.ctx.cmd_tx.send_after_sync(Arc::clone(&entry.cmd), i);
            log.last_as = i;
            if log.last_exe < log.last_as {
                log.last_exe = log.last_as;
            }

            debug!(
                "{} committed log[{i}], last_applied updated to {}",
                self.id(),
                i
            );
        }
        log.compact();
    }

    /// Get quorum: the smallest number of servers who must be online for the cluster to work
    fn quorum(&self) -> u64 {
        (self.ctx.others.len() / 2 + 1).numeric_cast()
    }

    /// Get `recover_quorum`: the smallest number of servers who must contain a command in speculative pool for it to be recovered
    fn recover_quorum(&self) -> u64 {
        self.quorum() / 2 + 1
    }

    /// When leader retires, it should reset state
    fn leader_retires(&self) {
        debug!("leader {} retires", self.id());

        // when a leader retires, it should wipe up speculatively executed cmds by resetting and re-executing
        let _ig = self.ctx.cmd_tx.send_reset(None);

        let mut cb_w = self.ctx.cb.write();
        cb_w.clear();

        let log_r = self.log.read();
        for i in 1..=log_r.commit_index {
            let entry = log_r.get(i).unwrap_or_else(|| {
                unreachable!(
                    "system corrupted, apply log[{i}] when we only have {} log entries",
                    log_r.last_log_index()
                )
            });
            self.ctx.cmd_tx.send_after_sync(Arc::clone(&entry.cmd), i);
        }
    }
}

#[cfg(test)]
mod tests;
