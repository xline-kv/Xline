//! READ THIS BEFORE YOU START WRITING CODE FOR THIS MODULE
//! To avoid deadlock, let's make some rules:
//! 1. To group similar functions, I divide Curp impl into three scope: one for utils(don't grab lock here), one for tick, one for handlers
//! 2. Lock order should be:
//!     1. self.st
//!     2. self.lst || self.cst (there is no need for grabbing both)
//!     3. self.log

#![allow(clippy::similar_names)] // st, lst, cst is similar but not confusing
#![allow(clippy::integer_arithmetic)] // u64 is large enough and won't overflow

use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
};

use clippy_utilities::NumericCast;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard};
use tokio::sync::broadcast;
use tracing::{debug, error};
use utils::{
    config::ServerTimeout,
    parking_lot_lock::{MutexMap, RwLockMap},
};

use self::{
    log::Log,
    state::{CandidateState, LeaderState, State},
};
use super::cmd_worker::CmdExeSenderInterface;
use crate::{
    cmd::{Command, ProposeId},
    log::LogEntry,
    message::ServerId,
    server::{cmd_board::CmdBoardRef, spec_pool::SpecPoolRef},
};

/// Curp log
mod log;

/// Curp state
mod state;

/// The curp state machine
#[derive(Debug)]
pub(super) struct RawCurp<C: Command> {
    /// Curp state
    st: RwLock<State>,
    /// Additional leader state
    lst: RwLock<LeaderState>,
    /// Additional candidate state
    cst: Mutex<CandidateState<C>>,
    /// Curp logs
    log: RwLock<Log<C>>,
    /// Relevant context
    ctx: Context<C>,
}

/// Actions to perform on tick
pub(super) enum TickAction<C> {
    /// Send out heartbeats
    Heartbeat(HashMap<ServerId, AppendEntries<C>>),
    /// Send out votes
    Votes(HashMap<ServerId, Vote>),
    /// Do nothing
    Nothing,
}

/// Invoked by candidates to gather votes
#[derive(Clone)]
pub(super) struct Vote {
    /// Candidate's term
    pub(super) term: u64,
    /// Candidate's Id
    pub(super) candidate_id: ServerId,
    /// Candidate's last log index
    pub(super) last_log_index: usize,
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
    pub(super) prev_log_index: usize,
    /// Term of log entry immediately preceding new ones
    pub(super) prev_log_term: u64,
    /// Leader's commit index
    pub(super) leader_commit: usize,
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
    /// Timeout
    timeout: Arc<ServerTimeout>,
    /// Cmd board for tracking the cmd sync results
    cb: CmdBoardRef<C>,
    /// Speculative pool
    sp: SpecPoolRef<C>,
    /// Tx to send leader changes
    leader_tx: broadcast::Sender<Option<ServerId>>,
    /// Election tick
    election_tick: AtomicU8,
    /// Heartbeat opt out flag
    hb_opt: AtomicBool,
    /// Tx to send cmds to execute and do after sync
    cmd_tx: Box<dyn CmdExeSenderInterface<C>>,
}

impl<C: Command> Debug for Context<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("id", &self.id)
            .field("others", &self.others)
            .field("timeout", &self.timeout)
            .field("cb", &self.cb)
            .field("sp", &self.sp)
            .field("leader_tx", &self.leader_tx)
            .field("election_tick", &self.election_tick)
            .field("hb_opt", &self.hb_opt)
            .finish()
    }
}

// Tick
impl<C: 'static + Command> RawCurp<C> {
    /// Tick
    pub(super) fn tick(&self) -> TickAction<C> {
        let (role, timeout) = self.st.map_read(|st_r| {
            (
                st_r.role,
                if st_r.role == Role::Follower {
                    st_r.follower_timeout_ticks
                } else {
                    st_r.candidate_timeout_ticks
                },
            )
        });
        if role == Role::Leader {
            self.tick_heartbeat()
        } else {
            self.tick_election(timeout)
        }
    }

    /// Tick election, will generate votes if timeout
    fn tick_election(&self, timeout: u8) -> TickAction<C> {
        let tick = self.ctx.election_tick.fetch_add(1, Ordering::AcqRel);
        if tick < timeout {
            return TickAction::Nothing;
        }

        // start election
        let vote = self.become_candidate(
            &mut self.st.write(),
            &mut self.cst.lock(),
            &*self.log.read(),
        );
        let votes = self
            .ctx
            .others
            .iter()
            .map(|id| (id.clone(), vote.clone()))
            .collect();
        TickAction::Votes(votes)
    }

    /// Tick heartbeat, will generate heartbeat if timeout
    fn tick_heartbeat(&self) -> TickAction<C> {
        // check if heartbeat has been optimized out
        if self
            .ctx
            .hb_opt
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return TickAction::Nothing;
        }

        let term = self.st.map_read(|st_r| st_r.term);
        let lst_r = self.lst.read();
        let log_r = self.log.read();

        #[allow(clippy::indexing_slicing)] // use index to access log is safe
        let hbs = self
            .ctx
            .others
            .iter()
            .map(|id| {
                let next_index = lst_r.get_next_index(id);
                debug!("{} send heartbeat to {}", self.id(), id);
                (
                    id.clone(),
                    AppendEntries {
                        term,
                        leader_id: self.id().clone(),
                        prev_log_index: next_index - 1,
                        prev_log_term: log_r.entries[next_index - 1].term(),
                        leader_commit: log_r.commit_index,
                        entries: vec![],
                    },
                )
            })
            .collect();
        TickAction::Heartbeat(hbs)
    }
}

// Curp handlers
impl<C: 'static + Command> RawCurp<C> {
    /// Handle `append_entries`
    /// Return `Ok(term)` if succeeds
    /// Return `Err(term, hint_index)` if fails
    pub(super) fn handle_append_entries(
        &self,
        term: u64,
        leader_id: String,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<LogEntry<C>>,
        leader_commit: usize,
    ) -> Result<u64, (u64, usize)> {
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
    /// Return `Ok(append_entries_succeeded)`
    /// Return `Err(())` if self is no longer the leader
    #[allow(clippy::unwrap_in_result)]
    pub(super) fn handle_append_entries_resp(
        &self,
        follower_id: &ServerId,
        last_sent_index: Option<usize>, // None means the ae is a heartbeat
        term: u64,
        success: bool,
        hint_index: usize,
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

        #[allow(clippy::unwrap_used)] // there must be a next_index for each follower
        if !success {
            let mut lst_w = self.lst.write();
            lst_w.update_next_index(follower_id, hint_index);
            debug!(
                "{} updates follower {}'s next_index to {hint_index}",
                self.id(),
                follower_id,
            );
            return Ok(false);
        }

        // if ae is a heartbeat, return
        let Some(last_sent_index) = last_sent_index else {
            return Ok(true);
        };

        self.lst
            .map_write(|mut lst_w| lst_w.update_match_index(follower_id, last_sent_index));

        // check if commit_index needs to be updated
        if self.can_update_commit_index_to(
            &self.lst.read(),
            &self.log.read(),
            last_sent_index,
            cur_term,
        ) {
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
        last_log_index: usize,
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
        debug!("vote is granted by server {}", id);

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
        let mut lst_w = self.lst.write();
        let mut log_w = self.log.write();

        self.recover_from_spec_pools(&mut st_w, &mut log_w, &spec_pools);
        let last_log_index = log_w.last_log_index();

        self.become_leader(&mut st_w);

        // update next_index for each follower
        for other in &self.ctx.others {
            lst_w.update_next_index(other, last_log_index + 1); // iter from the end to front is more likely to match the follower
        }

        Ok(true)
    }

    /// Push a new log entry to the log, returns its index
    pub(super) fn push_log_entry(&self, entry: LogEntry<C>) -> usize {
        let mut log_w = self.log.write();
        log_w.entries.push(entry);
        log_w.last_log_index()
    }
}

/// Other small public interface
impl<C: 'static + Command> RawCurp<C> {
    /// Create a new `RawCurp`
    pub(super) fn new(
        id: ServerId,
        others: HashSet<ServerId>,
        is_leader: bool,
        cmb_board: CmdBoardRef<C>,
        spec_pool: SpecPoolRef<C>,
        timeout: Arc<ServerTimeout>,
        cmd_tx: Box<dyn CmdExeSenderInterface<C>>,
    ) -> Self {
        let next_index = others.iter().map(|o| (o.clone(), 1)).collect();
        let match_index = others.iter().map(|o| (o.clone(), 0)).collect();
        let raw_curp = Self {
            st: RwLock::new(State::new(
                0,
                None,
                Role::Follower,
                None,
                *timeout.follower_timeout_ticks(),
                *timeout.candidate_timeout_ticks(),
            )),
            lst: RwLock::new(LeaderState::new(next_index, match_index)),
            cst: Mutex::new(CandidateState::new()),
            log: RwLock::new(Log::new()),
            ctx: Context {
                id,
                others,
                cb: cmb_board,
                sp: spec_pool,
                leader_tx: broadcast::channel(1).0,
                timeout,
                election_tick: AtomicU8::new(0),
                hb_opt: AtomicBool::new(false),
                cmd_tx,
            },
        };
        if is_leader {
            let mut st_w = raw_curp.st.write();
            raw_curp.become_leader(&mut st_w);
        }
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

    /// Get current term
    pub(super) fn term(&self) -> u64 {
        self.st.map_read(|st_r| st_r.term)
    }

    /// Get a rx for leader changes
    pub(super) fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.ctx.leader_tx.subscribe()
    }

    /// Get `append_entries` request for log[i]
    #[allow(clippy::indexing_slicing)] // use index to access log is safe
    pub(super) fn append_entries_single(&self, i: usize) -> AppendEntries<C> {
        let term = self.term();
        let log_r = self.log.read();
        AppendEntries {
            term,
            leader_id: self.id().clone(),
            prev_log_index: i - 1,
            prev_log_term: log_r.entries[i - 1].term(),
            leader_commit: log_r.commit_index,
            entries: vec![log_r.entries[i].clone()],
        }
    }

    /// Get `append_entries` request for `follower_id` that contains the latest log entries
    #[allow(clippy::indexing_slicing)] // use index to access log is safe
    pub(super) fn append_entries(&self, follower_id: &ServerId) -> AppendEntries<C> {
        let term = self.term();
        let next_index = self.lst.map_read(|lst_r| lst_r.get_next_index(follower_id));
        let log_r = self.log.read();
        AppendEntries {
            term,
            leader_id: self.id().clone(),
            prev_log_index: next_index - 1,
            prev_log_term: log_r.entries[next_index - 1].term(),
            leader_commit: log_r.commit_index,
            entries: log_r.entries[next_index..].to_vec(),
        }
    }

    /// Optimize out heartbeat
    pub(super) fn opt_out_hb(&self) {
        self.ctx.hb_opt.store(true, Ordering::Relaxed);
    }

    /// Get a reference to `ServerTimeout`
    pub(super) fn timeout(&self) -> &ServerTimeout {
        self.ctx.timeout.as_ref()
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
        self.ctx.hb_opt.store(false, Ordering::Relaxed);
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
    fn can_update_commit_index_to(
        &self,
        lst: &LeaderState,
        log: &Log<C>,
        i: usize,
        cur_term: u64,
    ) -> bool {
        if log.commit_index >= i {
            return false;
        }

        // don't commit log from previous term
        #[allow(clippy::indexing_slicing)] // use index to access log is safe
        if log.entries[i].term() != cur_term {
            return false;
        }

        let replicated_cnt: u64 = self
            .ctx
            .others
            .iter()
            .filter(|&id| lst.get_match_index(id) >= i)
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
        debug!("{} collected spec pools: {spec_pools:?}", self.id());

        let mut cmd_cnt: HashMap<ProposeId, (Arc<C>, u64)> = HashMap::new();
        for cmd in spec_pools.values().flatten().cloned() {
            let entry = cmd_cnt.entry(cmd.id().clone()).or_insert((cmd, 0));
            entry.1 += 1;
        }

        // get all possibly executed(fast path) commands
        let existing_log_ids: HashSet<ProposeId> = log
            .entries
            .iter()
            .flat_map(|entries| entries.cmds().iter().map(|cmd| cmd.id()).cloned())
            .collect();
        let recovered_cmds = cmd_cnt
            .into_values()
            // only cmds whose cnt >= 3/4 can be recovered
            .filter_map(|(cmd, cnt)| (cnt >= self.superquorum()).then_some(cmd))
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
            debug!(
                "{} recovers speculatively executed cmd {:?} in log[{}]",
                self.id(),
                cmd.id(),
                log.entries.len(),
            );
            let _ig_sync = cb_w.sync.insert(cmd.id().clone()); // may have been inserted before
            let _ig_spec = sp_l.insert(Arc::clone(&cmd)); // may have been inserted before
            log.entries.push(LogEntry::new(term, &[cmd]));
        }
    }

    /// Apply new logs
    fn apply(&self, log: &mut Log<C>) {
        for i in (log.last_applied + 1)..=log.commit_index {
            #[allow(clippy::indexing_slicing)] // use index to access log is safe
            for cmd in log.entries[i].cmds() {
                self.ctx
                    .cmd_tx
                    .send_after_sync(Arc::clone(cmd), i.numeric_cast());
            }
            log.last_applied = i;

            debug!(
                "{} committed log[{i}], last_applied updated to {}",
                self.id(),
                i
            );
        }
    }

    /// Get quorum: the smallest number of servers who must be online for the cluster to work
    fn quorum(&self) -> u64 {
        (self.ctx.others.len() / 2 + 1).numeric_cast()
    }

    /// Get superquorum: the smallest number of servers who must contain a command in speculative pool for it to be recovered
    fn superquorum(&self) -> u64 {
        self.quorum() / 2 + 1
    }

    /// When leader retires, it should reset state
    fn leader_retires(&self) {
        // when a leader retires, it should wipe up speculatively executed cmds by resetting and re-executing
        self.ctx.cmd_tx.send_reset();

        let mut cb_w = self.ctx.cb.write();
        cb_w.clear();

        let log_r = self.log.read();
        for i in 0..=log_r.commit_index {
            #[allow(clippy::indexing_slicing)] // use index to access log is safe
            let entry = &log_r.entries[i];
            for cmd in entry.cmds() {
                self.ctx
                    .cmd_tx
                    .send_after_sync(Arc::clone(cmd), i.numeric_cast());
            }
        }
    }
}

#[cfg(test)]
mod tests;
