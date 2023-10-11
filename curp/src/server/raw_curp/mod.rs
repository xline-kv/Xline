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

use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp_external_api::cmd::ConflictCheck;
use dashmap::DashMap;
use event_listener::Event;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{
    debug, error,
    log::{log_enabled, Level},
    trace,
};
use utils::{
    config::CurpConfig,
    parking_lot_lock::{MutexMap, RwLockMap},
    shutdown::{self, Signal},
};

use self::{
    log::Log,
    state::{CandidateState, LeaderState, State},
};
use super::{cmd_worker::CEEventTxApi, CurpError, PoolEntry};
use crate::{
    cmd::{Command, ProposeId},
    connect::InnerConnectApi,
    error::ProposeError,
    log_entry::{EntryData, LogEntry},
    members::{ClusterInfo, ServerId},
    role_change::RoleChange,
    rpc::{
        connect::InnerConnectApiWrapper, ConfChange, ConfChangeEntry, ConfChangeError,
        ConfChangeType, IdSet, Member, ReadState,
    },
    server::{
        cmd_board::CmdBoardRef,
        raw_curp::{log::FallbackContext, state::VoteResult},
        spec_pool::SpecPoolRef,
    },
    snapshot::{Snapshot, SnapshotMeta},
    LogIndex,
};

/// Curp state
mod state;

/// Curp log
mod log;

/// test utils
#[cfg(test)]
mod tests;

/// Uncommitted pool type
pub(super) type UncommittedPool<C> = HashMap<ProposeId, PoolEntry<C>>;

/// Reference to uncommitted pool
pub(super) type UncommittedPoolRef<C> = Arc<Mutex<UncommittedPool<C>>>;

/// Default Size of channel
const CHANGE_CHANNEL_SIZE: usize = 128;

/// Max gap between leader and learner when promoting a learner
const MAX_PROMOTE_GAP: u64 = 500;

/// The curp state machine
#[derive(Debug)]
pub(super) struct RawCurp<C: Command, RC: RoleChange> {
    /// Curp state
    st: RwLock<State>,
    /// Additional leader state
    lst: LeaderState,
    /// Additional candidate state
    cst: Mutex<CandidateState<C>>,
    /// Curp logs
    log: RwLock<Log<C>>,
    /// Relevant context
    ctx: Context<C, RC>,
    /// Shutdown trigger
    shutdown_trigger: shutdown::Trigger,
}

/// Actions of syncing
pub(super) enum SyncAction<C> {
    /// Use append entries to calibrate
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
    pub(super) entries: Vec<Arc<LogEntry<C>>>,
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
struct Context<C: Command, RC: RoleChange> {
    /// Cluster information
    cluster_info: Arc<ClusterInfo>,
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
    sync_events: DashMap<ServerId, Arc<Event>>,
    /// Become leader event
    leader_event: Arc<Event>,
    /// Leader change callback
    role_change: RC,
    /// Conf change tx, used to update sync tasks
    change_tx: flume::Sender<ConfChange>,
    /// Conf change rx, used to update sync tasks
    change_rx: flume::Receiver<ConfChange>,
    /// Connects of peers
    connects: DashMap<ServerId, InnerConnectApiWrapper>,
}

impl<C: Command, RC: RoleChange> Debug for Context<C, RC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("cluster_info", &self.cluster_info)
            .field("cfg", &self.cfg)
            .field("cb", &self.cb)
            .field("sp", &self.sp)
            .field("ucp", &self.ucp)
            .field("leader_tx", &self.leader_tx)
            .field("election_tick", &self.election_tick)
            .field("cmd_tx", &"CEEventTxApi")
            .field("sync_events", &self.sync_events)
            .field("leader_event", &self.leader_event)
            .field("role_change", &self.role_change)
            .finish()
    }
}

// Tick
impl<C: 'static + Command, RC: RoleChange + 'static> RawCurp<C, RC> {
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

        self.become_candidate(
            &mut self.st.write(),
            &mut self.cst.lock(),
            self.log.upgradable_read(),
        )
    }
}

// Curp handlers
impl<C: 'static + Command, RC: RoleChange + 'static> RawCurp<C, RC> {
    /// Handle `propose` request
    /// Return `((leader_id, term), Ok(spec_executed))` if the proposal succeeds, `Ok(true)` if leader speculatively executed the command
    /// Return `((leader_id, term), Err(ProposeError))` if the cmd cannot be speculatively executed or is duplicated
    #[allow(clippy::type_complexity)] // it's clear
    pub(super) fn handle_propose(
        &self,
        cmd: Arc<C>,
    ) -> Result<((Option<ServerId>, u64), Result<bool, ProposeError>), CurpError> {
        debug!("{} gets proposal for cmd({})", self.id(), cmd.id());
        let mut conflict = self.insert_sp(Arc::clone(&cmd));
        let st_r = self.st.read();
        let info = (st_r.leader_id, st_r.term);
        // Non-leader doesn't need to sync or execute
        if st_r.role != Role::Leader {
            return Ok((
                info,
                if conflict {
                    Err(ProposeError::KeyConflict)
                } else {
                    Ok(false)
                },
            ));
        }
        let id = cmd.id();
        if !self.ctx.cb.map_write(|mut cb_w| cb_w.sync.insert(id)) {
            return Ok((info, Err(ProposeError::Duplicated)));
        }
        // leader also needs to check if the cmd conflicts un-synced commands
        conflict |= self.insert_ucp(Arc::clone(&cmd));
        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, cmd)?;
        debug!("{} gets new log[{}]", self.id(), entry.index);

        self.entry_process(&mut log_w, entry, conflict);

        Ok((
            info,
            if conflict {
                Err(ProposeError::KeyConflict)
            } else {
                Ok(true)
            },
        ))
    }

    /// Handle `shutdown` request
    pub(super) fn handle_shutdown(&self) -> Result<(), CurpError> {
        let st_r = self.st.read();
        if st_r.role != Role::Leader {
            return Err(CurpError::Redirect(st_r.leader_id, st_r.term));
        }
        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, EntryData::Shutdown)?;
        debug!("{} gets new log[{}]", self.id(), entry.index);
        self.entry_process(&mut log_w, entry, true);
        Ok(())
    }

    /// Handle `propose_conf_change` request
    #[allow(clippy::type_complexity)] // it's clear that the `CurpError` is an out-of-bound error
    pub(super) fn handle_propose_conf_change(
        &self,
        conf_change: ConfChangeEntry,
    ) -> Result<((Option<ServerId>, u64), Result<(), ConfChangeError>), CurpError> {
        debug!(
            "{} gets conf change for with id {}",
            self.id(),
            conf_change.id()
        );
        let st_r = self.st.read();
        let info = (st_r.leader_id, st_r.term);

        // Non-leader doesn't need to sync or execute
        if st_r.role != Role::Leader {
            return Err(CurpError::Redirect(st_r.leader_id, st_r.term));
        }
        if let Err(e) = self.check_new_config(conf_change.changes()) {
            return Ok((info, Err(e)));
        }
        let pool_entry = PoolEntry::from(conf_change.clone());
        let mut conflict = self.insert_sp(pool_entry.clone());
        conflict |= self.insert_ucp(pool_entry);
        let id = conf_change.id();
        if !self.ctx.cb.map_write(|mut cb_w| cb_w.sync.insert(id)) {
            return Ok((
                info,
                Err(ConfChangeError::new_propose(ProposeError::Duplicated)),
            ));
        }
        let changes = conf_change.changes().to_owned();
        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, conf_change)?;
        debug!("{} gets new log[{}]", self.id(), entry.index);
        let (addrs, name, is_learner) = self.apply_conf_change(changes);
        let _ig = log_w.fallback_contexts.insert(
            entry.index,
            FallbackContext::new(Arc::clone(&entry), addrs, name, is_learner),
        );
        self.entry_process(&mut log_w, entry, conflict);
        Ok((info, Ok(())))
    }

    /// Handle `append_entries`
    /// Return `Ok(term)` if succeeds
    /// Return `Err(term, hint_index)` if fails
    pub(super) fn handle_append_entries(
        &self,
        term: u64,
        leader_id: ServerId,
        prev_log_index: LogIndex,
        prev_log_term: u64,
        entries: Vec<LogEntry<C>>,
        leader_commit: LogIndex,
    ) -> Result<u64, (u64, LogIndex)> {
        if entries.is_empty() {
            trace!(
                "{} received heartbeat from {}: term({}), commit({}), prev_log_index({}), prev_log_term({})",
                self.id(), leader_id, term, leader_commit, prev_log_index, prev_log_term
            );
        } else {
            debug!(
                "{} received append_entries from {}: term({}), commit({}), prev_log_index({}), prev_log_term({}), {} entries",
                self.id(), leader_id, term, leader_commit, prev_log_index, prev_log_term, entries.len()
            );
        }

        // validate term and set leader id
        let st_r = self.st.upgradable_read();
        match st_r.term.cmp(&term) {
            std::cmp::Ordering::Less => {
                let mut st_w = RwLockUpgradableReadGuard::upgrade(st_r);
                self.update_to_term_and_become_follower(&mut st_w, term);
                st_w.leader_id = Some(leader_id);
                let _ig = self.ctx.leader_tx.send(Some(leader_id)).ok();
            }
            std::cmp::Ordering::Equal => {
                if st_r.leader_id.is_none() {
                    let mut st_w = RwLockUpgradableReadGuard::upgrade(st_r);
                    st_w.leader_id = Some(leader_id);
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
        let (cc_entries, fallback_indexes) = log_w
            .try_append_entries(entries, prev_log_index, prev_log_term)
            .map_err(|_ig| (term, log_w.commit_index + 1))?;
        // fallback overwritten conf change entries
        for idx in fallback_indexes.iter().sorted().rev() {
            let info = log_w.fallback_contexts.remove(idx).unwrap_or_else(|| {
                unreachable!("fall_back_infos should contain the entry need to fallback")
            });
            let EntryData::ConfChange(ref conf_change) = info.origin_entry.entry_data else {
                unreachable!("the entry in the fallback_info should be conf change entry");
            };
            let changes = conf_change.changes().to_owned();
            self.fallback_conf_change(changes, info.addrs, info.name, info.is_learner);
        }
        // apply conf change entries
        for e in cc_entries {
            let EntryData::ConfChange(ref cc) = e.entry_data else {
                unreachable!("cc_entry should be conf change entry");
            };
            let (addrs, name, is_learner) = self.apply_conf_change(cc.changes().to_owned());
            let _ig = log_w.fallback_contexts.insert(
                e.index,
                FallbackContext::new(Arc::clone(&e), addrs, name, is_learner),
            );
        }
        // update commit index
        let prev_commit_index = log_w.commit_index;
        log_w.commit_index = min(leader_commit, log_w.last_log_index());
        if prev_commit_index < log_w.commit_index {
            self.apply(&mut *log_w);
        }
        Ok(term)
    }

    /// Handle `append_entries` response
    /// Return `Ok(ae_succeeded)`
    /// Return `Err(())` if self is no longer the leader
    pub(super) fn handle_append_entries_resp(
        &self,
        follower_id: ServerId,
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
        let log_r = self.log.upgradable_read();
        if self.can_update_commit_index_to(&log_r, last_sent_index, cur_term) {
            let mut log_w = RwLockUpgradableReadGuard::upgrade(log_r);
            if last_sent_index > log_w.commit_index {
                log_w.commit_to(last_sent_index);
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
    ) -> Result<(u64, Vec<PoolEntry<C>>), u64> {
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
        id: ServerId,
        term: u64,
        vote_granted: bool,
        spec_pool: Vec<PoolEntry<C>>,
    ) -> Result<bool, ()> {
        let mut st_w = self.st.write();
        if st_w.term < term {
            self.update_to_term_and_become_follower(&mut st_w, term);
            return Err(());
        }
        if st_w.role != Role::Candidate {
            return Err(());
        }

        let mut cst_w = self.cst.lock();
        let _ig = cst_w.votes_received.insert(id, vote_granted);

        if !vote_granted {
            return Ok(false);
        }

        debug!("{}'s vote is granted by server {}", self.id(), id);

        assert!(
            cst_w.sps.insert(id, spec_pool).is_none(),
            "a server can't vote twice"
        );

        if !matches!(cst_w.check_vote(), VoteResult::Won) {
            return Ok(false);
        }

        // vote is granted by the majority of servers, can become leader
        let spec_pools = cst_w.sps.drain().collect();
        drop(cst_w);
        let mut log_w = self.log.write();

        let prev_last_log_index = log_w.last_log_index();
        self.recover_from_spec_pools(&mut st_w, &mut log_w, spec_pools);
        self.recover_ucp_from_log(&mut log_w);
        let last_log_index = log_w.last_log_index();

        self.become_leader(&mut st_w);

        // update next_index for each follower
        for other in self.ctx.cluster_info.peers_ids() {
            self.lst.update_next_index(other, last_log_index + 1); // iter from the end to front is more likely to match the follower
        }
        if prev_last_log_index < last_log_index {
            // if some entries are recovered, sync with followers immediately
            self.ctx
                .sync_events
                .iter()
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
        follower_id: ServerId,
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
    pub(super) fn handle_fetch_read_state(&self, cmd: &C) -> ReadState {
        let ids = self.ctx.sp.map_lock(|sp| {
            sp.pool
                .iter()
                .filter_map(|(id, c)| c.is_conflict_with_cmd(cmd).then_some(*id))
                .collect_vec()
        });
        if ids.is_empty() {
            ReadState::CommitIndex(self.log.read().commit_index)
        } else {
            ReadState::Ids(IdSet::new(ids))
        }
    }
}

/// Other small public interface
impl<C: 'static + Command, RC: RoleChange + 'static> RawCurp<C, RC> {
    /// Create a new `RawCurp`
    #[allow(clippy::too_many_arguments)] // only called once
    pub(super) fn new(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        cmd_board: CmdBoardRef<C>,
        spec_pool: SpecPoolRef<C>,
        uncommitted_pool: UncommittedPoolRef<C>,
        cfg: Arc<CurpConfig>,
        cmd_tx: Arc<dyn CEEventTxApi<C>>,
        sync_events: DashMap<ServerId, Arc<Event>>,
        log_tx: mpsc::UnboundedSender<Arc<LogEntry<C>>>,
        role_change: RC,
        shutdown_trigger: shutdown::Trigger,
        connects: DashMap<ServerId, InnerConnectApiWrapper>,
    ) -> Self {
        let (change_tx, change_rx) = flume::bounded(CHANGE_CHANNEL_SIZE);
        let raw_curp = Self {
            st: RwLock::new(State::new(
                0,
                None,
                Role::Follower,
                None,
                cfg.follower_timeout_ticks,
                cfg.candidate_timeout_ticks,
            )),
            lst: LeaderState::new(&cluster_info.peers_ids()),
            cst: Mutex::new(CandidateState::new(cluster_info.all_ids().into_iter())),
            log: RwLock::new(Log::new(log_tx, cfg.batch_max_size, cfg.log_entries_cap)),
            ctx: Context {
                cluster_info,
                cb: cmd_board,
                sp: spec_pool,
                ucp: uncommitted_pool,
                leader_tx: broadcast::channel(1).0,
                cfg,
                election_tick: AtomicU8::new(0),
                cmd_tx,
                sync_events,
                leader_event: Arc::new(Event::new()),
                role_change,
                change_tx,
                change_rx,
                connects,
            },
            shutdown_trigger,
        };
        if is_leader {
            let mut st_w = raw_curp.st.write();
            st_w.term = 1;
            raw_curp.become_leader(&mut st_w);
        }
        raw_curp
    }

    /// Create a new `RawCurp`
    /// `is_leader` will only take effect when all servers start from a fresh state
    #[allow(clippy::too_many_arguments)] // only called once
    pub(super) fn recover_from(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        cmd_board: CmdBoardRef<C>,
        spec_pool: SpecPoolRef<C>,
        uncommitted_pool: UncommittedPoolRef<C>,
        cfg: &Arc<CurpConfig>,
        cmd_tx: Arc<dyn CEEventTxApi<C>>,
        sync_event: DashMap<ServerId, Arc<Event>>,
        log_tx: mpsc::UnboundedSender<Arc<LogEntry<C>>>,
        voted_for: Option<(u64, ServerId)>,
        entries: Vec<LogEntry<C>>,
        last_applied: LogIndex,
        role_change: RC,
        shutdown_trigger: shutdown::Trigger,
        connects: DashMap<ServerId, InnerConnectApiWrapper>,
    ) -> Self {
        let raw_curp = Self::new(
            cluster_info,
            is_leader,
            cmd_board,
            spec_pool,
            uncommitted_pool,
            Arc::clone(cfg),
            cmd_tx,
            sync_event,
            log_tx,
            role_change,
            shutdown_trigger,
            connects,
        );

        if let Some((term, server_id)) = voted_for {
            let mut st_w = raw_curp.st.write();
            raw_curp.update_to_term_and_become_follower(&mut st_w, term);
            st_w.voted_for = Some(server_id);
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
    pub(super) fn leader(&self) -> (Option<ServerId>, u64, bool) {
        self.st
            .map_read(|st_r| (st_r.leader_id, st_r.term, st_r.role == Role::Leader))
    }

    /// Get cluster info
    pub(super) fn cluster(&self) -> &ClusterInfo {
        self.ctx.cluster_info.as_ref()
    }

    /// Get self's id
    pub(super) fn id(&self) -> ServerId {
        self.ctx.cluster_info.self_id()
    }

    /// Get a rx for leader changes
    pub(super) fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.ctx.leader_tx.subscribe()
    }

    /// Get `append_entries` request for `follower_id` that contains the latest log entries
    pub(super) fn sync(&self, follower_id: ServerId) -> Option<SyncAction<C>> {
        let term = {
            let lst_r = self.st.read();
            if lst_r.role != Role::Leader {
                return None;
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
            Some(SyncAction::Snapshot(self.ctx.cmd_tx.send_snapshot(
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
                leader_id: self.id(),
                prev_log_index,
                prev_log_term,
                leader_commit: log_r.commit_index,
                entries,
            };
            Some(SyncAction::AppendEntries(ae))
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
    pub(super) fn sync_event(&self, id: ServerId) -> Arc<Event> {
        Arc::clone(
            self.ctx
                .sync_events
                .get(&id)
                .unwrap_or_else(|| unreachable!("server id {id} not found"))
                .value(),
        )
    }

    /// Enter shutdown state
    pub(super) fn enter_shutdown(&self) {
        self.shutdown_trigger.cluster_shutdown();
        debug!("enter cluster shutdown state");
    }

    /// Check if the cluster is shutting down
    pub(super) fn is_shutdown(&self) -> bool {
        !matches!(self.shutdown_trigger.state(), Signal::Running)
    }

    /// Get a shutdown listener
    pub(super) fn shutdown_trigger(&self) -> &shutdown::Trigger {
        &self.shutdown_trigger
    }

    /// Get a shutdown listener
    pub(super) fn shutdown_listener(&self) -> shutdown::Listener {
        self.shutdown_trigger.subscribe()
    }

    /// Check if all followers have caught up with the leader
    pub(super) fn is_synced(&self) -> bool {
        let log_r = self.log.read();
        let leader_commit_index = log_r.commit_index;
        self.lst.check_all(|f| f.match_index == leader_commit_index)
    }

    /// Check if the new config is valid
    pub(super) fn check_new_config(&self, changes: &[ConfChange]) -> Result<(), ConfChangeError> {
        assert_eq!(changes.len(), 1, "Joint consensus is not supported yet");
        let Some(conf_change) = changes.iter().next() else {
            unreachable!("conf change is empty");
        };
        let mut statuses_ids = self
            .lst
            .get_all_statuses()
            .keys()
            .copied()
            .chain([self.id()])
            .collect::<HashSet<_>>();
        let mut config = self.cst.map_lock(|cst_l| cst_l.config.clone());
        let node_id = conf_change.node_id;
        match conf_change.change_type() {
            ConfChangeType::Add => {
                if !statuses_ids.insert(node_id) || !config.insert(node_id, false) {
                    return Err(ConfChangeError::NodeAlreadyExists(()));
                }
            }
            ConfChangeType::Remove => {
                if !statuses_ids.remove(&node_id) || !config.remove(node_id) {
                    return Err(ConfChangeError::NodeNotExists(()));
                }
            }
            ConfChangeType::Update => {
                if statuses_ids.get(&node_id).is_none() || !config.contains(node_id) {
                    return Err(ConfChangeError::NodeNotExists(()));
                }
            }
            ConfChangeType::AddLearner => {
                if !statuses_ids.insert(node_id) || !config.insert(node_id, true) {
                    return Err(ConfChangeError::NodeAlreadyExists(()));
                }
            }
            ConfChangeType::Promote => {
                if statuses_ids.get(&node_id).is_none() || !config.contains(node_id) {
                    return Err(ConfChangeError::NodeNotExists(()));
                }
                let learner_index = self.lst.get_match_index(node_id);
                let leader_index = self.log.read().last_log_index();
                if leader_index.overflow_sub(learner_index) > MAX_PROMOTE_GAP {
                    return Err(ConfChangeError::LearnerNotCatchUp(()));
                }
            }
        }
        let mut all_nodes = HashSet::new();
        all_nodes.extend(config.voters());
        all_nodes.extend(&config.learners);
        if statuses_ids.len() < 3
            || all_nodes != statuses_ids
            || !config.voters().is_disjoint(&config.learners)
        {
            return Err(ConfChangeError::InvalidConfig(()));
        }
        Ok(())
    }

    /// Apply conf changes and return true if self node is removed
    pub(super) fn apply_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> (Vec<String>, String, bool) {
        assert_eq!(changes.len(), 1, "Joint consensus is not supported yet");
        let Some(conf_change) = changes.into_iter().next() else {
            unreachable!("conf change is empty");
        };
        self.switch_config(conf_change)
    }

    /// Fallback conf change
    pub(super) fn fallback_conf_change(
        &self,
        changes: Vec<ConfChange>,
        old_addrs: Vec<String>,
        name: String,
        is_learner: bool,
    ) {
        assert_eq!(changes.len(), 1, "Joint consensus is not supported yet");
        let Some(conf_change) = changes.into_iter().next() else {
            unreachable!("conf change is empty");
        };
        let node_id = conf_change.node_id;
        let fallback_change = match conf_change.change_type() {
            ConfChangeType::Add | ConfChangeType::AddLearner => {
                self.cst
                    .map_lock(|mut cst_l| _ = cst_l.config.remove(node_id));
                self.lst.remove(node_id);
                _ = self.ctx.sync_events.remove(&node_id);
                let _ig = self.ctx.cluster_info.remove(&node_id);
                _ = self.ctx.connects.remove(&node_id);
                Some(ConfChange::remove(node_id))
            }
            ConfChangeType::Remove => {
                let member = Member::new(node_id, name, old_addrs.clone(), is_learner);
                self.cst
                    .map_lock(|mut cst_l| _ = cst_l.config.insert(node_id, is_learner));
                self.lst.insert(node_id, is_learner);
                _ = self.ctx.sync_events.insert(node_id, Arc::new(Event::new()));
                self.ctx.cluster_info.insert(member);
                if is_learner {
                    Some(ConfChange::add_learner(node_id, old_addrs))
                } else {
                    Some(ConfChange::add(node_id, old_addrs))
                }
            }
            ConfChangeType::Update => {
                _ = self.ctx.cluster_info.update(&node_id, old_addrs.clone());
                Some(ConfChange::update(node_id, old_addrs))
            }
            ConfChangeType::Promote => {
                self.cst.map_lock(|mut cst_l| {
                    _ = cst_l.config.remove(node_id);
                    _ = cst_l.config.insert(node_id, true);
                });
                self.ctx.cluster_info.demote(node_id);
                self.lst.demote(node_id);
                None
            }
        };
        if let Some(c) = fallback_change {
            self.ctx
                .change_tx
                .send(c)
                .unwrap_or_else(|_e| unreachable!("change_rx should not be dropped"));
        }
    }

    /// Get a receiver for conf changes
    pub(super) fn change_rx(&self) -> flume::Receiver<ConfChange> {
        self.ctx.change_rx.clone()
    }

    /// Get all connects
    pub(super) fn connects(&self) -> &DashMap<ServerId, InnerConnectApiWrapper> {
        &self.ctx.connects
    }

    /// Insert connect
    pub(super) fn insert_connect(&self, connect: InnerConnectApiWrapper) {
        let _ig = self.ctx.connects.insert(connect.id(), connect);
    }

    /// Update connect
    pub(super) async fn update_connect(
        &self,
        id: ServerId,
        addrs: Vec<String>,
    ) -> Result<(), CurpError> {
        match self.ctx.connects.get(&id) {
            Some(connect) => connect
                .update_addrs(addrs)
                .await
                .map_err(|err| CurpError::Transport(err.to_string())),
            None => Ok(()),
        }
    }

    /// Get voters connects
    pub(super) fn voters_connects(&self) -> Vec<Arc<dyn InnerConnectApi>> {
        let cst_r = self.cst.lock();
        let voters = cst_r.config.voters();
        self.connects()
            .iter()
            .filter(|c| voters.contains(c.key()))
            .map(|c| Arc::clone(c.value()))
            .collect()
    }
}

// Utils
// Don't grab lock in the following functions(except cb or sp's lock)
impl<C: 'static + Command, RC: RoleChange + 'static> RawCurp<C, RC> {
    /// Server becomes a candidate
    fn become_candidate(
        &self,
        st: &mut State,
        cst: &mut CandidateState<C>,
        log: RwLockUpgradableReadGuard<'_, Log<C>>,
    ) -> Option<Vote> {
        let prev_role = st.role;
        assert_ne!(prev_role, Role::Leader, "leader can't start election");

        st.term += 1;
        st.role = Role::Candidate;
        st.voted_for = Some(self.id());
        st.leader_id = None;
        let _ig = self.ctx.leader_tx.send(None).ok();
        self.reset_election_tick();

        let self_sp = self
            .ctx
            .sp
            .map_lock(|sp| sp.pool.values().cloned().collect());

        if prev_role == Role::Follower {
            debug!("Follower {} starts election", self.id());
        } else {
            debug!("Candidate {} restarts election", self.id());
        }

        // vote to self
        debug!("{}'s vote is granted by server {}", self.id(), self.id());
        cst.votes_received = HashMap::from([(self.id(), true)]);
        cst.sps = HashMap::from([(self.id(), self_sp)]);

        if matches!(cst.check_vote(), VoteResult::Won) {
            // single node cluster
            // vote is granted by the majority of servers, can become leader
            let spec_pools = cst.sps.drain().collect();
            let mut log_w = RwLockUpgradableReadGuard::upgrade(log);
            self.recover_from_spec_pools(st, &mut log_w, spec_pools);
            self.recover_ucp_from_log(&mut log_w);
            self.become_leader(st);
            None
        } else {
            Some(Vote {
                term: st.term,
                candidate_id: self.id(),
                last_log_index: log.last_log_index(),
                last_log_term: log.last_log_term(),
            })
        }
    }

    /// Server becomes a leader
    fn become_leader(&self, st: &mut State) {
        st.role = Role::Leader;
        st.leader_id = Some(self.id());
        let _ig = self.ctx.leader_tx.send(Some(self.id())).ok();
        self.ctx.leader_event.notify(usize::MAX);
        self.ctx.role_change.on_election_win();
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
            self.ctx.role_change.on_calibrate();
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
            .lst
            .iter()
            .filter(|f| !f.is_learner && f.match_index >= i)
            .count()
            .numeric_cast();
        replicated_cnt + 1 >= self.quorum()
    }

    /// Recover from all voter's spec pools
    fn recover_from_spec_pools(
        &self,
        st: &mut State,
        log: &mut Log<C>,
        spec_pools: HashMap<ServerId, Vec<PoolEntry<C>>>,
    ) {
        if log_enabled!(Level::Debug) {
            let debug_sps: HashMap<ServerId, String> = spec_pools
                .iter()
                .map(|(id, sp)| {
                    let sp: Vec<String> = sp.iter().map(|cmd| cmd.id().to_string()).collect();
                    (*id, sp.join(","))
                })
                .collect();
            debug!("{} collected spec pools: {debug_sps:?}", self.id());
        }

        let mut cmd_cnt: HashMap<ProposeId, (PoolEntry<C>, u64)> = HashMap::new();
        for cmd in spec_pools.into_values().flatten() {
            let entry = cmd_cnt.entry(cmd.id()).or_insert((cmd, 0));
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
                !existing_log_ids.contains(&cmd.id())
            })
            .collect_vec();

        let mut cb_w = self.ctx.cb.write();
        let mut sp_l = self.ctx.sp.lock();

        let term = st.term;
        for cmd in recovered_cmds {
            let _ig_sync = cb_w.sync.insert(cmd.id()); // may have been inserted before
            let _ig_spec = sp_l.insert(cmd.clone()); // may have been inserted before
            #[allow(clippy::expect_used)]
            let entry = log
                .push(term, cmd)
                .expect("cmd {cmd:?} cannot be serialized");
            debug!(
                "{} recovers speculatively executed cmd({}) in log[{}]",
                self.id(),
                entry.id(),
                entry.index,
            );
        }
    }

    /// Recover the ucp from uncommitted log entries
    fn recover_ucp_from_log(&self, log: &mut Log<C>) {
        let mut ucp_l = self.ctx.ucp.lock();

        for i in log.commit_index + 1..=log.last_log_index() {
            let entry = log.get(i).unwrap_or_else(|| {
                unreachable!("system corrupted, get a `None` value on log[{i}]")
            });
            match entry.entry_data {
                EntryData::Command(ref cmd) => {
                    let _ignore = ucp_l.insert(cmd.id(), Arc::clone(cmd).into());
                }
                EntryData::ConfChange(ref conf_change) => {
                    let _ignore =
                        ucp_l.insert(conf_change.id(), conf_change.as_ref().clone().into());
                }
                EntryData::Shutdown => {}
            }
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
            self.ctx.cmd_tx.send_after_sync(Arc::clone(entry));
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
        (self.ctx.cluster_info.voters_len() / 2 + 1).numeric_cast()
    }

    /// Get `recover_quorum`: the smallest number of servers who must contain a command in speculative pool for it to be recovered
    fn recover_quorum(&self) -> u64 {
        self.quorum() / 2 + 1
    }

    /// When leader retires, it should reset state
    fn leader_retires(&self) {
        debug!("leader {} retires", self.id());
        self.ctx.cb.write().clear();
        self.ctx.ucp.lock().clear();
    }

    /// Switch to a new config and return old member infos for fallback
    fn switch_config(&self, conf_change: ConfChange) -> (Vec<String>, String, bool) {
        let node_id = conf_change.node_id;
        let fallback_info = match conf_change.change_type() {
            ConfChangeType::Add | ConfChangeType::AddLearner => {
                let is_learner = matches!(conf_change.change_type(), ConfChangeType::AddLearner);
                let member = Member::new(node_id, "", conf_change.address.clone(), is_learner);
                self.cst
                    .map_lock(|mut cst_l| _ = cst_l.config.insert(node_id, is_learner));
                self.lst.insert(node_id, is_learner);
                _ = self.ctx.sync_events.insert(node_id, Arc::new(Event::new()));
                self.ctx.cluster_info.insert(member);
                (vec![], String::new(), is_learner)
            }
            ConfChangeType::Remove => {
                // the removed follower need to commit the conf change entry, so when leader applies
                // the conf change entry, it will not remove the follower status, and after the log
                // entry is committed on the removed follower, leader will remove the follower status
                // and stop the sync_follower_task.
                self.cst
                    .map_lock(|mut cst_l| _ = cst_l.config.remove(node_id));
                let m = self.ctx.cluster_info.remove(&node_id);
                _ = self.ctx.sync_events.remove(&node_id);
                _ = self.ctx.connects.remove(&node_id);
                let removed_member =
                    m.unwrap_or_else(|| unreachable!("the member should exist before remove"));
                (
                    removed_member.addrs,
                    removed_member.name,
                    removed_member.is_learner,
                )
            }
            ConfChangeType::Update => {
                let old_addrs = self
                    .ctx
                    .cluster_info
                    .update(&node_id, conf_change.address.clone());
                (old_addrs, String::new(), false)
            }
            ConfChangeType::Promote => {
                self.cst.map_lock(|mut cst_l| {
                    _ = cst_l.config.learners.remove(&node_id);
                    _ = cst_l.config.insert(node_id, false);
                });
                self.ctx.cluster_info.promote(node_id);
                self.lst.promote(node_id);
                (vec![], String::new(), false)
            }
        };
        if self.is_leader() {
            self.ctx
                .change_tx
                .send(conf_change)
                .unwrap_or_else(|_e| unreachable!("change_rx should not be dropped"));
        }
        fallback_info
    }

    /// Entry process shared by `handle_xxx`
    fn entry_process(
        &self,
        log_w: &mut RwLockWriteGuard<'_, Log<C>>,
        entry: Arc<LogEntry<C>>,
        conflict: bool,
    ) {
        let index = entry.index;
        if !conflict {
            log_w.last_exe = index;
            self.ctx.cmd_tx.send_sp_exe(entry);
        }
        self.ctx.sync_events.iter().for_each(|e| {
            let next = self.lst.get_next_index(*e.key());
            if next > log_w.base_index && log_w.has_next_batch(next) {
                e.notify(1);
            }
        });

        // check if commit_index needs to be updated
        if self.can_update_commit_index_to(log_w, index, self.term()) && index > log_w.commit_index
        {
            log_w.commit_to(index);
            debug!("{} updates commit index to {index}", self.id());
            self.apply(&mut *log_w);
        }
    }

    /// Insert entry to spec pool
    fn insert_sp(&self, pool_entry: impl Into<PoolEntry<C>>) -> bool {
        self.ctx
            .sp
            .map_lock(|mut spec_l| spec_l.insert(pool_entry).is_some())
    }

    /// Insert entry to uncommitted pool
    fn insert_ucp(&self, pool_entry: impl Into<PoolEntry<C>>) -> bool {
        let entry = pool_entry.into();
        self.ctx.ucp.map_lock(|mut ucp_l| {
            let conflict_uncommitted = ucp_l.values().any(|c| c.is_conflict(&entry));
            assert!(
                ucp_l.insert(entry.id(), entry.clone()).is_none(),
                "cmd should never be inserted to uncommitted pool twice"
            );
            conflict_uncommitted
        })
    }
}
