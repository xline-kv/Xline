//! READ THIS BEFORE YOU START WRITING CODE FOR THIS MODULE
//! To avoid deadlock, let's make some rules:
//! 1. To group similar functions, I divide Curp impl into three scope: one for utils(don't grab lock here), one for tick, one for handlers
//! 2. Lock order should be:
//!     1. self.st
//!     2. self.cst
//!     3. self.log

#![allow(clippy::similar_names)] // st, lst, cst is similar but not confusing
#![allow(clippy::arithmetic_side_effects)] // u64 is large enough and won't overflow

use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use curp_external_api::cmd::ConflictCheck;
use dashmap::DashMap;
use derive_builder::Builder;
use event_listener::Event;
use itertools::Itertools;
use opentelemetry::KeyValue;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use tokio::sync::{broadcast, mpsc, oneshot};
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::{
    debug, error,
    log::{log_enabled, Level},
    trace, warn,
};
#[cfg(madsim)]
use utils::ClientTlsConfig;
use utils::{
    config::CurpConfig,
    parking_lot_lock::{MutexMap, RwLockMap},
    task_manager::TaskManager,
};

use self::{
    log::Log,
    state::{CandidateState, LeaderState, State},
};
use super::{cmd_worker::CEEventTxApi, lease_manager::LeaseManagerRef, storage::StorageApi, DB};
use crate::{
    cmd::Command,
    log_entry::{EntryData, LogEntry},
    members::{ClusterInfo, ServerId},
    quorum, recover_quorum,
    role_change::RoleChange,
    rpc::{
        connect::{InnerConnectApi, InnerConnectApiWrapper},
        ConfChange, ConfChangeType, CurpError, IdSet, Member, PoolEntry, PoolEntryInner, ProposeId,
        PublishRequest, ReadState,
    },
    server::{
        cmd_board::CmdBoardRef,
        metrics,
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
pub struct RawCurp<C: Command, RC: RoleChange> {
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
    /// Task manager
    task_manager: Arc<TaskManager>,
}

/// Tmp struct for building `RawCurp`
#[derive(Builder)]
#[builder(name = "RawCurpBuilder")]
pub(super) struct RawCurpArgs<C: Command, RC: RoleChange> {
    /// Cluster information
    cluster_info: Arc<ClusterInfo>,
    /// Current node is leader or not
    is_leader: bool,
    /// Cmd board for tracking the cmd sync results
    cmd_board: CmdBoardRef<C>,
    /// Speculative pool
    spec_pool: SpecPoolRef<C>,
    /// Lease Manager
    lease_manager: LeaseManagerRef,
    /// Uncommitted pool
    uncommitted_pool: UncommittedPoolRef<C>,
    /// Config
    cfg: Arc<CurpConfig>,
    /// Tx to send cmds to execute and do after sync
    cmd_tx: Arc<dyn CEEventTxApi<C>>,
    /// Tx to send log entries
    log_tx: mpsc::UnboundedSender<Arc<LogEntry<C>>>,
    /// Role change callback
    role_change: RC,
    /// Task manager
    task_manager: Arc<TaskManager>,
    /// Sync events
    sync_events: DashMap<ServerId, Arc<Event>>,
    /// Connects of peers
    connects: DashMap<ServerId, InnerConnectApiWrapper>,
    /// curp storage
    curp_storage: Arc<DB<C>>,
    /// client tls config
    #[builder(default)]
    client_tls_config: Option<ClientTlsConfig>,
    /// Last applied index
    #[builder(setter(strip_option), default)]
    last_applied: Option<LogIndex>,
    /// Voted for
    #[builder(default)]
    voted_for: Option<(u64, ServerId)>,
    /// Log entries
    #[builder(default)]
    entries: Vec<LogEntry<C>>,
}

impl<C: Command, RC: RoleChange> RawCurpBuilder<C, RC> {
    /// build `RawCurp` from `RawCurpBuilder`
    pub(super) fn build_raw_curp(&mut self) -> Result<RawCurp<C, RC>, RawCurpBuilderError> {
        let args = self.build()?;

        let st = RwLock::new(State::new(
            args.cfg.follower_timeout_ticks,
            args.cfg.candidate_timeout_ticks,
        ));
        let lst = LeaderState::new(&args.cluster_info.peers_ids());
        let cst = Mutex::new(CandidateState::new(args.cluster_info.all_ids().into_iter()));
        let log = RwLock::new(Log::new(
            args.log_tx,
            args.cfg.batch_max_size,
            args.cfg.log_entries_cap,
        ));

        let ctx = Context::builder()
            .cluster_info(args.cluster_info)
            .cb(args.cmd_board)
            .sp(args.spec_pool)
            .lm(args.lease_manager)
            .ucp(args.uncommitted_pool)
            .cfg(args.cfg)
            .cmd_tx(args.cmd_tx)
            .sync_events(args.sync_events)
            .role_change(args.role_change)
            .connects(args.connects)
            .curp_storage(args.curp_storage)
            .client_tls_config(args.client_tls_config)
            .build()
            .map_err(|e| match e {
                ContextBuilderError::UninitializedField(s) => {
                    RawCurpBuilderError::UninitializedField(s)
                }
                ContextBuilderError::ValidationError(s) => RawCurpBuilderError::ValidationError(s),
            })?;

        let raw_curp = RawCurp {
            st,
            lst,
            cst,
            log,
            ctx,
            task_manager: args.task_manager,
        };

        if args.is_leader {
            let mut st_w = raw_curp.st.write();
            st_w.term = 1;
            raw_curp.become_leader(&mut st_w);
        }

        if let Some((term, server_id)) = args.voted_for {
            let mut st_w = raw_curp.st.write();
            raw_curp.update_to_term_and_become_follower(&mut st_w, term);
            st_w.voted_for = Some(server_id);
        }

        if !args.entries.is_empty() {
            let last_applied = args.last_applied.ok_or_else(|| {
                RawCurpBuilderError::ValidationError("last_applied is not set".to_owned())
            })?;
            let mut log_w = raw_curp.log.write();
            log_w.last_as = last_applied;
            log_w.last_exe = last_applied;
            log_w.commit_index = last_applied;
            log_w.restore_entries(args.entries);
        }

        Ok(raw_curp)
    }
}

impl<C: Command, RC: RoleChange> Debug for RawCurp<C, RC> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawCurp")
            .field("st", &self.st)
            .field("lst", &self.lst)
            .field("cst", &self.cst)
            .field("log", &self.log)
            .field("ctx", &self.ctx)
            .field("task_manager", &self.task_manager)
            .finish()
    }
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
    /// Is this a pre vote
    pub(super) is_pre_vote: bool,
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
    /// PreCandidate
    PreCandidate,
    /// Candidate
    Candidate,
    /// Leader
    Leader,
}

/// Relevant context for Curp
#[derive(Builder)]
#[builder(build_fn(skip))]
struct Context<C: Command, RC: RoleChange> {
    /// Cluster information
    cluster_info: Arc<ClusterInfo>,
    /// Config
    cfg: Arc<CurpConfig>,
    /// Client tls config
    client_tls_config: Option<ClientTlsConfig>,
    /// Cmd board for tracking the cmd sync results
    cb: CmdBoardRef<C>,
    /// Speculative pool
    sp: SpecPoolRef<C>,
    /// The lease manager
    lm: LeaseManagerRef,
    /// Uncommitted pool
    ucp: UncommittedPoolRef<C>,
    /// Tx to send leader changes
    #[builder(setter(skip))]
    leader_tx: broadcast::Sender<Option<ServerId>>,
    /// Election tick
    #[builder(setter(skip))]
    election_tick: AtomicU8,
    /// Tx to send cmds to execute and do after sync
    cmd_tx: Arc<dyn CEEventTxApi<C>>,
    /// Followers sync event trigger
    sync_events: DashMap<ServerId, Arc<Event>>,
    /// Become leader event
    #[builder(setter(skip))]
    leader_event: Arc<Event>,
    /// Leader change callback
    role_change: RC,
    /// Conf change tx, used to update sync tasks
    #[builder(setter(skip))]
    change_tx: flume::Sender<ConfChange>,
    /// Conf change rx, used to update sync tasks
    #[builder(setter(skip))]
    change_rx: flume::Receiver<ConfChange>,
    /// Connects of peers
    connects: DashMap<ServerId, InnerConnectApiWrapper>,
    /// last conf change idx
    #[builder(setter(skip))]
    last_conf_change_idx: AtomicU64,
    /// Curp storage
    curp_storage: Arc<DB<C>>,
}

impl<C: Command, RC: RoleChange> Context<C, RC> {
    /// Create a new `ContextBuilder`
    pub(super) fn builder() -> ContextBuilder<C, RC> {
        ContextBuilder::default()
    }
}

impl<C: Command, RC: RoleChange> ContextBuilder<C, RC> {
    /// Build the context from the builder
    pub(super) fn build(&mut self) -> Result<Context<C, RC>, ContextBuilderError> {
        let (change_tx, change_rx) = flume::bounded(CHANGE_CHANNEL_SIZE);
        Ok(Context {
            cluster_info: match self.cluster_info.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("cluster_info")),
            },
            cfg: match self.cfg.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("cfg")),
            },
            cb: match self.cb.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("cb")),
            },
            sp: match self.sp.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("sp")),
            },
            lm: match self.lm.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("lm")),
            },
            ucp: match self.ucp.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("ucp")),
            },
            leader_tx: broadcast::channel(1).0,
            election_tick: AtomicU8::new(0),
            cmd_tx: match self.cmd_tx.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("cmd_tx")),
            },
            sync_events: match self.sync_events.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("sync_events")),
            },
            leader_event: Arc::new(Event::new()),
            role_change: match self.role_change.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("role_change")),
            },
            change_tx,
            change_rx,
            connects: match self.connects.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("connects")),
            },
            last_conf_change_idx: AtomicU64::new(0),
            curp_storage: match self.curp_storage.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("curp_storage")),
            },
            client_tls_config: match self.client_tls_config.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("client_tls_config")),
            },
        })
    }
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
            .finish()
    }
}

// Tick
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Tick
    pub(super) fn tick_election(&self) -> Option<Vote> {
        let st_r = self.st.upgradable_read();
        let timeout = match st_r.role {
            Role::Follower | Role::Leader => st_r.follower_timeout_ticks,
            Role::PreCandidate | Role::Candidate => st_r.candidate_timeout_ticks,
        };
        let tick = self.ctx.election_tick.fetch_add(1, Ordering::AcqRel);
        if tick < timeout {
            return None;
        }
        let mut st_w = RwLockUpgradableReadGuard::upgrade(st_r);
        let mut cst_l = self.cst.lock();
        let log_r = self.log.upgradable_read();
        match st_w.role {
            Role::Follower | Role::PreCandidate => {
                self.become_pre_candidate(&mut st_w, &mut cst_l, log_r)
            }
            Role::Candidate => self.become_candidate(&mut st_w, &mut cst_l, log_r),
            Role::Leader => {
                self.lst.reset_transferee();
                None
            }
        }
    }
}

// Curp handlers
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Handle `propose` request
    /// Return `true` if the leader speculatively executed the command
    pub(super) fn handle_propose(
        &self,
        propose_id: ProposeId,
        cmd: Arc<C>,
    ) -> Result<bool, CurpError> {
        debug!("{} gets proposal for cmd({})", self.id(), propose_id);
        let mut conflict = self.insert_sp(propose_id, Arc::clone(&cmd));
        let st_r = self.st.read();
        // Non-leader doesn't need to sync or execute
        if st_r.role != Role::Leader {
            if conflict {
                metrics::get()
                    .proposals_failed
                    .add(1, &[KeyValue::new("reason", "follower key conflict")]);
                return Err(CurpError::key_conflict());
            }
            return Ok(false);
        }
        if self.lst.get_transferee().is_some() {
            return Err(CurpError::LeaderTransfer("leader transferring".to_owned()));
        }
        if !self
            .ctx
            .cb
            .map_write(|mut cb_w| cb_w.sync.insert(propose_id))
        {
            metrics::get()
                .proposals_failed
                .add(1, &[KeyValue::new("reason", "duplicated proposal")]);
            return Err(CurpError::duplicated());
        }

        // leader also needs to check if the cmd conflicts un-synced commands
        conflict |= self.insert_ucp(propose_id, Arc::clone(&cmd));
        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, propose_id, cmd).map_err(|e| {
            metrics::get()
                .proposals_failed
                .add(1, &[KeyValue::new("reason", "log serialize failed")]);
            e
        })?;
        debug!("{} gets new log[{}]", self.id(), entry.index);

        self.entry_process(&mut log_w, entry, conflict, st_r.term);

        if conflict {
            metrics::get()
                .proposals_failed
                .add(1, &[KeyValue::new("reason", "leader key conflict")]);
            return Err(CurpError::key_conflict());
        }

        Ok(true)
    }

    /// Handle `shutdown` request
    pub(super) fn handle_shutdown(&self, propose_id: ProposeId) -> Result<(), CurpError> {
        let st_r = self.st.read();
        if st_r.role != Role::Leader {
            return Err(CurpError::redirect(st_r.leader_id, st_r.term));
        }
        if self.lst.get_transferee().is_some() {
            return Err(CurpError::LeaderTransfer("leader transferring".to_owned()));
        }
        let mut log_w = self.log.write();
        let entry = log_w
            .push(st_r.term, propose_id, EntryData::Shutdown)
            .map_err(|e| {
                metrics::get()
                    .proposals_failed
                    .add(1, &[KeyValue::new("reason", "log serialize failed")]);
                e
            })?;
        debug!("{} gets new log[{}]", self.id(), entry.index);
        self.entry_process(&mut log_w, entry, true, st_r.term);
        Ok(())
    }

    /// Handle `propose_conf_change` request
    pub(super) fn handle_propose_conf_change(
        &self,
        propose_id: ProposeId,
        conf_changes: Vec<ConfChange>,
    ) -> Result<(), CurpError> {
        debug!("{} gets conf change for with id {}", self.id(), propose_id);
        let st_r = self.st.read();

        // Non-leader doesn't need to sync or execute
        if st_r.role != Role::Leader {
            return Err(CurpError::redirect(st_r.leader_id, st_r.term));
        }

        if self.lst.get_transferee().is_some() {
            metrics::get()
                .proposals_failed
                .add(1, &[KeyValue::new("reason", "leader transferring")]);
            return Err(CurpError::LeaderTransfer("leader transferring".to_owned()));
        }
        self.check_new_config(&conf_changes)?;

        let mut conflict = self.insert_sp(propose_id, conf_changes.clone());
        conflict |= self.insert_ucp(propose_id, conf_changes.clone());

        let mut log_w = self.log.write();
        let entry = log_w
            .push(st_r.term, propose_id, conf_changes.clone())
            .map_err(|e| {
                metrics::get()
                    .proposals_failed
                    .add(1, &[KeyValue::new("reason", "log serialize failed")]);
                e
            })?;
        debug!("{} gets new log[{}]", self.id(), entry.index);
        let (addrs, name, is_learner) = self.apply_conf_change(conf_changes);
        self.ctx
            .last_conf_change_idx
            .store(entry.index, Ordering::Release);
        let _ig = log_w.fallback_contexts.insert(
            entry.index,
            FallbackContext::new(Arc::clone(&entry), addrs, name, is_learner),
        );
        self.entry_process(&mut log_w, entry, conflict, st_r.term);
        Ok(())
    }

    /// Handle `publish` request
    pub(super) fn handle_publish(&self, req: PublishRequest) -> Result<(), CurpError> {
        debug!(
            "{} gets publish with propose id {}",
            self.id(),
            req.propose_id()
        );
        let st_r = self.st.read();
        if st_r.role != Role::Leader {
            return Err(CurpError::redirect(st_r.leader_id, st_r.term));
        }
        if self.lst.get_transferee().is_some() {
            return Err(CurpError::leader_transfer("leader transferring"));
        }
        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, req.propose_id(), req).map_err(|e| {
            metrics::get()
                .proposals_failed
                .add(1, &[KeyValue::new("reason", "log serialize failed")]);
            e
        })?;
        debug!("{} gets new log[{}]", self.id(), entry.index);
        self.entry_process(&mut log_w, entry, false, st_r.term);
        Ok(())
    }

    /// Handle `lease_keep_alive` message
    pub(super) fn handle_lease_keep_alive(&self, client_id: u64) -> Option<u64> {
        let mut lm_w = self.ctx.lm.write();
        if client_id == 0 {
            return Some(lm_w.grant());
        }
        if lm_w.check_alive(client_id) {
            lm_w.renew(client_id);
            None
        } else {
            metrics::get().client_id_revokes.add(1, &[]);
            lm_w.revoke(client_id);
            Some(lm_w.grant())
        }
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
            let changes = conf_change.clone();
            self.fallback_conf_change(changes, info.addrs, info.name, info.is_learner);
        }
        // apply conf change entries
        for e in cc_entries {
            let EntryData::ConfChange(ref cc) = e.entry_data else {
                unreachable!("cc_entry should be conf change entry");
            };
            let (addrs, name, is_learner) = self.apply_conf_change(cc.clone());
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
                metrics::get()
                    .proposals_committed
                    .observe(last_sent_index, &[]);
                metrics::get()
                    .proposals_pending
                    .observe(log_w.last_log_index().overflow_sub(last_sent_index), &[]);
                self.apply(&mut *log_w);
            }
        }

        Ok(true)
    }

    /// Handle `vote`
    /// Return `Ok(term, spec_pool)` if the vote is granted
    /// Return `Err(Some(term))` if the vote is rejected
    /// The `Err(None)` will never be returned here, just to keep the return type consistent with the `handle_pre_vote`
    pub(super) fn handle_vote(
        &self,
        term: u64,
        candidate_id: ServerId,
        last_log_index: LogIndex,
        last_log_term: u64,
    ) -> Result<(u64, Vec<PoolEntry<C>>), Option<u64>> {
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
            return Err(Some(st_w.term));
        }
        if term > st_w.term {
            self.update_to_term_and_become_follower(&mut st_w, term);
        }

        // check self role
        if !matches!(st_w.role, Role::Follower | Role::PreCandidate) {
            return Err(Some(st_w.term));
        }

        // check if voted before
        if st_w
            .voted_for
            .as_ref()
            .map_or(false, |id| id != &candidate_id)
        {
            return Err(Some(st_w.term));
        }

        // check if the candidate's log is up-to-date
        if !log_r.log_up_to_date(last_log_term, last_log_index) {
            return Err(Some(st_w.term));
        }

        // grant the vote
        debug!("{} votes for server {}", self.id(), candidate_id);
        st_w.voted_for = Some(candidate_id);
        let self_spec_pool = self.ctx.sp.lock().pool.values().cloned().collect_vec();
        self.reset_election_tick();
        Ok((st_w.term, self_spec_pool))
    }

    /// Handle `pre_vote`
    /// Return `Ok(term, spec_pool)` if the vote is granted
    /// Return `Err(Some(term))` if the vote is rejected
    /// Return `Err(None)` if the candidate is removed from the cluster
    pub(super) fn handle_pre_vote(
        &self,
        term: u64,
        candidate_id: ServerId,
        last_log_index: LogIndex,
        last_log_term: u64,
    ) -> Result<(u64, Vec<PoolEntry<C>>), Option<u64>> {
        debug!(
            "{} received pre vote: term({}), last_log_index({}), last_log_term({}), id({})",
            self.id(),
            term,
            last_log_index,
            last_log_term,
            candidate_id
        );

        let st_r = self.st.read();
        let log_r = self.log.read();
        let contains_candidate = self.cluster().contains(candidate_id);
        let remove_candidate_is_not_committed =
            log_r
                .fallback_contexts
                .iter()
                .any(|(_, ctx)| match ctx.origin_entry.entry_data {
                    EntryData::ConfChange(ref cc) => cc.iter().any(|c| {
                        matches!(c.change_type(), ConfChangeType::Remove)
                            && c.node_id == candidate_id
                    }),
                    EntryData::Empty
                    | EntryData::Command(_)
                    | EntryData::Shutdown
                    | EntryData::SetNodeState(_, _, _) => false,
                });
        // extra check to shutdown removed node
        if !contains_candidate && !remove_candidate_is_not_committed {
            debug!(
                "{} received pre vote from removed node {}",
                self.id(),
                candidate_id
            );
            return Err(None);
        }

        // calibrate term
        if term < st_r.term {
            return Err(Some(st_r.term));
        }
        if term > st_r.term {
            let timeout = st_r.follower_timeout_ticks;
            if st_r.leader_id.is_some() && self.ctx.election_tick.load(Ordering::Acquire) < timeout
            {
                return Err(Some(st_r.term));
            }
        }

        // check if the candidate's log is up-to-date
        if !log_r.log_up_to_date(last_log_term, last_log_index) {
            return Err(Some(st_r.term));
        }

        // grant the vote
        debug!("{} pre votes for server {}", self.id(), candidate_id);
        Ok((st_r.term, vec![]))
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
        // TODO: Generate client id in the same way as client
        let propose_id = ProposeId(rand::random(), 0);
        let _ignore = log_w.push(st_w.term, propose_id, EntryData::Empty);
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

    /// Handle `pre_vote` responses
    /// Return `Ok(election_ends)` if succeeds
    /// Return `Err(())` if self is no longer a pre candidate
    pub(super) fn handle_pre_vote_resp(
        &self,
        id: ServerId,
        term: u64,
        vote_granted: bool,
    ) -> Result<Option<Vote>, ()> {
        let mut st_w = self.st.write();
        if st_w.term < term && !vote_granted {
            self.update_to_term_and_become_follower(&mut st_w, term);
            return Err(());
        }
        if st_w.role != Role::PreCandidate {
            return Err(());
        }

        let mut cst_w = self.cst.lock();
        let _ig = cst_w.votes_received.insert(id, vote_granted);

        if !vote_granted {
            return Ok(None);
        }

        debug!("{}'s pre vote is granted by server {}", self.id(), id);

        if !matches!(cst_w.check_vote(), VoteResult::Won) {
            return Ok(None);
        }

        let log_r = self.log.upgradable_read();
        Ok(self.become_candidate(&mut st_w, &mut cst_w, log_r))
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
            ReadState::Ids(IdSet::new(
                ids.into_iter()
                    .map(crate::log_entry::propose_id_to_inflight_id)
                    .collect(),
            ))
        }
    }

    /// Handle `move_leader`
    pub(super) fn handle_move_leader(&self, target_id: ServerId) -> Result<bool, CurpError> {
        debug!("{} received move leader to {}", self.id(), target_id);
        let st_r = self.st.read();
        if st_r.role != Role::Leader {
            return Err(CurpError::redirect(st_r.leader_id, st_r.term));
        }
        if !self
            .cluster()
            .get(&target_id)
            .is_some_and(|m| !m.is_learner)
        {
            return Err(CurpError::LeaderTransfer(
                "target node does not exist or it is a learner".to_owned(),
            ));
        }

        if target_id == self.id() {
            // transfer to self
            self.lst.reset_transferee();
            return Ok(false);
        }
        let last_leader_transferee = self.lst.swap_transferee(target_id);
        if last_leader_transferee.is_some_and(|id| id == target_id) {
            // Already transferring.
            return Ok(false);
        }
        self.reset_election_tick();
        let match_index = self
            .lst
            .get_match_index(target_id)
            .unwrap_or_else(|| unreachable!("node should exist,checked before"));
        if match_index == self.log.read().last_log_index() {
            Ok(true)
        } else {
            self.sync_event(target_id).notify(1);
            Ok(false)
        }
    }

    /// Handle `try_become_leader_now`
    pub(super) fn handle_try_become_leader_now(&self) -> Option<Vote> {
        debug!("{} received try become leader now", self.id());
        let mut st_w = self.st.write();
        if st_w.role == Role::Leader {
            return None;
        }
        if self.cluster().self_member().is_learner() {
            return None;
        }
        let mut cst_l = self.cst.lock();
        let log_r = self.log.upgradable_read();
        self.become_candidate(&mut st_w, &mut cst_l, log_r)
    }
}

/// Other small public interface
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Create a `RawCurpBuilder`
    pub(super) fn builder() -> RawCurpBuilder<C, RC> {
        RawCurpBuilder::default()
    }

    /// Get the leader id, attached with the term
    #[inline]
    pub fn leader(&self) -> (Option<ServerId>, u64, bool) {
        self.st
            .map_read(|st_r| (st_r.leader_id, st_r.term, st_r.role == Role::Leader))
    }

    /// Get commit index
    #[inline]
    pub fn commit_index(&self) -> LogIndex {
        self.log.read().commit_index
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
            let st_r = self.st.read();
            if st_r.role != Role::Leader {
                return None;
            }
            st_r.term
        };

        let Some(next_index) = self.lst.get_next_index(follower_id) else {
            warn!("follower {} is not found, it maybe has been removed", follower_id);
            return None;
        };
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

    /// Get the lease manager
    pub(super) fn lease_manager(&self) -> LeaseManagerRef {
        Arc::clone(&self.ctx.lm)
    }

    /// Get a reference to spec pool
    pub(super) fn spec_pool(&self) -> SpecPoolRef<C> {
        Arc::clone(&self.ctx.sp)
    }

    /// Get a reference to uncommitted pool
    pub(super) fn uncommitted_pool(&self) -> UncommittedPoolRef<C> {
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

    /// Check if the cluster is shutting down
    pub(super) fn is_shutdown(&self) -> bool {
        self.task_manager.is_shutdown()
    }

    /// Get a cloned task manager
    pub(super) fn task_manager(&self) -> Arc<TaskManager> {
        Arc::clone(&self.task_manager)
    }

    /// Check if the specified follower has caught up with the leader
    pub(super) fn is_synced(&self, node_id: ServerId) -> bool {
        let log_r = self.log.read();
        let leader_commit_index = log_r.commit_index;
        self.lst
            .get_match_index(node_id)
            .is_some_and(|match_index| match_index == leader_commit_index)
    }

    /// Check if the new config is valid
    pub(super) fn check_new_config(&self, changes: &[ConfChange]) -> Result<(), CurpError> {
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
                    return Err(CurpError::node_already_exists());
                }
            }
            ConfChangeType::Remove => {
                if !statuses_ids.remove(&node_id) || !config.remove(node_id) {
                    return Err(CurpError::node_not_exist());
                }
            }
            ConfChangeType::Update => {
                if statuses_ids.get(&node_id).is_none() || !config.contains(node_id) {
                    return Err(CurpError::node_not_exist());
                }
            }
            ConfChangeType::AddLearner => {
                if !statuses_ids.insert(node_id) || !config.insert(node_id, true) {
                    return Err(CurpError::node_already_exists());
                }
            }
            ConfChangeType::Promote => {
                if statuses_ids.get(&node_id).is_none() || !config.contains(node_id) {
                    metrics::get()
                        .learner_promote_failed
                        .add(1, &[KeyValue::new("reason", "learner not exist")]);
                    return Err(CurpError::node_not_exist());
                }
                let learner_index = self
                    .lst
                    .get_match_index(node_id)
                    .unwrap_or_else(|| unreachable!("learner should exist here"));
                let leader_index = self.log.read().last_log_index();
                if leader_index.overflow_sub(learner_index) > MAX_PROMOTE_GAP {
                    metrics::get()
                        .learner_promote_failed
                        .add(1, &[KeyValue::new("reason", "learner not catch up")]);
                    return Err(CurpError::learner_not_catch_up());
                }
            }
        }
        let mut all_nodes = HashSet::new();
        all_nodes.extend(config.voters());
        all_nodes.extend(&config.learners);
        if all_nodes != statuses_ids || !config.voters().is_disjoint(&config.learners) {
            return Err(CurpError::invalid_config());
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
        debug!("{} applies conf change {:?}", self.id(), conf_change);
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
        if is_learner {
            metrics::get().learner_promote_failed.add(
                1,
                &[KeyValue::new(
                    "reason",
                    "configuration revert by new leader",
                )],
            );
        }
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
                let _ig1 = self.ctx.cluster_info.remove(&node_id);
                let _ig2 = self.ctx.curp_storage.remove_member(node_id);
                _ = self.ctx.connects.remove(&node_id);
                Some(ConfChange::remove(node_id))
            }
            ConfChangeType::Remove => {
                let member = Member::new(node_id, name, old_addrs.clone(), [], is_learner);
                self.cst
                    .map_lock(|mut cst_l| _ = cst_l.config.insert(node_id, is_learner));
                self.lst.insert(node_id, is_learner);
                _ = self.ctx.sync_events.insert(node_id, Arc::new(Event::new()));
                let _ig1 = self.ctx.curp_storage.put_member(&member);
                let _ig2 = self.ctx.cluster_info.insert(member);
                if is_learner {
                    Some(ConfChange::add_learner(node_id, old_addrs))
                } else {
                    Some(ConfChange::add(node_id, old_addrs))
                }
            }
            ConfChangeType::Update => {
                _ = self.ctx.cluster_info.update(&node_id, old_addrs.clone());
                let m = self.ctx.cluster_info.get(&node_id).unwrap_or_else(|| {
                    unreachable!("node {} should exist in cluster info", node_id)
                });
                let _ig = self.ctx.curp_storage.put_member(&m);
                Some(ConfChange::update(node_id, old_addrs))
            }
            ConfChangeType::Promote => {
                self.cst.map_lock(|mut cst_l| {
                    _ = cst_l.config.remove(node_id);
                    _ = cst_l.config.insert(node_id, true);
                });
                self.ctx.cluster_info.demote(node_id);
                self.lst.demote(node_id);
                let m = self.ctx.cluster_info.get(&node_id).unwrap_or_else(|| {
                    unreachable!("node {} should exist in cluster info", node_id)
                });
                let _ig = self.ctx.curp_storage.put_member(&m);
                None
            }
        };
        self.ctx.cluster_info.cluster_version_update();
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
            Some(connect) => Ok(connect.update_addrs(addrs).await?),
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

    /// Get transferee
    pub(super) fn get_transferee(&self) -> Option<ServerId> {
        self.lst.get_transferee()
    }

    /// Get match index of a node
    pub(super) fn get_match_index(&self, id: ServerId) -> Option<u64> {
        self.lst.get_match_index(id)
    }

    /// Get last log index
    pub(super) fn last_log_index(&self) -> u64 {
        self.log.read().last_log_index()
    }

    /// Pick a node that has the same log as the current node
    pub(super) fn pick_new_leader(&self) -> Option<ServerId> {
        let last_idx = self.log.read().last_log_index();
        for (id, status) in self.lst.get_all_statuses() {
            if status.match_index == last_idx && !status.is_learner {
                return Some(id);
            }
        }
        None
    }

    /// Get client tls config
    pub(super) fn client_tls_config(&self) -> Option<&ClientTlsConfig> {
        self.ctx.client_tls_config.as_ref()
    }
}

// Utils
// Don't grab lock in the following functions(except cb or sp's lock)
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Server becomes a pre candidate
    fn become_pre_candidate(
        &self,
        st: &mut State,
        cst: &mut CandidateState<C>,
        log: RwLockUpgradableReadGuard<'_, Log<C>>,
    ) -> Option<Vote> {
        let prev_role = st.role;
        assert_ne!(prev_role, Role::Leader, "leader can't start election");
        assert_ne!(
            prev_role,
            Role::Candidate,
            "candidate can't become pre candidate"
        );

        st.role = Role::PreCandidate;
        cst.votes_received = HashMap::from([(self.id(), true)]);
        st.leader_id = None;
        let _ig = self.ctx.leader_tx.send(None).ok();
        self.reset_election_tick();

        if prev_role == Role::Follower {
            debug!("Follower {} starts pre election", self.id());
        } else {
            debug!("PreCandidate {} restarts pre election", self.id());
        }

        // vote to self
        debug!("{}'s vote is granted by server {}", self.id(), self.id());
        cst.votes_received = HashMap::from([(self.id(), true)]);

        if matches!(cst.check_vote(), VoteResult::Won) {
            self.become_candidate(st, cst, log)
        } else {
            Some(Vote {
                term: st.term.overflow_add(1),
                candidate_id: self.id(),
                last_log_index: log.last_log_index(),
                last_log_term: log.last_log_term(),
                is_pre_vote: true,
            })
        }
    }

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

        if prev_role == Role::PreCandidate {
            debug!("PreCandidate {} starts election", self.id());
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
                is_pre_vote: false,
            })
        }
    }

    /// Server becomes a leader
    fn become_leader(&self, st: &mut State) {
        metrics::get().leader_changes.add(1, &[]);
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
            // a leader fallback into the follower
            metrics::get().leader_changes.add(1, &[]);
        }
        st.term = term;
        self.lst.reset_transferee();
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

        let replicated_cnt = self
            .lst
            .iter()
            .filter(|f| !f.is_learner && f.match_index >= i)
            .count();
        replicated_cnt + 1 >= quorum(self.ctx.cluster_info.voters_len())
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
                    let sp: Vec<String> = sp
                        .iter()
                        .map(|entry: &PoolEntry<C>| entry.id.to_string())
                        .collect();
                    (*id, sp.join(","))
                })
                .collect();
            debug!("{} collected spec pools: {debug_sps:?}", self.id());
        }

        let mut entry_cnt: HashMap<ProposeId, (PoolEntry<C>, usize)> = HashMap::new();
        for entry in spec_pools.into_values().flatten() {
            let entry = entry_cnt.entry(entry.id).or_insert((entry, 0));
            entry.1 += 1;
        }

        // get all possibly executed(fast path) entries
        let existing_log_ids = log.get_cmd_ids();
        let recovered_cmds = entry_cnt
            .into_values()
            // only cmds whose cnt >= ( f + 1 ) / 2 + 1 can be recovered
            .filter_map(|(cmd, cnt)| {
                (cnt >= recover_quorum(self.ctx.cluster_info.voters_len())).then_some(cmd)
            })
            // dedup in current logs
            .filter(|entry| {
                // TODO: better dedup mechanism
                !existing_log_ids.contains(&entry.id)
            })
            .collect_vec();

        let mut cb_w = self.ctx.cb.write();
        let mut sp_l = self.ctx.sp.lock();

        let term = st.term;
        for entry in recovered_cmds {
            let _ig_sync = cb_w.sync.insert(entry.id); // may have been inserted before
            let _ig_spec = sp_l.insert(entry.clone()); // may have been inserted before
            #[allow(clippy::expect_used)]
            let entry = log
                .push(term, entry.id, entry.inner)
                .expect("cmd {cmd:?} cannot be serialized");
            debug!(
                "{} recovers speculatively executed cmd({}) in log[{}]",
                self.id(),
                entry.propose_id,
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
            let propose_id = entry.propose_id;
            match entry.entry_data {
                EntryData::Command(ref cmd) => {
                    let _ignore =
                        ucp_l.insert(propose_id, PoolEntry::new(propose_id, Arc::clone(cmd)));
                }
                EntryData::ConfChange(ref conf_change) => {
                    let _ignore =
                        ucp_l.insert(propose_id, PoolEntry::new(propose_id, conf_change.clone()));
                }
                EntryData::Shutdown | EntryData::Empty | EntryData::SetNodeState(_, _, _) => {}
            }
        }
    }

    /// Apply new logs
    fn apply(&self, log: &mut Log<C>) {
        for i in (log.last_as + 1)..=log.commit_index {
            metrics::get().proposals_applied.observe(i, &[]);
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

    /// When leader retires, it should reset state
    fn leader_retires(&self) {
        debug!("leader {} retires", self.id());
        self.ctx.cb.write().clear();
        self.ctx.lm.write().clear();
        self.ctx.ucp.lock().clear();
    }

    /// Switch to a new config and return old member infos for fallback
    fn switch_config(&self, conf_change: ConfChange) -> (Vec<String>, String, bool) {
        let node_id = conf_change.node_id;
        let mut cst_l = self.cst.lock();
        let (modified, fallback_info) = match conf_change.change_type() {
            ConfChangeType::Add | ConfChangeType::AddLearner => {
                let is_learner = matches!(conf_change.change_type(), ConfChangeType::AddLearner);
                let member = Member::new(node_id, "", conf_change.address.clone(), [], is_learner);
                _ = cst_l.config.insert(node_id, is_learner);
                self.lst.insert(node_id, is_learner);
                _ = self.ctx.sync_events.insert(node_id, Arc::new(Event::new()));
                let _ig = self.ctx.curp_storage.put_member(&member);
                let m = self.ctx.cluster_info.insert(member);
                (m.is_none(), (vec![], String::new(), is_learner))
            }
            ConfChangeType::Remove => {
                _ = cst_l.config.remove(node_id);
                self.lst.remove(node_id);
                _ = self.ctx.sync_events.remove(&node_id);
                _ = self.ctx.connects.remove(&node_id);
                let _ig = self.ctx.curp_storage.remove_member(node_id);
                let m = self.ctx.cluster_info.remove(&node_id);
                let removed_member =
                    m.unwrap_or_else(|| unreachable!("the member should exist before remove"));
                (
                    true,
                    (
                        removed_member.peer_urls,
                        removed_member.name,
                        removed_member.is_learner,
                    ),
                )
            }
            ConfChangeType::Update => {
                let old_addrs = self
                    .ctx
                    .cluster_info
                    .update(&node_id, conf_change.address.clone());
                let m = self.ctx.cluster_info.get(&node_id).unwrap_or_else(|| {
                    unreachable!("the member should exist after update");
                });
                let _ig = self.ctx.curp_storage.put_member(&m);
                (
                    old_addrs != conf_change.address,
                    (old_addrs, String::new(), false),
                )
            }
            ConfChangeType::Promote => {
                _ = cst_l.config.learners.remove(&node_id);
                _ = cst_l.config.insert(node_id, false);
                self.lst.promote(node_id);
                let modified = self.ctx.cluster_info.promote(node_id);
                let m = self.ctx.cluster_info.get(&node_id).unwrap_or_else(|| {
                    unreachable!("the member should exist after promote");
                });
                let _ig = self.ctx.curp_storage.put_member(&m);
                (modified, (vec![], String::new(), false))
            }
        };
        if modified {
            self.ctx.cluster_info.cluster_version_update();
        }
        if self.is_leader() {
            self.ctx
                .change_tx
                .send(conf_change)
                .unwrap_or_else(|_e| unreachable!("change_rx should not be dropped"));
            if self
                .lst
                .get_transferee()
                .is_some_and(|transferee| !cst_l.config.voters().contains(&transferee))
            {
                self.lst.reset_transferee();
            }
        }
        fallback_info
    }

    /// Entry process shared by `handle_xxx`
    fn entry_process(
        &self,
        log_w: &mut RwLockWriteGuard<'_, Log<C>>,
        entry: Arc<LogEntry<C>>,
        conflict: bool,
        term: u64,
    ) {
        let index = entry.index;
        if !conflict {
            log_w.last_exe = index;
            self.ctx.cmd_tx.send_sp_exe(entry);
        }
        self.ctx.sync_events.iter().for_each(|e| {
            if let Some(next) = self.lst.get_next_index(*e.key()) {
                if next > log_w.base_index && log_w.has_next_batch(next) {
                    e.notify(1);
                }
            }
        });

        // check if commit_index needs to be updated
        if self.can_update_commit_index_to(log_w, index, term) && index > log_w.commit_index {
            log_w.commit_to(index);
            metrics::get().proposals_committed.observe(index, &[]);
            metrics::get()
                .proposals_pending
                .observe(log_w.last_log_index().overflow_sub(index), &[]);
            debug!("{} updates commit index to {index}", self.id());
            self.apply(&mut *log_w);
        }
    }

    /// Insert entry to spec pool
    fn insert_sp(&self, propose_id: ProposeId, inner: impl Into<PoolEntryInner<C>>) -> bool {
        self.ctx
            .sp
            .map_lock(|mut spec_l| spec_l.insert(PoolEntry::new(propose_id, inner)).is_some())
    }

    /// Insert entry to uncommitted pool
    fn insert_ucp(&self, propose_id: ProposeId, inner: impl Into<PoolEntryInner<C>>) -> bool {
        let entry = PoolEntry::new(propose_id, inner);
        self.ctx.ucp.map_lock(|mut ucp_l| {
            let conflict_uncommitted = ucp_l.values().any(|c| c.is_conflict(&entry));
            assert!(
                ucp_l.insert(propose_id, entry.clone()).is_none(),
                "cmd should never be inserted to uncommitted pool twice"
            );
            conflict_uncommitted
        })
    }
}
