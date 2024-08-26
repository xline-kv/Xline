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
    cmp::{self, min},
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc,
    },
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use dashmap::DashMap;
use derive_builder::Builder;
use event_listener::Event;
use futures::Future;
use itertools::Itertools;
use opentelemetry::KeyValue;
use parking_lot::{Mutex, RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use tokio::sync::{broadcast, oneshot};
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
    barrier::IdBarrier,
    config::CurpConfig,
    parking_lot_lock::{MutexMap, RwLockMap},
    task_manager::TaskManager,
};

use self::{
    log::Log,
    state::{CandidateState, LeaderState, State},
};
use super::{
    cmd_board::CommandBoard,
    conflict::{spec_pool_new::SpeculativePool, uncommitted_pool::UncommittedPool},
    curp_node::TaskType,
    lease_manager::LeaseManagerRef,
    storage::StorageApi,
    DB,
};
use crate::{
    cmd::Command,
    log_entry::{EntryData, LogEntry},
    members::{ClusterInfo, ServerId},
    quorum, recover_quorum,
    response::ResponseSender,
    role_change::RoleChange,
    rpc::{
        connect::{InnerConnectApi, InnerConnectApiWrapper},
        ConfChange, ConfChangeType, CurpError, IdSet, Member, PoolEntry, ProposeId, PublishRequest,
        ReadState, Redirect,
    },
    server::{
        cmd_board::CmdBoardRef,
        metrics,
        raw_curp::{log::FallbackContext, state::VoteResult},
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
    /// Lease Manager
    lease_manager: LeaseManagerRef,
    /// Config
    cfg: Arc<CurpConfig>,
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
    /// Speculative pool
    spec_pool: Arc<Mutex<SpeculativePool<C>>>,
    /// Uncommitted pool
    uncommitted_pool: Arc<Mutex<UncommittedPool<C>>>,
    /// Tx to send entries to after_sync
    as_tx: flume::Sender<TaskType<C>>,
    /// Response Senders
    resp_txs: Arc<Mutex<HashMap<LogIndex, Arc<ResponseSender>>>>,
    /// Barrier for waiting unsynced commands
    id_barrier: Arc<IdBarrier<ProposeId>>,
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
        let log = RwLock::new(Log::new(args.cfg.batch_max_size, args.cfg.log_entries_cap));

        let ctx = Context::builder()
            .cluster_info(args.cluster_info)
            .cb(args.cmd_board)
            .lm(args.lease_manager)
            .cfg(args.cfg)
            .sync_events(args.sync_events)
            .role_change(args.role_change)
            .connects(args.connects)
            .curp_storage(args.curp_storage)
            .client_tls_config(args.client_tls_config)
            .spec_pool(args.spec_pool)
            .uncommitted_pool(args.uncommitted_pool)
            .as_tx(args.as_tx)
            .resp_txs(args.resp_txs)
            .id_barrier(args.id_barrier)
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
            log_w
                .restore_entries(args.entries)
                .map_err(|e| RawCurpBuilderError::ValidationError(e.to_string()))?;
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
///
/// WARN: To avoid deadlock, the lock order should be:
/// 1. `spec_pool`
/// 2. `uncommitted_pool`
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
    /// The lease manager
    lm: LeaseManagerRef,
    /// Tx to send leader changes
    #[builder(setter(skip))]
    leader_tx: broadcast::Sender<Option<ServerId>>,
    /// Election tick
    #[builder(setter(skip))]
    election_tick: AtomicU8,
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
    /// Speculative pool
    spec_pool: Arc<Mutex<SpeculativePool<C>>>,
    /// Uncommitted pool
    uncommitted_pool: Arc<Mutex<UncommittedPool<C>>>,
    /// Tx to send entries to after_sync
    as_tx: flume::Sender<TaskType<C>>,
    /// Response Senders
    // TODO: this could be replaced by a queue
    resp_txs: Arc<Mutex<HashMap<LogIndex, Arc<ResponseSender>>>>,
    /// Barrier for waiting unsynced commands
    id_barrier: Arc<IdBarrier<ProposeId>>,
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
            lm: match self.lm.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("lm")),
            },
            leader_tx: broadcast::channel(1).0,
            election_tick: AtomicU8::new(0),
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
            spec_pool: match self.spec_pool.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("spec_pool")),
            },
            uncommitted_pool: match self.uncommitted_pool.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("uncommitted_pool")),
            },
            as_tx: match self.as_tx.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("as_tx")),
            },
            resp_txs: match self.resp_txs.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("resp_txs")),
            },
            id_barrier: match self.id_barrier.take() {
                Some(value) => value,
                None => return Err(ContextBuilderError::UninitializedField("id_barrier")),
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

/// Term, entries
type AppendEntriesSuccess<C> = (u64, Vec<Arc<LogEntry<C>>>);
/// Term, index
type AppendEntriesFailure = (u64, LogIndex);

// Curp handlers
// TODO: Tidy up the handlers
//   Possible improvements:
//    * split metrics collection from CurpError into a separate function
//    * split the handlers into separate modules
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Checks the if term are up-to-date
    pub(super) fn check_term(&self, term: u64) -> Result<(), CurpError> {
        let st_r = self.st.read();

        // Rejects the request
        // When `st_r.term > term`, the client is using an outdated leader
        // When `st_r.term < term`, the current node is a zombie
        match st_r.term.cmp(&term) {
            // Current node is a zombie
            cmp::Ordering::Less => Err(CurpError::Zombie(())),
            cmp::Ordering::Greater => Err(CurpError::Redirect(Redirect {
                leader_id: st_r.leader_id.map(Into::into),
                term: st_r.term,
            })),
            cmp::Ordering::Equal => Ok(()),
        }
    }

    /// Handles record
    pub(super) fn follower_record(&self, propose_id: ProposeId, cmd: &Arc<C>) -> bool {
        let conflict = self
            .ctx
            .spec_pool
            .lock()
            .insert(PoolEntry::new(propose_id, Arc::clone(cmd)))
            .is_some();
        if conflict {
            metrics::get()
                .proposals_failed
                .add(1, &[KeyValue::new("reason", "follower key conflict")]);
        }
        conflict
    }

    /// Handles record
    pub(super) fn leader_record(&self, entries: impl Iterator<Item = PoolEntry<C>>) -> Vec<bool> {
        let mut sp_l = self.ctx.spec_pool.lock();
        let mut ucp_l = self.ctx.uncommitted_pool.lock();
        let mut conflicts = Vec::new();
        for entry in entries {
            let mut conflict = sp_l.insert(entry.clone()).is_some();
            conflict |= ucp_l.insert(&entry);
            conflicts.push(conflict);
        }
        metrics::get().proposals_failed.add(
            conflicts.iter().filter(|c| **c).count().numeric_cast(),
            &[KeyValue::new("reason", "leader key conflict")],
        );
        conflicts
    }

    /// Handles leader propose
    pub(super) fn push_logs(
        &self,
        proposes: Vec<(Arc<C>, ProposeId, u64, Arc<ResponseSender>)>,
    ) -> Vec<Arc<LogEntry<C>>> {
        let term = proposes
            .first()
            .unwrap_or_else(|| unreachable!("no propose in proposes"))
            .2;
        let mut log_entries = Vec::with_capacity(proposes.len());
        let mut to_process = Vec::with_capacity(proposes.len());
        let mut log_w = self.log.write();
        self.ctx.resp_txs.map_lock(|mut tx_map| {
            for propose in proposes {
                let (cmd, id, _term, resp_tx) = propose;
                let entry = log_w.push(term, id, cmd);
                let index = entry.index;
                let conflict = resp_tx.is_conflict();
                to_process.push((index, conflict));
                log_entries.push(entry);
                assert!(
                    tx_map.insert(index, Arc::clone(&resp_tx)).is_none(),
                    "Should not insert resp_tx twice"
                );
            }
        });
        self.entry_process_multi(&mut log_w, &to_process, term);

        let log_r = RwLockWriteGuard::downgrade(log_w);
        self.persistent_log_entries(
            &log_entries.iter().map(Arc::as_ref).collect::<Vec<_>>(),
            &log_r,
        );

        log_entries
    }

    /// Persistent log entries
    ///
    /// NOTE: A `&Log<C>` is required because we do not want the `Log` structure gets mutated
    /// during the persistent
    #[allow(clippy::panic)]
    #[allow(dropping_references)]
    fn persistent_log_entries(&self, entries: &[&LogEntry<C>], _log: &Log<C>) {
        // We panic when the log persistence fails because it likely indicates an unrecoverable error.
        // Our WAL implementation does not support rollback on failure, as a file write syscall is not
        // guaranteed to be atomic.
        if let Err(e) = self.ctx.curp_storage.put_log_entries(entries) {
            panic!("log persistent failed: {e}");
        }
    }

    /// Wait synced for all conflict commands
    pub(super) fn wait_conflicts_synced(&self, cmd: Arc<C>) -> impl Future<Output = ()> + Send {
        let conflict_cmds: Vec<_> = self
            .ctx
            .uncommitted_pool
            .lock()
            .all_conflict(&PoolEntry::new(ProposeId::default(), cmd))
            .into_iter()
            .map(|e| e.id)
            .collect();
        self.ctx.id_barrier.wait_all(conflict_cmds)
    }

    /// Wait all logs in previous term have been applied to state machine
    pub(super) fn wait_no_op_applied(&self) -> Box<dyn Future<Output = ()> + Send + Unpin> {
        // if the leader is at term 1, it won't commit a no-op log
        if self.term() == 1 {
            return Box::new(futures::future::ready(()));
        }
        Box::new(self.lst.wait_no_op_applied())
    }

    /// Sets the no-op log as applied
    pub(super) fn set_no_op_applied(&self) {
        self.lst.set_no_op_applied();
    }

    /// Trigger the barrier of the given inflight id.
    pub(super) fn trigger(&self, propose_id: &ProposeId) {
        self.ctx.id_barrier.trigger(propose_id);
    }

    /// Returns `CurpError::LeaderTransfer` if the leadership is transferring
    pub(super) fn check_leader_transfer(&self) -> Result<(), CurpError> {
        if self.lst.get_transferee().is_some() {
            return Err(CurpError::LeaderTransfer("leader transferring".to_owned()));
        }
        Ok(())
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
        self.deduplicate(propose_id, None)?;
        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, propose_id, EntryData::Shutdown);
        debug!("{} gets new log[{}]", self.id(), entry.index);
        self.entry_process_single(&mut log_w, entry.as_ref(), true, st_r.term);

        let log_r = RwLockWriteGuard::downgrade(log_w);
        self.persistent_log_entries(&[entry.as_ref()], &log_r);

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

        self.deduplicate(propose_id, None)?;
        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, propose_id, conf_changes.clone());
        debug!("{} gets new log[{}]", self.id(), entry.index);
        let apply_opt = self.apply_conf_change(conf_changes);
        self.ctx
            .last_conf_change_idx
            .store(entry.index, Ordering::Release);
        if let Some((addrs, name, is_learner)) = apply_opt {
            let _ig = log_w.fallback_contexts.insert(
                entry.index,
                FallbackContext::new(Arc::clone(&entry), addrs, name, is_learner),
            );
        }
        self.entry_process_single(&mut log_w, &entry, false, st_r.term);

        let log_r = RwLockWriteGuard::downgrade(log_w);
        self.persistent_log_entries(&[entry.as_ref()], &log_r);

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

        self.deduplicate(req.propose_id(), None)?;

        let mut log_w = self.log.write();
        let entry = log_w.push(st_r.term, req.propose_id(), req);
        debug!("{} gets new log[{}]", self.id(), entry.index);
        self.entry_process_single(&mut log_w, entry.as_ref(), false, st_r.term);

        let log_r = RwLockWriteGuard::downgrade(log_w);
        self.persistent_log_entries(&[entry.as_ref()], &log_r);

        Ok(())
    }

    /// Handle `lease_keep_alive` message
    pub(super) fn handle_lease_keep_alive(&self, client_id: u64) -> Option<u64> {
        let mut lm_w = self.ctx.lm.write();
        if client_id == 0 {
            return Some(lm_w.grant(None));
        }
        if lm_w.check_alive(client_id) {
            lm_w.renew(client_id, None);
            None
        } else {
            metrics::get().client_id_revokes.add(1, &[]);
            lm_w.revoke(client_id);
            Some(lm_w.grant(None))
        }
    }

    /// Handle `append_entries`
    /// Return `Ok(term, entries)` if succeeds
    /// Return `Err(term, hint_index)` if fails
    pub(super) fn handle_append_entries(
        &self,
        term: u64,
        leader_id: ServerId,
        prev_log_index: LogIndex,
        prev_log_term: u64,
        entries: Vec<LogEntry<C>>,
        leader_commit: LogIndex,
    ) -> Result<AppendEntriesSuccess<C>, AppendEntriesFailure> {
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
        let (to_persist, cc_entries, fallback_indexes) = log_w
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
            let Some((addrs, name, is_learner)) = self.apply_conf_change(cc.clone()) else {
                continue;
            };
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
        Ok((term, to_persist))
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
        let self_spec_pool = self.ctx.spec_pool.lock().all();
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
    #[allow(clippy::shadow_unrelated)] // allow reuse the `_ignore` variable name.
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
        let _ignore = cst_w.votes_received.insert(id, vote_granted);

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
        let entry = log_w.push(st_w.term, propose_id, EntryData::Empty);
        self.persistent_log_entries(&[&entry], &log_w);
        self.recover_from_spec_pools(&st_w, &mut log_w, spec_pools);
        self.recover_ucp_from_log(&log_w);
        let last_log_index = log_w.last_log_index();

        self.become_leader(&mut st_w);

        // update next_index for each follower
        for other in self.ctx.cluster_info.peers_ids() {
            self.lst.update_next_index(other, last_log_index + 1); // iter from the end to front is more likely to match the follower
        }
        if prev_last_log_index < last_log_index {
            // if some entries are recovered, sync with followers immediately
            self.ctx.sync_events.iter().for_each(|event| {
                let _ignore = event.notify(1);
            });
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
    pub(super) fn handle_fetch_read_state(&self, cmd: Arc<C>) -> ReadState {
        let ids: Vec<_> = self
            .ctx
            .uncommitted_pool
            .map_lock(|ucp| ucp.all_conflict(&PoolEntry::new(ProposeId::default(), cmd)))
            .into_iter()
            .map(|entry| entry.id)
            .collect();
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
            let _ignore = self.sync_event(target_id).notify(1);
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
            warn!(
                "follower {} is not found, it maybe has been removed",
                follower_id
            );
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
            let meta = SnapshotMeta {
                last_included_index: entry.index,
                last_included_term: entry.term,
            };
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self.ctx.as_tx.send(TaskType::Snapshot(meta, tx)) {
                error!("failed to send task to after sync: {e}");
            }
            Some(SyncAction::Snapshot(rx))
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
    pub(super) fn spec_pool(&self) -> &Mutex<SpeculativePool<C>> {
        &self.ctx.spec_pool
    }

    /// Get a reference to uncommitted pool
    pub(super) fn uncommitted_pool(&self) -> &Mutex<UncommittedPool<C>> {
        &self.ctx.uncommitted_pool
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

    /// Check if the current node is shutting down
    pub(super) fn is_node_shutdown(&self) -> bool {
        self.task_manager.is_node_shutdown()
    }

    /// Check if the current node is shutting down
    pub(super) fn is_cluster_shutdown(&self) -> bool {
        self.task_manager.is_cluster_shutdown()
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
    ) -> Option<(Vec<String>, String, bool)> {
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
        #[allow(clippy::explicit_auto_deref)] // Avoid compiler complaint about `Dashmap::Ref` type
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
                let _ig = self.ctx.curp_storage.put_member(&*m);
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
                let _ig = self.ctx.curp_storage.put_member(&*m);
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

    /// Get last applied index
    pub(super) fn last_applied(&self) -> u64 {
        self.log.read().last_as
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

    /// Mark a client id as bypassed
    pub(super) fn mark_client_id_bypassed(&self, client_id: u64) {
        let mut lm_w = self.ctx.lm.write();
        lm_w.bypass(client_id);
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

        let self_sp = self.ctx.spec_pool.map_lock(|sp| sp.all());

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
            self.recover_ucp_from_log(&log_w);
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
        let _ignore = self.ctx.leader_event.notify(usize::MAX);
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
        st: &State,
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

        let mut sp_l = self.ctx.spec_pool.lock();

        let term = st.term;
        let mut entries = vec![];
        for entry in recovered_cmds {
            let _ig_spec = sp_l.insert(entry.clone()); // may have been inserted before
            #[allow(clippy::expect_used)]
            let entry = log.push(term, entry.id, entry.cmd);
            debug!(
                "{} recovers speculatively executed cmd({}) in log[{}]",
                self.id(),
                entry.propose_id,
                entry.index,
            );
            entries.push(entry);
        }

        self.persistent_log_entries(&entries.iter().map(Arc::as_ref).collect::<Vec<_>>(), log);
    }

    /// Recover the ucp from uncommitted log entries
    fn recover_ucp_from_log(&self, log: &Log<C>) {
        let mut ucp_l = self.ctx.uncommitted_pool.lock();

        for i in log.commit_index + 1..=log.last_log_index() {
            let entry = log.get(i).unwrap_or_else(|| {
                unreachable!("system corrupted, get a `None` value on log[{i}]")
            });
            let propose_id = entry.propose_id;
            match entry.entry_data {
                EntryData::Command(ref cmd) => {
                    let _ignore = ucp_l.insert(&PoolEntry::new(propose_id, Arc::clone(cmd)));
                }
                EntryData::ConfChange(_)
                | EntryData::Shutdown
                | EntryData::Empty
                | EntryData::SetNodeState(_, _, _) => {}
            }
        }
    }

    /// Apply new logs
    fn apply(&self, log: &mut Log<C>) {
        let mut entries = Vec::new();
        let mut resp_txs_l = self.ctx.resp_txs.lock();
        for i in (log.last_as + 1)..=log.commit_index {
            let entry = log.get(i).unwrap_or_else(|| {
                unreachable!(
                    "system corrupted, apply log[{i}] when we only have {} log entries",
                    log.last_log_index()
                )
            });
            let tx = resp_txs_l.remove(&i);
            entries.push((Arc::clone(entry), tx));
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
        debug!("sending {} entries to after sync task", entries.len());
        let _ignore = self.ctx.as_tx.send(TaskType::Entries(entries));
        log.compact();
    }

    /// When leader retires, it should reset state
    fn leader_retires(&self) {
        debug!("leader {} retires", self.id());
        self.ctx.cb.write().clear();
        self.ctx.lm.write().clear();
        self.ctx.uncommitted_pool.lock().clear();
        self.lst.reset_no_op_state();
    }

    /// Switch to a new config and return old member infos for fallback
    ///
    /// FIXME: The state of `ctx.cluster_info` might be inconsistent with the log. A potential
    /// fix would be to include the entire cluster info in the conf change log entry and
    /// overwrite `ctx.cluster_info` when switching
    fn switch_config(&self, conf_change: ConfChange) -> Option<(Vec<String>, String, bool)> {
        let node_id = conf_change.node_id;
        let mut cst_l = self.cst.lock();
        #[allow(clippy::explicit_auto_deref)] // Avoid compiler complaint about `Dashmap::Ref` type
        let (modified, fallback_info) = match conf_change.change_type() {
            ConfChangeType::Add | ConfChangeType::AddLearner => {
                let is_learner = matches!(conf_change.change_type(), ConfChangeType::AddLearner);
                let member = Member::new(node_id, "", conf_change.address.clone(), [], is_learner);
                _ = cst_l.config.insert(node_id, is_learner);
                self.lst.insert(node_id, is_learner);
                _ = self.ctx.sync_events.insert(node_id, Arc::new(Event::new()));
                let _ig = self.ctx.curp_storage.put_member(&member);
                let m = self.ctx.cluster_info.insert(member);
                (m.is_none(), Some((vec![], String::new(), is_learner)))
            }
            ConfChangeType::Remove => {
                _ = cst_l.config.remove(node_id);
                self.lst.remove(node_id);
                _ = self.ctx.sync_events.remove(&node_id);
                _ = self.ctx.connects.remove(&node_id);
                let _ig = self.ctx.curp_storage.remove_member(node_id);
                // The member may not exist because the node could be restarted
                // and has fetched the newest cluster info
                //
                // TODO: Review all the usages of `ctx.cluster_info` to ensure all
                // the assertions are correct.
                let member_opt = self.ctx.cluster_info.remove(&node_id);
                (
                    true,
                    member_opt.map(|m| (m.peer_urls, m.name, m.is_learner)),
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
                let _ig = self.ctx.curp_storage.put_member(&*m);
                (
                    old_addrs != conf_change.address,
                    Some((old_addrs, String::new(), false)),
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
                let _ig = self.ctx.curp_storage.put_member(&*m);
                (modified, Some((vec![], String::new(), false)))
            }
        };
        if modified {
            self.ctx.cluster_info.cluster_version_update();
        }
        self.ctx
            .change_tx
            .send(conf_change)
            .unwrap_or_else(|_e| unreachable!("change_rx should not be dropped"));
        // TODO: We could wrap lst inside a role checking to prevent accidental lst mutation
        if self.is_leader()
            && self
                .lst
                .get_transferee()
                .is_some_and(|transferee| !cst_l.config.voters().contains(&transferee))
        {
            self.lst.reset_transferee();
        }
        fallback_info
    }

    /// Notify sync events
    fn notify_sync_events(&self, log: &Log<C>) {
        self.ctx.sync_events.iter().for_each(|e| {
            if let Some(next) = self.lst.get_next_index(*e.key()) {
                if next > log.base_index && log.has_next_batch(next) {
                    let _ignore = e.notify(1);
                }
            }
        });
    }

    /// Update index in single node cluster
    fn update_index_single_node(&self, log: &mut Log<C>, index: u64, term: u64) {
        // check if commit_index needs to be updated
        if self.can_update_commit_index_to(log, index, term) && index > log.commit_index {
            log.commit_to(index);
            debug!("{} updates commit index to {index}", self.id());
            self.apply(&mut *log);
        }
    }

    /// Entry process shared by `handle_xxx`
    #[allow(clippy::pattern_type_mismatch)] // Can't be fixed
    fn entry_process_multi(&self, log: &mut Log<C>, entries: &[(u64, bool)], term: u64) {
        if let Some(last_no_conflict) = entries
            .iter()
            .rev()
            .find(|(_, conflict)| *conflict)
            .map(|(index, _)| *index)
        {
            log.last_exe = last_no_conflict;
        }
        let highest_index = entries
            .last()
            .unwrap_or_else(|| unreachable!("no log in entries"))
            .0;
        self.notify_sync_events(log);
        self.update_index_single_node(log, highest_index, term);
    }

    /// Entry process shared by `handle_xxx`
    fn entry_process_single(
        &self,
        log_w: &mut RwLockWriteGuard<'_, Log<C>>,
        entry: &LogEntry<C>,
        conflict: bool,
        term: u64,
    ) {
        let index = entry.index;
        if !conflict {
            log_w.last_exe = index;
        }
        self.notify_sync_events(log_w);
        self.update_index_single_node(log_w, index, term);
    }

    /// Process deduplication and acknowledge the `first_incomplete` for this client id
    pub(crate) fn deduplicate(
        &self,
        ProposeId(client_id, seq_num): ProposeId,
        first_incomplete: Option<u64>,
    ) -> Result<(), CurpError> {
        // deduplication
        if self.ctx.lm.read().check_alive(client_id) {
            let mut cb_w = self.ctx.cb.write();
            let tracker = cb_w.tracker(client_id);
            if tracker.only_record(seq_num) {
                // TODO: obtain the previous ER from cmd_board and packed into CurpError::Duplicated as an entry.
                return Err(CurpError::duplicated());
            }
            if let Some(first_incomplete) = first_incomplete {
                let before = tracker.first_incomplete();
                if tracker.must_advance_to(first_incomplete) {
                    for seq_num_ack in before..first_incomplete {
                        Self::ack(ProposeId(client_id, seq_num_ack), &mut cb_w);
                    }
                }
            }
        } else {
            self.ctx.cb.write().client_expired(client_id);
            return Err(CurpError::expired_client_id());
        }
        Ok(())
    }

    /// Acknowledge the propose id and GC it's cmd board result
    fn ack(id: ProposeId, cb: &mut CommandBoard<C>) {
        let _ignore_er = cb.er_buffer.swap_remove(&id);
        let _ignore_asr = cb.asr_buffer.swap_remove(&id);
        let _ignore_conf = cb.conf_buffer.swap_remove(&id);
    }
}
