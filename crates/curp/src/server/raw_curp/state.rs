use std::{
    collections::HashMap,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use event_listener::Event;
use futures::{future, Future};
use madsim::rand::{thread_rng, Rng};
use parking_lot::RwLock;
use tracing::{debug, warn};
use utils::parking_lot_lock::RwLockMap;

use super::Role;
use crate::{members::ServerId, rpc::PoolEntry, LogIndex};

/// Curp state
#[derive(Debug)]
pub(super) struct State {
    /* persisted state */
    /// Current term
    pub(super) term: u64,
    /// Candidate id that received vote in current term
    pub(super) voted_for: Option<ServerId>,

    /* volatile state */
    /// Role of the server
    pub(super) role: Role,
    /// Cached id of the leader.
    pub(super) leader_id: Option<ServerId>,

    /// Randomized follower timeout ticks
    pub(super) follower_timeout_ticks: u8,
    /// Randomized candidate timeout ticks
    pub(super) candidate_timeout_ticks: u8,

    /// Base of follower timeout ticks
    follower_timeout_ticks_base: u8,
    /// Base of candidate timeout ticks
    candidate_timeout_ticks_base: u8,
}

/// Additional state for the candidate, all volatile
#[derive(Debug)]
pub(super) struct CandidateState<C> {
    /// Collected speculative pools, used for recovery
    pub(super) sps: HashMap<ServerId, Vec<PoolEntry<C>>>,
    /// Votes received in the election
    pub(super) votes_received: HashMap<ServerId, bool>,
}

/// Status of a Node
#[derive(Debug, Copy, Clone)]
pub(super) struct NodeStatus {
    /// Index of the next log entry to send to that node
    pub(super) next_index: LogIndex,
    /// Index of highest log entry known to be replicated on that node
    pub(super) match_index: LogIndex,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self {
            next_index: 1,
            match_index: 0,
        }
    }
}

/// Additional state for the leader, all volatile
#[derive(Debug)]
pub(super) struct LeaderState {
    /// For each server, the leader maintains its status
    statuses: RwLock<HashMap<ServerId, NodeStatus>>,
    /// Leader Transferee
    leader_transferee: AtomicU64,
    /// Event of the application of the no-op log, used for readIndex
    no_op_state: NoOpState,
}

/// The state of the no-op log entry application
#[derive(Debug, Default)]
struct NoOpState {
    /// The event that triggers after application
    event: Event,
    /// Whether the no-op entry has been applied
    applied: AtomicBool,
}

impl NoOpState {
    /// Sets the no-op entry as applied
    fn set_applied(&self) {
        self.applied.store(true, Ordering::Release);
        let _ignore = self.event.notify(usize::MAX);
    }

    /// Resets the no-op application state
    fn reset(&self) {
        self.applied.store(false, Ordering::Release);
    }

    /// Waits for the no-op log to be applied
    fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        if self.applied.load(Ordering::Acquire) {
            return Box::pin(future::ready(()));
        }
        Box::pin(self.event.listen())
    }
}

impl State {
    /// Create a new `State`
    pub(super) fn new(follower_timeout_ticks: u8, candidate_timeout_ticks: u8) -> Self {
        let mut st = Self {
            term: 0,
            voted_for: None,
            role: Role::Follower,
            leader_id: None,
            follower_timeout_ticks,
            candidate_timeout_ticks,
            follower_timeout_ticks_base: follower_timeout_ticks,
            candidate_timeout_ticks_base: candidate_timeout_ticks,
        };
        st.randomize_timeout_ticks();
        st
    }

    /// Randomize `follower_timeout_ticks` and `candidate_timeout_ticks` to reduce vote split possibility
    pub(super) fn randomize_timeout_ticks(&mut self) {
        let mut rng = thread_rng();
        self.follower_timeout_ticks =
            rng.gen_range(self.follower_timeout_ticks_base..(self.follower_timeout_ticks_base * 2));
        self.candidate_timeout_ticks = rng
            .gen_range(self.candidate_timeout_ticks_base..(self.candidate_timeout_ticks_base * 2));
    }
}

impl LeaderState {
    /// Create a `LeaderState`
    pub(super) fn new<I>(others: I) -> Self
    where
        I: IntoIterator<Item = u64>,
    {
        let statuses = others
            .into_iter()
            .map(|o| (o, NodeStatus::default()))
            .collect();

        Self {
            statuses: RwLock::new(statuses),
            leader_transferee: AtomicU64::new(0),
            no_op_state: NoOpState::default(),
        }
    }

    /// Get status for a server
    fn map_status_with_id<F, R>(&self, id: ServerId, f: F) -> Option<R>
    where
        F: FnMut(&NodeStatus) -> R,
    {
        self.statuses.map_read(|statuses| statuses.get(&id).map(f))
    }

    /// Get status for a server
    fn map_status_with_id_mut<F, R>(&self, id: ServerId, f: F) -> Option<R>
    where
        F: FnMut(&mut NodeStatus) -> R,
    {
        self.statuses
            .map_write(|mut statuses| statuses.get_mut(&id).map(f))
    }

    /// Get `next_index` for server
    pub(super) fn get_next_index(&self, id: ServerId) -> Option<LogIndex> {
        self.map_status_with_id(id, |s| s.next_index)
    }

    /// Get `match_index` for server
    pub(super) fn get_match_index(&self, id: ServerId) -> Option<LogIndex> {
        self.map_status_with_id(id, |s| s.match_index)
    }

    /// Update `next_index` for server
    pub(super) fn update_next_index(&self, id: ServerId, index: LogIndex) {
        let opt = self.map_status_with_id_mut(id, |status| status.next_index = index);
        if opt.is_none() {
            warn!("follower {} is not found, it maybe has been removed", id);
        }
    }

    /// Update `match_index` for server, will update `next_index` if possible
    pub(super) fn update_match_index(&self, id: ServerId, index: LogIndex) {
        let opt = self.map_status_with_id_mut(id, |status| {
            if status.match_index >= index {
                return;
            }
            status.match_index = index;
            status.next_index = index + 1;
            debug!("follower {id}'s match_index updated to {index}");
        });
        if opt.is_none() {
            warn!("follower {} is not found, it maybe has been removed", id);
        };
    }

    /// Create a `Iterator` for all statuses
    pub(super) fn map_status<F, R>(&self, f: F) -> impl Iterator<Item = R>
    where
        F: FnMut((&u64, &NodeStatus)) -> R,
    {
        self.statuses
            .map_read(|status| status.iter().map(f).collect::<Vec<_>>().into_iter())
    }

    /// Get transferee
    pub(super) fn get_transferee(&self) -> Option<ServerId> {
        let val = self.leader_transferee.load(Ordering::Acquire);
        (val != 0).then_some(val)
    }

    /// Reset transferee
    pub(super) fn reset_transferee(&self) {
        self.leader_transferee.store(0, Ordering::Release);
    }

    /// Swap transferee
    pub(super) fn swap_transferee(&self, node_id: ServerId) -> Option<ServerId> {
        let val = self.leader_transferee.swap(node_id, Ordering::SeqCst);
        (val != 0).then_some(val)
    }

    /// Sets the no-op log as applied
    pub(super) fn set_no_op_applied(&self) {
        self.no_op_state.set_applied();
    }

    /// Resets the no-op application state
    pub(super) fn reset_no_op_state(&self) {
        self.no_op_state.reset();
    }

    /// Waits for the no-op log to be applied
    pub(super) fn wait_no_op_applied(&self) -> impl Future<Output = ()> + Send {
        self.no_op_state.wait()
    }
}

impl<C> CandidateState<C> {
    /// Create a new `CandidateState`
    pub(super) fn new() -> Self {
        Self {
            sps: HashMap::new(),
            votes_received: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn check_vote_should_return_right_vote_result() {
        // unimplement
    }
}
