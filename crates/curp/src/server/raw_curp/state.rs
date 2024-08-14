use std::{
    collections::HashMap,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use dashmap::{
    mapref::{
        multiple::RefMulti,
        one::{Ref, RefMut},
    },
    DashMap,
};
use event_listener::Event;
use futures::{future, Future};
use madsim::rand::{thread_rng, Rng};
use tracing::{debug, warn};

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

/// Status of a follower
#[derive(Debug, Copy, Clone)]
pub(super) struct FollowerStatus {
    /// Index of the next log entry to send to that follower
    pub(super) next_index: LogIndex,
    /// Index of highest log entry known to be replicated on that follower
    pub(super) match_index: LogIndex,
    /// This node is a learner or not
    pub(super) is_learner: bool,
}

impl Default for FollowerStatus {
    fn default() -> Self {
        Self {
            next_index: 1,
            match_index: 0,
            is_learner: false,
        }
    }
}

impl FollowerStatus {
    /// Create a new `FollowerStatus`
    fn new(next_index: LogIndex, match_index: LogIndex, is_learner: bool) -> Self {
        Self {
            next_index,
            match_index,
            is_learner,
        }
    }
}

/// Additional state for the leader, all volatile
#[derive(Debug)]
pub(super) struct LeaderState {
    /// For each server, the leader maintains its status
    statuses: DashMap<ServerId, FollowerStatus>,
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
    pub(super) fn new(others: &[ServerId]) -> Self {
        Self {
            statuses: others
                .iter()
                .map(|o| (*o, FollowerStatus::default()))
                .collect(),
            leader_transferee: AtomicU64::new(0),
            no_op_state: NoOpState::default(),
        }
    }

    /// Get statuses for all servers
    pub(super) fn get_all_statuses(&self) -> HashMap<ServerId, FollowerStatus> {
        self.statuses
            .iter()
            .map(|e| (*e.key(), *e.value()))
            .collect()
    }

    /// insert new status for id
    pub(super) fn insert(&self, id: ServerId, is_learner: bool) {
        _ = self
            .statuses
            .insert(id, FollowerStatus::new(1, 0, is_learner));
    }

    /// Remove a status
    pub(super) fn remove(&self, id: ServerId) {
        _ = self.statuses.remove(&id);
    }

    /// Get status for a server
    fn get_status(&self, id: ServerId) -> Option<Ref<'_, u64, FollowerStatus>> {
        self.statuses.get(&id)
    }

    /// Get status for a server
    fn get_status_mut(&self, id: ServerId) -> Option<RefMut<'_, u64, FollowerStatus>> {
        self.statuses.get_mut(&id)
    }

    /// Get `next_index` for server
    pub(super) fn get_next_index(&self, id: ServerId) -> Option<LogIndex> {
        self.get_status(id).map(|s| s.next_index)
    }

    /// Get `match_index` for server
    pub(super) fn get_match_index(&self, id: ServerId) -> Option<LogIndex> {
        self.get_status(id).map(|s| s.match_index)
    }

    /// Update `next_index` for server
    pub(super) fn update_next_index(&self, id: ServerId, index: LogIndex) {
        let Some(mut status) = self.get_status_mut(id) else {
            warn!("follower {} is not found, it maybe has been removed", id);
            return;
        };
        status.next_index = index;
    }

    /// Update `match_index` for server, will update `next_index` if possible
    pub(super) fn update_match_index(&self, id: ServerId, index: LogIndex) {
        let Some(mut status) = self.get_status_mut(id) else {
            warn!("follower {} is not found, it maybe has been removed", id);
            return;
        };
        if status.match_index >= index {
            return;
        }
        status.match_index = index;
        status.next_index = index + 1;
        debug!("follower {id}'s match_index updated to {index}");
    }

    /// Create a `Iterator` for all statuses
    pub(super) fn iter(&self) -> impl Iterator<Item = RefMulti<'_, ServerId, FollowerStatus>> {
        self.statuses.iter()
    }

    /// Promote a learner to voter
    pub(super) fn promote(&self, node_id: ServerId) {
        if let Some(mut s) = self.statuses.get_mut(&node_id) {
            s.is_learner = false;
        }
    }

    /// Demote a voter to learner
    pub(super) fn demote(&self, node_id: ServerId) {
        if let Some(mut s) = self.statuses.get_mut(&node_id) {
            s.is_learner = true;
        }
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

    /// Check if the candidate has won the election
    pub(super) fn check_vote(&self) -> VoteResult {
        unimplemented!()
    }
}

/// Trait for cluster configuration
trait ClusterConfig {
    /// Check if the candidate has won the election
    fn check_vote(&self, votes_received: &HashMap<ServerId, bool>) -> VoteResult;
}

/// Result of a vote
#[derive(Debug, PartialEq)]
pub(super) enum VoteResult {
    /// Won the election
    Won,
    /// Pending
    Pending,
    /// Lost the election
    Lost,
}

#[cfg(test)]
mod test {

    #[test]
    fn check_vote_should_return_right_vote_result() {
        unimplemented!()
    }
}
