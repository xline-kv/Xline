use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

use dashmap::{
    mapref::{
        multiple::RefMulti,
        one::{Ref, RefMut},
    },
    DashMap,
};
use madsim::rand::{thread_rng, Rng};
use tracing::{debug, warn};

use super::Role;
use crate::{members::ServerId, quorum, rpc::PoolEntry, LogIndex};

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
    /// config in current cluster
    pub(super) config: Config,
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
}

impl<C> CandidateState<C> {
    /// Create a new `CandidateState`
    pub(super) fn new(voters: impl Iterator<Item = ServerId>) -> Self {
        Self {
            sps: HashMap::new(),
            config: Config::new(voters),
            votes_received: HashMap::new(),
        }
    }

    /// Check if the candidate has won the election
    pub(super) fn check_vote(&self) -> VoteResult {
        self.config.majority_config.check_vote(&self.votes_received)
    }
}

/// Trait for cluster configuration
trait ClusterConfig {
    /// Check if the candidate has won the election
    fn check_vote(&self, votes_received: &HashMap<ServerId, bool>) -> VoteResult;
}

/// `MajorityConfig` is a set of IDs that uses majority quorums to make decisions.
#[derive(Debug, Clone)]
pub(super) struct MajorityConfig {
    /// The voters in the cluster
    voters: HashSet<ServerId>,
}

/// Cluster config
#[derive(Debug, Clone)]
pub(super) struct Config {
    /// The majority config
    pub(super) majority_config: MajorityConfig,
    /// The learners in the cluster
    pub(super) learners: HashSet<ServerId>,
}

impl Config {
    /// Create a new `Config`
    pub(super) fn new(voters: impl Iterator<Item = ServerId>) -> Self {
        Self {
            majority_config: MajorityConfig::new(voters),
            learners: HashSet::new(),
        }
    }

    /// Get voters of current config
    pub(super) fn voters(&self) -> &HashSet<ServerId> {
        &self.majority_config.voters
    }

    /// Insert a voter
    pub(super) fn insert(&mut self, id: ServerId, is_learner: bool) -> bool {
        if is_learner {
            self.learners.insert(id)
        } else {
            self.majority_config.voters.insert(id)
        }
    }

    /// Remove a node
    pub(super) fn remove(&mut self, id: ServerId) -> bool {
        let res1 = self.majority_config.voters.remove(&id);
        let res2 = self.learners.remove(&id);
        debug_assert!(
            res1 ^ res2,
            "a node should not exist in both voters and learners"
        );
        res1 || res2
    }

    /// Check if a server exists
    pub(super) fn contains(&self, id: ServerId) -> bool {
        self.majority_config.voters.contains(&id) || self.learners.contains(&id)
    }
}

impl MajorityConfig {
    /// Create a new `MajorityConfig`
    fn new(voters: impl Iterator<Item = ServerId>) -> Self {
        Self {
            voters: voters.collect(),
        }
    }
}

impl ClusterConfig for MajorityConfig {
    fn check_vote(&self, votes_received: &HashMap<ServerId, bool>) -> VoteResult {
        if self.voters.is_empty() {
            return VoteResult::Won;
        }

        let mut voted_cnt = 0;
        let mut missing_cnt = 0;
        for id in &self.voters {
            match votes_received.get(id) {
                Some(&true) => voted_cnt += 1,
                None => missing_cnt += 1,
                _ => {}
            }
        }

        let quorum = quorum(self.voters.len());
        if voted_cnt >= quorum {
            return VoteResult::Won;
        }
        if voted_cnt + missing_cnt >= quorum {
            return VoteResult::Pending;
        }
        VoteResult::Lost
    }
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

    use curp_test_utils::test_cmd::TestCommand;

    use super::*;

    #[test]
    fn check_vote_should_return_right_vote_result() {
        let servers = vec![1, 2, 3, 4, 5];
        let mut cst = CandidateState::<TestCommand>::new(servers.into_iter());

        cst.votes_received =
            HashMap::from([(1, true), (2, true), (3, true), (4, false), (5, false)]);
        assert_eq!(cst.check_vote(), VoteResult::Won);

        cst.votes_received =
            HashMap::from([(1, true), (2, true), (3, false), (4, false), (5, false)]);
        assert_eq!(cst.check_vote(), VoteResult::Lost);

        cst.votes_received = HashMap::from([(1, true), (2, true), (3, false), (4, false)]);
        assert_eq!(cst.check_vote(), VoteResult::Pending);
    }
}
