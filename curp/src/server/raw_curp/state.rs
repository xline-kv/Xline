use std::collections::{HashMap, HashSet};

use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use madsim::rand::{thread_rng, Rng};
use tracing::debug;

use super::Role;
use crate::{members::ServerId, server::PoolEntry, LogIndex};

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
    pub(super) config: MajorityConfig,
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
}

impl Default for FollowerStatus {
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
    statuses: DashMap<ServerId, FollowerStatus>,
}

impl State {
    /// Create a new `State`
    pub(super) fn new(
        term: u64,
        voted_for: Option<ServerId>,
        role: Role,
        leader_id: Option<ServerId>,
        follower_timeout_ticks: u8,
        candidate_timeout_ticks: u8,
    ) -> Self {
        let mut st = Self {
            term,
            voted_for,
            role,
            leader_id,
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
    pub(super) fn insert(&self, id: ServerId) {
        _ = self.statuses.insert(id, FollowerStatus::default());
    }

    /// Remove a status
    pub(super) fn remove(&self, id: ServerId) {
        _ = self.statuses.remove(&id);
    }

    /// Get status for a server
    fn get_status(&self, id: ServerId) -> Ref<'_, u64, FollowerStatus> {
        self.statuses
            .get(&id)
            .unwrap_or_else(|| unreachable!("no status for {id}"))
    }

    /// Get status for a server
    fn get_status_mut(&self, id: ServerId) -> RefMut<'_, u64, FollowerStatus> {
        self.statuses
            .get_mut(&id)
            .unwrap_or_else(|| unreachable!("no status for {id}"))
    }

    /// Check all followers by `f`
    pub(super) fn check_all(&self, f: impl Fn(&FollowerStatus) -> bool) -> bool {
        self.statuses.iter().all(|s| f(s.value()))
    }

    /// Get `next_index` for server
    pub(super) fn get_next_index(&self, id: ServerId) -> LogIndex {
        self.get_status(id).next_index
    }

    /// Get `match_index` for server
    pub(super) fn get_match_index(&self, id: ServerId) -> LogIndex {
        self.get_status(id).match_index
    }

    /// Update `next_index` for server
    pub(super) fn update_next_index(&self, id: ServerId, index: LogIndex) {
        self.get_status_mut(id).next_index = index;
    }

    /// Update `match_index` for server, will update `next_index` if possible
    pub(super) fn update_match_index(&self, id: ServerId, index: LogIndex) {
        let mut status = self.get_status_mut(id);
        if status.match_index >= index {
            return;
        }
        status.match_index = index;
        status.next_index = index + 1;
        debug!("follower {id}'s match_index updated to {index}");
    }
}

impl<C> CandidateState<C> {
    /// Create a new `CandidateState`
    pub(super) fn new(voters: impl Iterator<Item = ServerId>) -> Self {
        Self {
            sps: HashMap::new(),
            config: MajorityConfig::new(voters),
            votes_received: HashMap::new(),
        }
    }

    /// Check if the candidate has won the election
    pub(super) fn check_vote(&self) -> VoteResult {
        self.config.check_vote(&self.votes_received)
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

impl MajorityConfig {
    /// Create a new `MajorityConfig`
    fn new(voters: impl Iterator<Item = ServerId>) -> Self {
        Self {
            voters: voters.collect(),
        }
    }

    /// Get voters of current config
    pub(super) fn voters(&self) -> &HashSet<ServerId> {
        &self.voters
    }

    /// Get mutable voters of current config
    pub(super) fn voters_mut(&mut self) -> &mut HashSet<ServerId> {
        &mut self.voters
    }

    /// Get quorum: the smallest number of servers who must be online for the cluster to work
    fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
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

        let quorum = self.quorum();
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
    fn quorum_should_work() {
        let cst = MajorityConfig::new(0..3);
        assert_eq!(cst.quorum(), 2);

        let cst = MajorityConfig::new(0..4);
        assert_eq!(cst.quorum(), 3);

        let cst = MajorityConfig::new(0..5);
        assert_eq!(cst.quorum(), 3);

        let cst = MajorityConfig::new(0..9);
        assert_eq!(cst.quorum(), 5);

        let cst = MajorityConfig::new(0..10);
        assert_eq!(cst.quorum(), 6);
    }

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
