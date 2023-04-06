use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use madsim::rand::{thread_rng, Rng};
use parking_lot::{Mutex, MutexGuard};
use tracing::debug;

use super::Role;
use crate::{LogIndex, ServerId};

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
    pub(super) sps: HashMap<ServerId, Vec<Arc<C>>>,
    /// Votes received in the election
    pub(super) votes_received: u64,
}

/// Status of a follower
#[derive(Debug)]
struct FollowerStatus {
    /// Index of the next log entry to send to that follower
    next_index: LogIndex,
    /// Index of highest log entry known to be replicated on that follower
    match_index: LogIndex,
}

/// Additional state for the leader, all volatile
#[derive(Debug)]
pub(super) struct LeaderState {
    /// For each server, the leader maintains its status
    statuses: HashMap<ServerId, Mutex<FollowerStatus>>,
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
    pub(super) fn new(others: &HashSet<ServerId>) -> Self {
        Self {
            statuses: others
                .iter()
                .cloned()
                .map(|o| {
                    (
                        o,
                        Mutex::new(FollowerStatus {
                            next_index: 1,
                            match_index: 0,
                        }),
                    )
                })
                .collect(),
        }
    }

    /// Get status for a server
    fn get_status(&self, id: &ServerId) -> MutexGuard<'_, FollowerStatus> {
        self.statuses
            .get(id)
            .unwrap_or_else(|| unreachable!("no status for {id}"))
            .lock()
    }

    /// Get `next_index` for server
    pub(super) fn get_next_index(&self, id: &ServerId) -> LogIndex {
        self.get_status(id).next_index
    }

    /// Get `match_index` for server
    pub(super) fn get_match_index(&self, id: &ServerId) -> LogIndex {
        self.get_status(id).match_index
    }

    /// Update `next_index` for server
    pub(super) fn update_next_index(&self, id: &ServerId, index: LogIndex) {
        self.get_status(id).next_index = index;
    }

    /// Update `match_index` for server, will update `next_index` if possible
    pub(super) fn update_match_index(&self, id: &ServerId, index: LogIndex) {
        let mut status = self.get_status(id);

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
    pub(super) fn new() -> Self {
        Self {
            sps: HashMap::new(),
            votes_received: 0,
        }
    }
}
