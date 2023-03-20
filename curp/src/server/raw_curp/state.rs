use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use madsim::rand::{thread_rng, Rng};
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

/// Additional state for the leader, all volatile
#[derive(Debug)]
pub(super) struct LeaderState {
    /// For each server, index of the next log entry to send to that server
    next_index: HashMap<ServerId, LogIndex>,
    /// For each server, index of highest log entry known to be replicated on server
    match_index: HashMap<ServerId, LogIndex>,
    /// Servers that are being calibrated by the leader
    pub(super) calibrating: HashSet<ServerId>,
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
    pub(super) fn new(
        next_index: HashMap<ServerId, LogIndex>,
        match_index: HashMap<ServerId, LogIndex>,
    ) -> Self {
        Self {
            next_index,
            match_index,
            calibrating: HashSet::new(),
        }
    }

    /// Get `next_index` for server
    pub(super) fn get_next_index(&self, id: &ServerId) -> LogIndex {
        *self
            .next_index
            .get(id)
            .unwrap_or_else(|| unreachable!("no next_index for {id}"))
    }

    /// Get `match_index` for server
    pub(super) fn get_match_index(&self, id: &ServerId) -> LogIndex {
        *self
            .match_index
            .get(id)
            .unwrap_or_else(|| unreachable!("no match_index for {id}"))
    }

    /// Update `next_index` for server
    pub(super) fn update_next_index(&mut self, id: &ServerId, index: LogIndex) {
        *self
            .next_index
            .get_mut(id)
            .unwrap_or_else(|| unreachable!("no next_index for {id}")) = index;
    }

    /// Update `match_index` for server, will update `next_index` if possible
    pub(super) fn update_match_index(&mut self, id: &ServerId, index: LogIndex) {
        let match_index = self
            .match_index
            .get_mut(id)
            .unwrap_or_else(|| unreachable!("no match_index for {id}"));
        if *match_index >= index {
            return;
        }

        *match_index = index;
        debug!("follower {}'s match_index updated to {match_index}", id);

        let next_index = self
            .next_index
            .get_mut(id)
            .unwrap_or_else(|| unreachable!("no next_index for {id}"));
        *next_index = *match_index + 1;
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
