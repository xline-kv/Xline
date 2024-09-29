use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::iter;

use curp_external_api::LogIndex;
use serde::Deserialize;
use serde::Serialize;

use crate::quorum::Joint;
use crate::quorum::QuorumSet;
use crate::rpc::Change;
use crate::rpc::NodeMetadata;

/// The membership info, used to build the initial states
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MembershipInfo {
    /// The id of current node
    pub node_id: u64,
    /// The initial cluster members
    pub init_members: BTreeMap<u64, NodeMetadata>,
}

impl MembershipInfo {
    /// Creates a new `MembershipInfo`
    #[inline]
    #[must_use]
    pub fn new(node_id: u64, init_members: BTreeMap<u64, NodeMetadata>) -> Self {
        Self {
            node_id,
            init_members,
        }
    }

    /// Converts `MembershipInfo` into a `Membership`.
    pub(crate) fn into_membership(self) -> Membership {
        let MembershipInfo { init_members, .. } = self;

        Membership {
            nodes: init_members.clone(),
            members: vec![init_members.into_keys().collect()],
        }
    }
}

/// The membership state of the node
pub(crate) struct NodeMembershipState {
    /// The id of current node
    // WARN: This id should be diff from the old `ServerID`
    // TODO: use a distinct type for this
    node_id: u64,
    /// The membership state of the cluster
    cluster_state: MembershipState,
}

impl NodeMembershipState {
    /// Creates a new `NodeMembershipState` with initial state
    pub(crate) fn new(info: MembershipInfo) -> Self {
        let node_id = info.node_id;
        let cluster_state = MembershipState::new(info.into_membership());
        Self {
            node_id,
            cluster_state,
        }
    }

    /// Returns the id of the current node
    pub(crate) fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Returns a reference of the membership state
    pub(crate) fn cluster(&self) -> &MembershipState {
        &self.cluster_state
    }

    /// Returns a mutable reference of the membership state
    pub(crate) fn cluster_mut(&mut self) -> &mut MembershipState {
        &mut self.cluster_state
    }

    /// Returns `true` if the current node is a member of the cluster
    pub(crate) fn is_self_member(&self) -> bool {
        self.cluster().effective().contains_member(self.node_id())
    }

    /// Returns `true` if the given node is a member of the cluster
    pub(crate) fn is_member(&self, id: u64) -> bool {
        self.cluster().effective().contains_member(id)
    }

    /// Returns all member ids
    pub(crate) fn members_ids(&self) -> BTreeSet<u64> {
        self.cluster()
            .effective()
            .members()
            .map(|(id, _)| id)
            .collect()
    }

    /// Returns `true` if the given set of nodes forms a quorum
    pub(crate) fn check_quorum<I, Q>(&self, nodes: I, mut expect_quorum: Q) -> bool
    where
        I: IntoIterator<Item = u64> + Clone,
        Q: FnMut(&dyn QuorumSet<Vec<u64>>, Vec<u64>) -> bool,
    {
        let qs = self.cluster().effective().as_joint();
        expect_quorum(&qs, nodes.into_iter().collect())
    }
}

/// Membership state stored in current node
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MembershipState {
    /// Membership entries
    entries: Vec<MembershipEntry>,
    /// Commit log index
    commit_index: LogIndex,
}

#[allow(clippy::unwrap_used)] // `entries` should contains at least one entry
impl MembershipState {
    /// Creates a new `MembershipState`
    fn new(initial_membership: Membership) -> Self {
        let initial_entry = MembershipEntry::new(0, initial_membership);
        Self {
            entries: vec![initial_entry],
            commit_index: 0,
        }
    }

    /// Append a membership change entry
    pub(crate) fn append(&mut self, index: LogIndex, membership: Membership) {
        self.entries.push(MembershipEntry::new(index, membership));
    }

    /// Truncate at the give log index
    pub(crate) fn truncate(&mut self, at: LogIndex) {
        self.entries.retain(|entry| entry.index <= at);
    }

    /// Commit a membership index
    pub(crate) fn update_commit(&mut self, index: LogIndex) {
        self.commit_index = index;
        self.entries.retain(|entry| entry.index >= index);
    }

    /// Returns the committed membership
    #[cfg(test)]
    pub(crate) fn committed(&self) -> &Membership {
        &self
            .entries
            .iter()
            .take_while(|entry| entry.index <= self.commit_index)
            .last()
            .unwrap()
            .membership
    }

    /// Generates a new membership from `Change`
    ///
    /// Returns an empty `Vec` if there's an on-going membership change
    pub(crate) fn changes<Changes>(&self, changes: Changes) -> Vec<Membership>
    where
        Changes: IntoIterator<Item = Change>,
    {
        if self.has_uncommitted() {
            return vec![];
        }
        self.last().membership.changes(changes)
    }

    /// Returns the effective membership
    pub(crate) fn effective(&self) -> &Membership {
        &self.last().membership
    }

    /// Checks if there's an uncommitted membership change
    fn has_uncommitted(&self) -> bool {
        self.last().index > self.commit_index
    }

    /// Gets the last entry
    fn last(&self) -> &MembershipEntry {
        self.entries.last().unwrap()
    }
}

/// A membership log entry, including `Membership` and `LogIndex`
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq, Hash)]
struct MembershipEntry {
    /// The log index of the membership entry
    index: LogIndex,
    /// Membership
    membership: Membership,
}

impl MembershipEntry {
    /// Creates a new `MembershipEntry`
    fn new(index: LogIndex, membership: Membership) -> Self {
        Self { index, membership }
    }
}

/// Membership config
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub(crate) struct Membership {
    /// Member of the cluster
    pub(crate) members: Vec<BTreeSet<u64>>,
    /// All Nodes, including members and learners
    pub(crate) nodes: BTreeMap<u64, NodeMetadata>,
}

impl Membership {
    /// Creates a new `Membership`
    pub(crate) fn new(members: Vec<BTreeSet<u64>>, nodes: BTreeMap<u64, NodeMetadata>) -> Self {
        Self { members, nodes }
    }

    /// Generates a new membership from `Change`
    ///
    /// Returns `None` if the change is invalid
    pub(crate) fn changes<Changes>(&self, changes: Changes) -> Vec<Self>
    where
        Changes: IntoIterator<Item = Change>,
    {
        let mut nodes = self.nodes.clone();
        let mut target = self.current_member_set().clone();

        for change in changes {
            match change {
                Change::Add(node) => {
                    let (id, meta) = node.into_parts();
                    if nodes.insert(id, meta).is_some() {
                        return vec![];
                    }
                }
                Change::Remove(id) => {
                    if nodes.remove(&id).is_none() {
                        return vec![];
                    }
                }
                Change::Promote(id) => {
                    if self.is_current_member(id) {
                        return vec![];
                    }
                    let _ignore = target.insert(id);
                }
                Change::Demote(id) => {
                    if !self.is_current_member(id) {
                        return vec![];
                    }
                    let _ignore = target.remove(&id);
                }
            }
        }

        let all = Self::all_coherent(self.members.clone(), &target);

        all.into_iter()
            .map(|members| Self {
                members,
                nodes: nodes.clone(),
            })
            .collect()
    }

    /// Gets the current member set
    #[allow(clippy::unwrap_used)] // members should never be empty
    fn current_member_set(&self) -> &BTreeSet<u64> {
        self.members.last().unwrap()
    }

    /// Returns `true` if the given id exists in the current member set
    fn is_current_member(&self, id: u64) -> bool {
        self.current_member_set().contains(&id)
    }

    /// Generates all coherent membership to reach the target
    fn all_coherent(
        current: Vec<BTreeSet<u64>>,
        target: &BTreeSet<u64>,
    ) -> Vec<Vec<BTreeSet<u64>>> {
        iter::successors(Some(current), |curr| {
            let next = Joint::new(curr.clone())
                .coherent(target.clone())
                .into_inner();
            (curr != &next).then_some(next)
        })
        .skip(1)
        .collect()
    }

    /// Converts to `Joint`
    pub(crate) fn as_joint(&self) -> Joint<BTreeSet<u64>, &[BTreeSet<u64>]> {
        Joint::new(self.members.as_slice())
    }

    /// Gets the addresses of all members
    pub(crate) fn members(&self) -> impl Iterator<Item = (u64, &NodeMetadata)> {
        self.nodes.iter().filter_map(|(id, addr)| {
            self.members
                .iter()
                .any(|m| m.contains(id))
                .then_some((*id, addr))
        })
    }

    /// Returns `true` if the given node id is present in `members`.
    pub(crate) fn contains_member(&self, node_id: u64) -> bool {
        self.members.iter().any(|s| s.contains(&node_id))
    }
}

/// Trait for types that can provide a cluster ID.
pub trait ClusterId {
    /// Returns the cluster ID.
    fn cluster_id(&self) -> u64;
}

impl ClusterId for Membership {
    fn cluster_id(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl ClusterId for MembershipInfo {
    #[inline]
    fn cluster_id(&self) -> u64 {
        self.clone().into_membership().cluster_id()
    }
}
