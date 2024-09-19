use std::collections::btree_map::Entry;
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
        let init_ms = info.into_membership();
        let cluster_state = MembershipState {
            effective: init_ms.clone(),
            index_effective: 0,
            // The initial configuration considered as committed
            committed: init_ms,
        };
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
    pub(crate) fn is_member(&self) -> bool {
        self.cluster().effective().contains(self.node_id())
    }

    /// Returns `true` if the given node is a member of the cluster
    pub(crate) fn check_membership(&self, node_id: u64) -> bool {
        self.cluster().effective().contains(node_id)
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
    /// Config that exist in log, but haven't committed
    effective: Membership,
    /// Index of the effective membership
    index_effective: LogIndex,
    /// Committed membership config
    committed: Membership,
}

#[allow(unused)]
impl MembershipState {
    /// Append a membership change entry
    pub(crate) fn append(&mut self, index: LogIndex, membership: Membership) {
        self.index_effective = index;
        self.effective = membership;
    }

    /// Commit a membership index
    pub(crate) fn commit(&mut self, at: LogIndex) {
        if at >= self.index_effective {
            self.committed = self.effective.clone();
        }
    }

    /// Truncate at the give log index
    pub(crate) fn truncate(&mut self, at: LogIndex) {
        if at < self.index_effective {
            self.effective = self.committed.clone();
            self.index_effective = at;
        }
    }

    /// Returns the committed membership
    pub(crate) fn committed(&self) -> &Membership {
        &self.committed
    }

    /// Returns the effective membership
    pub(crate) fn effective(&self) -> &Membership {
        &self.effective
    }

    /// Returns the Some(membership) if there is NO membership change in flight
    pub(crate) fn in_flight(&self) -> Option<&Membership> {
        (self.effective != self.committed).then_some(&self.committed)
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
    #[cfg(test)]
    /// Creates a new `Membership`
    pub(crate) fn new(members: Vec<BTreeSet<u64>>, nodes: BTreeMap<u64, NodeMetadata>) -> Self {
        Self { members, nodes }
    }

    /// Generates a new membership from `Change`
    ///
    /// Returns `None` if the change is invalid
    pub(crate) fn change(&self, change: Change) -> Vec<Self> {
        match change {
            Change::AddLearner(learners) => {
                let members = self.members.clone();
                let mut nodes = self.nodes.clone();
                for (id, meta) in learners {
                    match nodes.entry(id) {
                        Entry::Occupied(_) => return vec![],
                        Entry::Vacant(e) => {
                            let _ignore = e.insert(meta);
                        }
                    }
                }

                vec![Self { members, nodes }]
            }
            Change::RemoveLearner(ids) => {
                let members = self.members.clone();
                let mut nodes = self.nodes.clone();
                for id in ids {
                    if nodes.remove(&id).is_none() {
                        return vec![];
                    }
                }

                vec![Self { members, nodes }]
            }
            Change::AddMember(ids) => self.update_members(ids, |i, set| {
                set.union(&i.into_iter().collect()).copied().collect()
            }),
            Change::RemoveMember(ids) => self.update_members(ids, |i, set| {
                set.difference(&i.into_iter().collect()).copied().collect()
            }),
        }
    }

    /// Updates the membership based on the given operation and returns
    /// a vector of coherent memberships.
    fn update_members<F>(&self, ids: Vec<u64>, op: F) -> Vec<Self>
    where
        F: FnOnce(Vec<u64>, BTreeSet<u64>) -> BTreeSet<u64>,
    {
        if !self.exists(&ids) {
            return vec![];
        }
        let last = self.last_set();
        let target = op(ids, last);
        self.all_coherent(&target)
    }

    /// Generates all coherent membership to reach the target
    fn all_coherent(&self, target: &BTreeSet<u64>) -> Vec<Self> {
        iter::successors(Some(self.clone()), |current| {
            let next = Self::next_coherent(current, target.clone());
            (current != &next).then_some(next)
        })
        .skip(1)
        .collect()
    }

    /// Generates a new coherent membership from a quorum set
    fn next_coherent(ms: &Self, set: BTreeSet<u64>) -> Self {
        let next = ms.as_joint_owned().coherent(set).into_inner();
        Self {
            members: next,
            nodes: ms.nodes.clone(),
        }
    }

    /// Returns the last member set
    ///
    fn last_set(&self) -> BTreeSet<u64> {
        self.members
            .last()
            .unwrap_or_else(|| unreachable!("there should be at least one member set"))
            .clone()
    }

    /// Validates the given ids for member operations
    fn exists(&self, ids: &[u64]) -> bool {
        // Ids should be in nodes
        ids.iter().all(|id| self.nodes.contains_key(id))
    }

    /// Converts to `Joint`
    pub(crate) fn as_joint(&self) -> Joint<BTreeSet<u64>, &[BTreeSet<u64>]> {
        Joint::new(self.members.as_slice())
    }

    /// Converts to `Joint`
    pub(crate) fn as_joint_owned(&self) -> Joint<BTreeSet<u64>, Vec<BTreeSet<u64>>> {
        Joint::new(self.members.clone())
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

    /// Returns `true` if the membership contains the given node id
    pub(crate) fn contains(&self, node_id: u64) -> bool {
        self.nodes.contains_key(&node_id)
    }
}

#[allow(unused)]
/// The change of membership
#[derive(Clone)]
pub(crate) enum Change {
    /// Adds learners
    AddLearner(Vec<(u64, NodeMetadata)>),
    /// Removes learners
    RemoveLearner(Vec<u64>),
    /// Adds members
    AddMember(Vec<u64>),
    /// Removes members
    RemoveMember(Vec<u64>),
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
