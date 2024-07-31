use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::Hash;

use curp_external_api::LogIndex;
use serde::Deserialize;
use serde::Serialize;

use crate::quorum::Joint;

/// Membership state stored in current node
#[derive(Debug, Default)]
pub(crate) struct MembershipState {
    /// Config that exist in log, but haven't committed
    effective: Membership,
    /// Index of the effective membership
    index_effective: LogIndex,
    /// Committed membership config
    committed: Membership,
}

#[allow(unused)]
impl MembershipState {
    /// Update the effective membership
    pub(crate) fn update_effective(&mut self, config: Membership) {
        self.effective = config;
    }

    /// Update the committed membership
    pub(crate) fn update_commit(&mut self, config: Membership) {
        self.committed = config;
    }

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
}

/// Membership config
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub(crate) struct Membership {
    /// Member of the cluster
    pub(crate) members: Vec<BTreeSet<u64>>,
    /// All Nodes, including members and learners
    pub(crate) nodes: BTreeMap<u64, String>,
}

impl Membership {
    /// Generates a new membership from `Change`
    ///
    /// Returns `None` if the change is invalid
    pub(crate) fn change(&self, change: Change) -> Option<Self> {
        match change {
            Change::AddLearner(learners) => {
                let members = self.members.clone();
                let mut nodes = self.nodes.clone();
                for (id, addr) in learners {
                    match nodes.entry(id) {
                        Entry::Occupied(_) => return None,
                        Entry::Vacant(e) => {
                            let _ignore = e.insert(addr);
                        }
                    }
                }

                Some(Self { members, nodes })
            }
            Change::RemoveLearner(ids) => {
                let members = self.members.clone();
                let mut nodes = self.nodes.clone();
                for id in ids {
                    let _ignore = nodes.remove(&id)?;
                }

                Some(Self { members, nodes })
            }
        }
    }

    #[allow(unused)]
    /// Converts to `Joint`
    pub(crate) fn as_joint(&self) -> Joint<BTreeSet<u64>, &[BTreeSet<u64>]> {
        Joint::new(self.members.as_slice())
    }
}

/// The change of membership
pub(crate) enum Change {
    /// Adds learners
    AddLearner(Vec<(u64, String)>),
    /// Removes learners
    RemoveLearner(Vec<u64>),
}
