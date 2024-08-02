use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

use crate::quorum::Joint;

/// Membership state stored in current node
#[derive(Debug, Default)]
pub(crate) struct MembershipState {
    /// Config that exist in log, but haven't committed
    effective: Membership,
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct Membership {
    /// Member of the cluster
    pub(crate) members: Vec<HashSet<u64>>,
    /// All Nodes, including members and learners
    pub(crate) nodes: HashMap<u64, String>,
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

    /// Converts to `Joint`
    pub(crate) fn as_joint(&self) -> Joint<HashSet<u64>, &[HashSet<u64>]> {
        Joint::new(self.members.as_slice())
    }

    /// Gets the addresses of all members
    pub(crate) fn members(&self) -> impl Iterator<Item = (u64, &String)> {
        self.nodes.iter().filter_map(|(id, addr)| {
            self.members
                .iter()
                .any(|m| m.contains(id))
                .then_some((*id, addr))
        })
    }
}

/// The change of membership
pub(crate) enum Change {
    /// Adds learners
    AddLearner(Vec<(u64, String)>),
    /// Removes learners
    RemoveLearner(Vec<u64>),
}
