use std::collections::BTreeMap;

use curp_external_api::cmd::Command;
use curp_external_api::role_change::RoleChange;
use curp_external_api::LogIndex;
use utils::parking_lot_lock::RwLockMap;

use crate::member::Membership;
use crate::rpc::connect::InnerConnectApiWrapper;
use crate::rpc::Change;
use crate::server::StorageApi;
use crate::server::StorageError;

use super::node_state::NodeState;
use super::RawCurp;
use super::Role;

// Lock order:
// - log
// - ms
// - node_states

// Leader methods
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Generate memberships based on the provided change
    pub(crate) fn generate_membership<Changes>(&self, changes: Changes) -> Vec<Membership>
    where
        Changes: IntoIterator<Item = Change>,
    {
        self.ms.read().cluster().changes(changes)
    }

    /// Updates the role if the node is leader
    pub(crate) fn update_transferee(&self) {
        let Some(transferee) = self.lst.get_transferee() else {
            return;
        };
        if !self.ms.map_read(|ms| ms.is_member(transferee)) {
            self.lst.reset_transferee();
        }
    }
}

// Common methods shared by both leader and followers
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Updates the membership state
    ///
    /// # Arguments
    ///
    /// * `truncate` - An optional `LogIndex` up to which the membership log should be truncated.
    /// * `append` - An iterator of tuples `(LogIndex, Membership)` to be appended to the membership log.
    /// * `commit` - An optional `LogIndex` up to which the membership log should be committed.
    pub(crate) fn update_membership_state<Entries>(
        &self,
        truncate: Option<LogIndex>,
        append: Entries,
        commit: Option<LogIndex>,
    ) where
        Entries: IntoIterator<Item = (LogIndex, Membership)>,
    {
        let mut ms_w = self.ms.write();
        if let Some(index) = truncate {
            ms_w.cluster_mut().truncate(index);
        }
        for (index, config) in append {
            ms_w.cluster_mut().append(index, config);
        }
        if let Some(index) = commit {
            ms_w.cluster_mut().commit(index);
        }
    }

    /// Persists the current membership state to storage.
    ///
    /// This method should only be called when new entries are appended to the membership state.
    pub(crate) fn persistent_membership_state(&self) -> Result<(), StorageError> {
        let (node_id, membership_state) =
            self.ms.map_read(|ms| (ms.node_id(), ms.cluster().clone()));
        self.ctx
            .curp_storage
            .put_membership(node_id, &membership_state)
    }

    /// Updates the node states
    pub(crate) fn update_node_states(
        &self,
        connects: BTreeMap<u64, InnerConnectApiWrapper>,
    ) -> BTreeMap<u64, NodeState> {
        self.ctx.node_states.update_with(connects)
    }

    /// Updates the role of the node based on the current membership state
    pub(crate) fn update_role(&self) {
        let ms = self.ms.read();
        let mut st_w = self.st.write();
        if ms.is_self_member() {
            if matches!(st_w.role, Role::Learner) {
                st_w.role = Role::Follower;
            }
        } else {
            st_w.role = Role::Learner;
        }

        // updates leader id
        if st_w.leader_id.map_or(false, |id| !ms.is_member(id)) {
            st_w.leader_id = None;
        }
    }

    /// Returns the current membership state
    #[cfg(test)]
    pub(crate) fn node_states(&self) -> BTreeMap<u64, NodeState> {
        self.ctx.node_states.all_states()
    }

    /// Return the current persisted membership
    #[cfg(test)]
    pub(crate) fn persisted_membership(&self) -> Option<(u64, crate::member::MembershipState)> {
        self.ctx.curp_storage.recover_membership().unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use curp_test_utils::mock_role_change;
    use utils::task_manager::TaskManager;

    use crate::rpc::NodeMetadata;

    use super::*;

    #[test]
    fn test_update_membership_state_ok() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));
        let membership1 = Membership::new(
            vec![(0..4).collect()],
            (0..4).map(|id| (id, NodeMetadata::default())).collect(),
        );
        let membership2 = Membership::new(
            vec![(0..5).collect()],
            (0..5).map(|id| (id, NodeMetadata::default())).collect(),
        );

        curp.update_membership_state(None, [(1, membership1.clone())], None);
        assert_eq!(*curp.ms.read().cluster().effective(), membership1);
        curp.update_membership_state(None, [(2, membership2.clone())], None);
        assert_eq!(*curp.ms.read().cluster().effective(), membership2);
        curp.update_membership_state(Some(1), [], None);
        assert_eq!(*curp.ms.read().cluster().effective(), membership1);

        curp.update_membership_state(None, [(2, membership2.clone())], None);
        curp.update_membership_state(None, [], Some(2));
        assert_eq!(*curp.ms.read().cluster().effective(), membership2);
        assert_eq!(*curp.ms.read().cluster().committed(), membership2);
    }

    #[test]
    fn test_update_role_ok() {
        let curp = RawCurp::new_test(3, mock_role_change(), Arc::new(TaskManager::new()));
        assert_eq!(curp.st.read().role, Role::Leader);
        // self is 0
        let membership1 = Membership::new(
            vec![(1..3).collect()],
            (1..3).map(|id| (id, NodeMetadata::default())).collect(),
        );
        let membership2 = Membership::new(
            vec![(0..3).collect()],
            (0..3).map(|id| (id, NodeMetadata::default())).collect(),
        );

        // remove from membership
        curp.update_membership_state(None, [(1, membership1.clone())], None);
        curp.update_role();
        assert_eq!(curp.st.read().role, Role::Learner);

        // add back
        curp.update_membership_state(None, [(2, membership2.clone())], None);
        curp.update_role();
        assert_eq!(curp.st.read().role, Role::Follower);
    }
}
