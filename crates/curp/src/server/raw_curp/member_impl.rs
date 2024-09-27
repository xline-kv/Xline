use std::collections::BTreeMap;
use std::sync::Arc;

use curp_external_api::cmd::Command;
use curp_external_api::role_change::RoleChange;
use curp_external_api::LogIndex;
use utils::parking_lot_lock::RwLockMap;

use crate::log_entry::EntryData;
use crate::log_entry::LogEntry;
use crate::member::Membership;
use crate::rpc::connect::InnerConnectApiWrapper;
use crate::rpc::Change;
use crate::rpc::ProposeId;
use crate::server::StorageApi;
use crate::server::StorageError;

use super::node_state::NodeState;
use super::RawCurp;
use super::Role;

// Lock order:
// - log
// - ms
// - node_states
impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Generate memberships based on the provided change
    pub(crate) fn generate_membership<Changes>(&self, changes: Changes) -> Vec<Membership>
    where
        Changes: IntoIterator<Item = Change>,
    {
        let ms_r = self.ms.read();
        ms_r.cluster().committed().changes(changes)
    }

    /// Push some logs into `Log`
    /// TODO: replace `push_logs`
    pub(crate) fn push_log_entries<Logs>(
        &self,
        entries: Logs,
    ) -> impl Iterator<Item = Arc<LogEntry<C>>>
    where
        Logs: IntoIterator<Item = (ProposeId, EntryData<C>)>,
    {
        let mut log_w = self.log.write();
        let st_r = self.st.read();
        entries
            .into_iter()
            .map(|(id, entry)| log_w.push(st_r.term, id, entry))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Append configs to membership state
    ///
    /// This method will also performs blocking IO
    pub(crate) fn append_to_membership_states<Entries>(
        &self,
        entries: Entries,
    ) -> Result<(), StorageError>
    where
        Entries: IntoIterator<Item = (LogIndex, Membership)>,
    {
        let mut ms_w = self.ms.write();
        for (index, config) in entries {
            ms_w.cluster_mut().append(index, config);
            self.ctx
                .curp_storage
                .put_membership(ms_w.node_id(), ms_w.cluster())?;
        }

        Ok(())
    }

    /// Updates the node states
    pub(crate) fn update_node_states(
        &self,
        connects: BTreeMap<u64, InnerConnectApiWrapper>,
    ) -> BTreeMap<u64, NodeState> {
        self.ctx.node_states.update_with(connects)
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

    /// Updates membership indices
    pub(crate) fn update_membership_indices(
        &self,
        truncate_at: Option<LogIndex>,
        commit: Option<LogIndex>,
    ) {
        let mut ms_w = self.ms.write();
        let _ignore = truncate_at.map(|index| ms_w.cluster_mut().truncate(index));
        let __ignore = commit.map(|index| ms_w.cluster_mut().update_commit(index));
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
}
