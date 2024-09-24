use std::collections::BTreeMap;
use std::collections::BTreeSet;

use curp_external_api::cmd::Command;
use curp_external_api::role_change::RoleChange;
use curp_external_api::LogIndex;
use utils::parking_lot_lock::RwLockMap;

use crate::log_entry::EntryData;
use crate::log_entry::LogEntry;
use crate::member::Membership;
use crate::member::NodeMembershipState;
use crate::rpc::connect::InnerConnectApiWrapper;
use crate::rpc::inner_connects;
use crate::rpc::Change;
use crate::rpc::ProposeId;
use crate::server::StorageApi;
use crate::server::StorageError;

use super::node_state::NodeState;
use super::RawCurp;
use super::Role;

impl<C: Command, RC: RoleChange> RawCurp<C, RC> {
    /// Generate memberships based on the provided change
    pub(crate) fn generate_membership<Changes>(&self, changes: Changes) -> Vec<Membership>
    where
        Changes: IntoIterator<Item = Change>,
    {
        let ms_r = self.ms.read();
        ms_r.cluster().committed().changes(changes)
    }

    /// Updates the membership config
    pub(crate) fn update_membership(
        &self,
        config: Membership,
    ) -> Result<(BTreeMap<u64, NodeState>, ProposeId), StorageError> {
        // FIXME: define the lock order of log and ms
        let mut log_w = self.log.write();
        let mut ms_w = self.ms.write();
        let st_r = self.st.read();
        let propose_id = ProposeId(rand::random(), 0);
        let entry = log_w.push(st_r.term, propose_id, config.clone());
        let new_nodes = self.on_membership_update(&config);
        ms_w.cluster_mut().append(entry.index, config);
        self.ctx
            .curp_storage
            .put_membership(ms_w.node_id(), ms_w.cluster())?;

        Ok((new_nodes, propose_id))
    }

    /// Updates the role if the node is leader
    pub(crate) fn update_role_leader(&self) {
        let ms_r = self.ms.read();
        self.update_role(&ms_r);
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

    /// Append membership entries
    pub(crate) fn append_membership<E, I>(
        &self,
        entries: I,
        truncate_at: LogIndex,
        commit_index: LogIndex,
    ) -> Result<BTreeMap<u64, NodeState>, StorageError>
    where
        E: AsRef<LogEntry<C>>,
        I: IntoIterator<Item = E>,
    {
        let mut new_nodes = BTreeMap::new();
        let mut ms_w = self.ms.write();
        ms_w.cluster_mut().truncate(truncate_at);
        let configs = entries.into_iter().filter_map(|entry| {
            let entry = entry.as_ref();
            if let EntryData::Member(ref m) = entry.entry_data {
                Some((entry.index, m.clone()))
            } else {
                None
            }
        });
        for (index, config) in configs {
            new_nodes.append(&mut self.on_membership_update(&config));
            ms_w.cluster_mut().append(index, config);
            self.ctx
                .curp_storage
                .put_membership(ms_w.node_id(), ms_w.cluster())?;
        }
        ms_w.cluster_mut().commit(commit_index);

        self.update_role(&ms_w);

        Ok(new_nodes)
    }

    /// Updates the commit index
    pub(crate) fn membership_commit_to(&self, index: LogIndex) {
        let mut ms_w = self.ms.write();
        ms_w.cluster_mut().commit(index);
    }

    /// Updates the role of the node based on the current membership state
    fn update_role(&self, current: &NodeMembershipState) {
        let mut st_w = self.st.write();
        if current.is_self_member() {
            if matches!(st_w.role, Role::Learner) {
                st_w.role = Role::Follower;
            }
        } else {
            st_w.role = Role::Learner;
        }

        // updates leader id
        if st_w.leader_id.map_or(false, |id| !current.is_member(id)) {
            st_w.leader_id = None;
        }
    }

    /// Creates connections for new membership configuration.
    ///
    /// Returns a closure can be used to update the existing connections
    fn build_connects(&self, config: &Membership) -> BTreeMap<u64, InnerConnectApiWrapper> {
        let nodes = config
            .nodes
            .iter()
            .map(|(id, meta)| (*id, meta.peer_urls().to_vec()))
            .collect();

        inner_connects(nodes, self.client_tls_config()).collect()
    }

    /// Actions on membership update
    ///
    /// Returns the newly added nodes
    fn on_membership_update(&self, membership: &Membership) -> BTreeMap<u64, NodeState> {
        let node_ids: BTreeSet<_> = membership.nodes.keys().copied().collect();
        let new_connects = self.build_connects(membership);
        let connect_to = move |ids: &BTreeSet<u64>| {
            ids.iter()
                .filter_map(|id| new_connects.get(id).cloned())
                .collect::<Vec<_>>()
        };
        self.ctx.node_states.update_with(&node_ids, connect_to)
    }
}
