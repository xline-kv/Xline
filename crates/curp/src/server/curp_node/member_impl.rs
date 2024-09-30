#![allow(
    clippy::unused_self,
    clippy::unimplemented,
    clippy::needless_pass_by_value
)] // TODO: remove this after implemented

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use curp_external_api::cmd::Command;
use curp_external_api::cmd::CommandExecutor;
use curp_external_api::role_change::RoleChange;
use curp_external_api::LogIndex;
use utils::task_manager::tasks::TaskName;

use super::CurpNode;
use crate::log_entry::EntryData;
use crate::log_entry::LogEntry;
use crate::member::Membership;
use crate::rpc::connect::InnerConnectApiWrapper;
use crate::rpc::inner_connects;
use crate::rpc::Change;
use crate::rpc::ChangeMembershipRequest;
use crate::rpc::ChangeMembershipResponse;
use crate::rpc::CurpError;
use crate::rpc::MembershipChange;
use crate::rpc::ProposeId;
use crate::rpc::Redirect;
use crate::server::raw_curp::node_state::NodeState;

// Leader methods
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Performs a membership change to the cluster
    pub(crate) async fn change_membership(
        &self,
        request: ChangeMembershipRequest,
    ) -> Result<ChangeMembershipResponse, CurpError> {
        self.ensure_leader()?;
        let changes = request
            .changes
            .into_iter()
            .map(MembershipChange::into_inner);
        let changes = Self::ensure_non_overlapping(changes)?;
        let configs = self.curp.generate_membership(changes);
        if configs.is_empty() {
            return Err(CurpError::invalid_member_change());
        }
        for config in configs {
            let propose_id = ProposeId(rand::random(), 0);
            let index = self.curp.push_log_entry(propose_id, config.clone()).index;
            self.update_states_with_membership(&config);
            self.curp
                .update_membership_state(None, Some((index, config)), None);
            self.curp.persistent_membership_state()?;
            // Leader also needs to update transferee
            self.curp.update_transferee();
            self.wait_commit(Some(propose_id)).await;
        }

        Ok(ChangeMembershipResponse {})
    }

    /// Wait the command with the propose id to be committed
    async fn wait_commit<Ids: IntoIterator<Item = ProposeId>>(&self, propose_ids: Ids) {
        self.curp.wait_propose_ids(propose_ids).await;
    }

    /// Ensures there are no overlapping ids
    fn ensure_non_overlapping<Changes>(changes: Changes) -> Result<Vec<Change>, CurpError>
    where
        Changes: IntoIterator<Item = Change>,
    {
        let changes: Vec<_> = changes.into_iter().collect();
        let mut ids = changes.iter().map(|c| match *c {
            Change::Add(ref node) => node.node_id,
            Change::Remove(id) | Change::Promote(id) | Change::Demote(id) => id,
        });

        let mut set = HashSet::new();
        if ids.all(|id| set.insert(id)) {
            return Ok(changes);
        }

        Err(CurpError::InvalidConfig(()))
    }

    /// Ensures that the current node is the leader
    fn ensure_leader(&self) -> Result<(), CurpError> {
        let (leader_id, term, is_leader) = self.curp.leader();
        if is_leader {
            return Ok(());
        }
        Err(CurpError::Redirect(Redirect {
            leader_id: leader_id.map(Into::into),
            term,
        }))
    }
}

// Common methods shared by both leader and followers
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Updates the membership config
    pub(crate) fn update_states_with_membership(&self, membership: &Membership) {
        let connects = self.connect_nodes(membership);
        let new_states = self.curp.update_node_states(connects);
        self.spawn_sync_follower_tasks(new_states.into_values());
        self.curp.update_role();
    }

    /// Filter out membership log entries
    pub(crate) fn filter_membership_entries<E, I>(
        entries: I,
    ) -> impl Iterator<Item = (LogIndex, Membership)>
    where
        E: AsRef<LogEntry<C>>,
        I: IntoIterator<Item = E>,
    {
        entries.into_iter().filter_map(|entry| {
            let entry = entry.as_ref();
            if let EntryData::Member(ref m) = entry.entry_data {
                Some((entry.index, m.clone()))
            } else {
                None
            }
        })
    }

    /// Connect to nodes of the given membership config
    pub(crate) fn connect_nodes(
        &self,
        config: &Membership,
    ) -> BTreeMap<u64, InnerConnectApiWrapper> {
        let nodes = config
            .nodes
            .iter()
            .map(|(id, meta)| (*id, meta.peer_urls().to_vec()))
            .collect();

        inner_connects(nodes, self.curp.client_tls_config()).collect()
    }

    /// Spawns background follower sync tasks
    pub(super) fn spawn_sync_follower_tasks(&self, new_nodes: impl IntoIterator<Item = NodeState>) {
        let task_manager = self.curp.task_manager();
        for (connect, sync_event, remove_event) in new_nodes.into_iter().map(NodeState::into_parts)
        {
            task_manager.spawn(TaskName::SyncFollower, |n| {
                Self::sync_follower_task(
                    Arc::clone(&self.curp),
                    connect,
                    sync_event,
                    remove_event,
                    n,
                )
            });
        }
    }
}
