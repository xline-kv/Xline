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
use utils::task_manager::tasks::TaskName;

use super::CurpNode;
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
            let propose_id = self.update_config(config)?;
            self.wait_commit(Some(propose_id)).await;
        }

        Ok(ChangeMembershipResponse {})
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

    /// Updates the membership config
    fn update_config(&self, config: Membership) -> Result<ProposeId, CurpError> {
        let propose_id = ProposeId(rand::random(), 0);
        let connects = self.connect_nodes(&config);
        let index = self.curp.push_membership_log(propose_id, config.clone());
        self.curp.update_membership_configs(Some((index, config)))?;
        let new_states = self.curp.update_node_states(connects);
        self.spawn_sync_follower_tasks(new_states.into_values());
        self.curp.update_role();
        self.curp.update_transferee();

        Ok(propose_id)
    }

    /// Wait the command with the propose id to be committed
    async fn wait_commit<Ids: IntoIterator<Item = ProposeId>>(&self, propose_ids: Ids) {
        self.curp.wait_propose_ids(propose_ids).await;
    }

    ///// Updates the membership based on the given change and waits for
    ///// the proposal to be committed
    //async fn update_and_wait(&self, config: Membership) -> Result<(), CurpError> {
    //    let entries = propose_ids.clone().into_iter().zip(configs.clone());
    //    let indices = self.curp.push_membership_logs(entries);
    //    let connects = self.connect_nodes(configs.last().unwrap());
    //    self.curp
    //        .update_membership_configs(indices.into_iter().zip(configs))?;
    //    let new_states = self.curp.update_node_states(connects);
    //
    //    self.spawn_sync_follower_tasks(new_states.into_values());
    //    self.curp.update_role();
    //    self.curp.update_transferee();
    //
    //    Ok(())
    //}

    /// Connect to nodes of the given membership config
    fn connect_nodes(&self, config: &Membership) -> BTreeMap<u64, InnerConnectApiWrapper> {
        let nodes = config
            .nodes
            .iter()
            .map(|(id, meta)| (*id, meta.peer_urls().to_vec()))
            .collect();

        inner_connects(nodes, self.curp.client_tls_config()).collect()
    }

    /// Executes an update operation and captures the difference in node states before and after the update.
    pub(super) fn with_states_difference<R, Update: FnOnce() -> R>(
        &self,
        update: Update,
    ) -> (Vec<NodeState>, R) {
        let old = self.curp.clone_node_states();
        let result = update();
        let new = self.curp.clone_node_states();
        let new_states = new
            .into_iter()
            .filter_map(|(id, state)| (!old.contains_key(&id)).then_some(state))
            .collect();

        (new_states, result)
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
