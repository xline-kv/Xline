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
use crate::rpc::Change;
use crate::rpc::ChangeMembershipRequest;
use crate::rpc::ChangeMembershipResponse;
use crate::rpc::CurpError;
use crate::rpc::MembershipChange;
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
        self.update_and_wait(configs).await?;

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

    /// Updates the membership based on the given change and waits for
    /// the proposal to be committed
    async fn update_and_wait(&self, configs: Vec<Membership>) -> Result<(), CurpError> {
        if configs.is_empty() {
            return Err(CurpError::invalid_member_change());
        }
        for config in configs {
            let (new_nodes, propose_id) = self.curp.update_membership(config)?;
            self.spawn_sync_follower_tasks(new_nodes);
            self.curp.wait_propose_ids(Some(propose_id)).await;
        }
        self.curp.update_role_leader();
        self.curp.update_transferee();

        Ok(())
    }

    /// Spawns background follower sync tasks
    pub(super) fn spawn_sync_follower_tasks(&self, new_nodes: BTreeMap<u64, NodeState>) {
        let task_manager = self.curp.task_manager();
        for (connect, sync_event, remove_event) in
            new_nodes.into_values().map(NodeState::into_parts)
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
