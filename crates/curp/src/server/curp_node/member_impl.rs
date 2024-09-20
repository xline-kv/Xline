#![allow(
    clippy::unused_self,
    clippy::unimplemented,
    clippy::needless_pass_by_value
)] // TODO: remove this after implemented

use std::sync::Arc;

use curp_external_api::cmd::Command;
use curp_external_api::cmd::CommandExecutor;
use curp_external_api::role_change::RoleChange;
use utils::task_manager::tasks::TaskName;

use crate::member::Change;
use crate::rpc::AddLearnerRequest;
use crate::rpc::AddLearnerResponse;
use crate::rpc::AddMemberRequest;
use crate::rpc::AddMemberResponse;
use crate::rpc::CurpError;
use crate::rpc::Redirect;
use crate::rpc::RemoveLearnerRequest;
use crate::rpc::RemoveLearnerResponse;
use crate::rpc::RemoveMemberRequest;
use crate::rpc::RemoveMemberResponse;

use super::CurpNode;

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> CurpNode<C, CE, RC> {
    /// Adds a learner to the cluster
    pub(crate) async fn add_learner(
        &self,
        request: AddLearnerRequest,
    ) -> Result<AddLearnerResponse, CurpError> {
        self.ensure_leader()?;
        let node_ids = self.curp.new_node_ids(request.nodes.len());
        let ids_with_meta = node_ids.clone().into_iter().zip(request.nodes).collect();

        self.update_and_wait(Change::AddLearner(ids_with_meta))
            .await?;

        Ok(AddLearnerResponse { node_ids })
    }

    /// Removes a learner from the cluster
    pub(crate) async fn remove_learner(
        &self,
        request: RemoveLearnerRequest,
    ) -> Result<RemoveLearnerResponse, CurpError> {
        self.ensure_leader()?;
        self.update_and_wait(Change::RemoveLearner(request.node_ids))
            .await?;

        Ok(RemoveLearnerResponse {})
    }

    /// Promotes a learner to a member
    pub(crate) async fn add_member(
        &self,
        request: AddMemberRequest,
    ) -> Result<AddMemberResponse, CurpError> {
        self.ensure_leader()?;
        self.update_and_wait(Change::AddMember(request.node_ids))
            .await?;

        Ok(AddMemberResponse {})
    }

    /// Demotes a member to a learner
    pub(crate) async fn remove_member(
        &self,
        request: RemoveMemberRequest,
    ) -> Result<RemoveMemberResponse, CurpError> {
        self.ensure_leader()?;
        self.update_and_wait(Change::RemoveMember(request.node_ids))
            .await?;

        Ok(RemoveMemberResponse {})
    }

    /// Updates the membership based on the given change and waits for
    /// the proposal to be committed
    async fn update_and_wait(&self, change: Change) -> Result<(), CurpError> {
        let configs = self.curp.generate_membership(change.clone());
        if configs.is_empty() {
            return Err(CurpError::invalid_member_change());
        }
        let spawn_sync = |sync_event, remove_event, connect| {
            self.curp.task_manager().spawn(TaskName::SyncFollower, |n| {
                Self::sync_follower_task(
                    Arc::clone(&self.curp),
                    connect,
                    sync_event,
                    Arc::clone(&remove_event),
                    n,
                )
            });
        };
        for config in configs {
            let propose_id = self.curp.update_membership(config, spawn_sync)?;
            self.curp.wait_propose_ids(Some(propose_id)).await;
        }
        self.curp.update_role_leader();

        Ok(())
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
