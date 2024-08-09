#![allow(
    clippy::unused_self,
    clippy::unimplemented,
    clippy::needless_pass_by_value
)] // TODO: remove this after implemented

use curp_external_api::cmd::Command;
use curp_external_api::cmd::CommandExecutor;
use curp_external_api::role_change::RoleChange;

use crate::member::Change;
use crate::rpc::AddLearnerRequest;
use crate::rpc::AddLearnerResponse;
use crate::rpc::AddMemberRequest;
use crate::rpc::AddMemberResponse;
use crate::rpc::CurpError;
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
        let addrs = request.node_addrs;
        let ret = self.curp.add_learner(&addrs);
        self.curp.wait_propose_ids(Some(ret.propose_id())).await;

        Ok(AddLearnerResponse {
            node_ids: ret.into_inner(),
        })
    }

    /// Removes a learner from the cluster
    pub(crate) async fn remove_learner(
        &self,
        request: RemoveLearnerRequest,
    ) -> Result<RemoveLearnerResponse, CurpError> {
        let node_ids = request.node_ids;
        let ret = self
            .curp
            .remove_learner(node_ids)
            .ok_or(CurpError::invalid_member_change())?;
        self.curp.wait_propose_ids(Some(ret.propose_id())).await;

        Ok(RemoveLearnerResponse {})
    }

    /// Promotes a learner to a member
    pub(crate) async fn add_member(
        &self,
        request: AddMemberRequest,
    ) -> Result<AddMemberResponse, CurpError> {
        self.update_and_wait(Change::AddMember(request.node_ids))
            .await?;

        Ok(AddMemberResponse {})
    }

    /// Demotes a member to a learner
    pub(crate) async fn remove_member(
        &self,
        request: RemoveMemberRequest,
    ) -> Result<RemoveMemberResponse, CurpError> {
        self.update_and_wait(Change::RemoveMember(request.node_ids))
            .await?;

        Ok(RemoveMemberResponse {})
    }

    /// Updates the membership based on the given change and waits for
    /// the proposal to be committed
    async fn update_and_wait(&self, change: Change) -> Result<(), CurpError> {
        let configs = self.curp.generate_membership(change);
        if configs.is_empty() {
            return Err(CurpError::invalid_member_change());
        }
        for config in configs {
            let propose_id = self.curp.update_membership(config);
            self.curp.wait_propose_ids(Some(propose_id)).await;
        }

        Ok(())
    }
}
