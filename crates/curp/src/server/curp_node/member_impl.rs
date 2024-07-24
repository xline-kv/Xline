#![allow(
    clippy::unused_self,
    clippy::unimplemented,
    clippy::needless_pass_by_value
)] // TODO: remove this after implemented

use curp_external_api::cmd::Command;
use curp_external_api::cmd::CommandExecutor;
use curp_external_api::role_change::RoleChange;

use crate::rpc::AddLearnerRequest;
use crate::rpc::AddLearnerResponse;
use crate::rpc::CurpError;
use crate::rpc::RemoveLearnerRequest;
use crate::rpc::RemoveLearnerResponse;

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
}
