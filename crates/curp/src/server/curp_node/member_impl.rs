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
    pub(crate) fn add_learner(
        &self,
        _request: AddLearnerRequest,
    ) -> Result<AddLearnerResponse, CurpError> {
        unimplemented!()
    }

    /// Removes a learner from the cluster
    pub(crate) fn remove_learner(
        &self,
        _request: RemoveLearnerRequest,
    ) -> Result<RemoveLearnerResponse, CurpError> {
        unimplemented!()
    }
}
