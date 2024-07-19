use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;

use crate::rpc::proto::memberpb::member_protocol_client::MemberProtocolClient;
use crate::rpc::proto::memberpb::AddLearnerRequest;
use crate::rpc::proto::memberpb::AddLearnerResponse;

/// Membership APIs.
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait MemberApi: Send + Sync + 'static {
    async fn add_learner(
        &self,
        request: AddLearnerRequest,
    ) -> Result<AddLearnerResponse, tonic::Status>;
}
