use std::time::Duration;

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use tonic::transport::Channel;

use crate::rpc::proto::memberpb::member_protocol_client::MemberProtocolClient;
use crate::rpc::proto::memberpb::AddLearnerRequest;
use crate::rpc::proto::memberpb::AddLearnerResponse;
use crate::rpc::proto::memberpb::RemoveLearnerRequest;
use crate::rpc::proto::memberpb::RemoveLearnerResponse;
use crate::rpc::CurpError;
use crate::with_timeout;

/// Membership APIs.
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait MemberApi: Send + Sync + 'static {
    /// Add a learner to the cluster.
    async fn add_learner(
        &self,
        request: AddLearnerRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AddLearnerResponse>, CurpError>;

    /// Remove a learner from the cluster.
    async fn remove_learner(
        &self,
        request: RemoveLearnerRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RemoveLearnerResponse>, CurpError>;
}

#[async_trait]
impl MemberApi for MemberProtocolClient<Channel> {
    async fn add_learner(
        &self,
        request: AddLearnerRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<AddLearnerResponse>, CurpError> {
        let mut client = self.clone();
        let req = tonic::Request::new(request);
        with_timeout!(timeout, Self::add_learner(&mut client, req)).map_err(Into::into)
    }

    async fn remove_learner(
        &self,
        request: RemoveLearnerRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<RemoveLearnerResponse>, CurpError> {
        let mut client = self.clone();
        let req = tonic::Request::new(request);
        with_timeout!(timeout, Self::remove_learner(&mut client, req)).map_err(Into::into)
    }
}
