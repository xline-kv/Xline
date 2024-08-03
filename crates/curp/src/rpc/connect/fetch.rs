use std::time::Duration;

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;
use tonic::transport::Channel;

use crate::rpc::protocol_client::ProtocolClient;
use crate::rpc::{CurpError, FetchMembershipRequest};
use crate::with_timeout;

/// Fetch membership APIs.
#[cfg_attr(test, automock)]
#[async_trait]
pub(crate) trait FetchApi: Send + Sync + 'static {
    async fn fetch_membership(
        &self,
        request: FetchMembershipRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<crate::rpc::FetchMembershipResponse>, CurpError>;
}

#[async_trait]
impl FetchApi for ProtocolClient<Channel> {
    async fn fetch_membership(
        &self,
        request: FetchMembershipRequest,
        timeout: Duration,
    ) -> Result<tonic::Response<crate::rpc::FetchMembershipResponse>, CurpError> {
        let mut client = self.clone();
        let req = tonic::Request::new(request);
        with_timeout!(timeout, Self::fetch_membership(&mut client, req)).map_err(Into::into)
    }
}
