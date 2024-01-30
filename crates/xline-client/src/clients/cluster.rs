use std::sync::Arc;

use tonic::transport::Channel;

use crate::{
    error::Result,
    types::cluster::{
        MemberAddRequest, MemberAddResponse, MemberListRequest, MemberListResponse,
        MemberPromoteRequest, MemberPromoteResponse, MemberRemoveRequest, MemberRemoveResponse,
        MemberUpdateRequest, MemberUpdateResponse,
    },
    AuthService,
};

/// Client for Cluster operations.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClusterClient {
    /// Inner client
    #[cfg(not(madsim))]
    inner: xlineapi::ClusterClient<AuthService<Channel>>,
    /// Inner client
    #[cfg(madsim)]
    inner: xlineapi::ClusterClient<Channel>,
}

impl ClusterClient {
    /// Create a new cluster client
    #[inline]
    #[must_use]
    pub fn new(channel: Channel, token: Option<String>) -> Self {
        Self {
            inner: xlineapi::ClusterClient::new(AuthService::new(
                channel,
                token.and_then(|t| t.parse().ok().map(Arc::new)),
            )),
        }
    }

    /// Add a new member to the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use xline_client::types::cluster::*;
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///
    ///     let resp = client.member_add(MemberAddRequest::new(vec!["127.0.0.1:2380".to_owned()], true)).await?;
    ///
    ///     println!(
    ///         "members: {:?}, added: {:?}",
    ///         resp.members, resp.member
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn member_add(&mut self, request: MemberAddRequest) -> Result<MemberAddResponse> {
        Ok(self
            .inner
            .member_add(xlineapi::MemberAddRequest::from(request))
            .await?
            .into_inner())
    }

    /// Remove an existing member from the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use xline_client::types::cluster::*;
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_remove(MemberRemoveRequest::new(1)).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    ///  }
    ///
    #[inline]
    pub async fn member_remove(
        &mut self,
        request: MemberRemoveRequest,
    ) -> Result<MemberRemoveResponse> {
        Ok(self
            .inner
            .member_remove(xlineapi::MemberRemoveRequest::from(request))
            .await?
            .into_inner())
    }

    /// Promote an existing member to be the leader of the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use xline_client::types::cluster::*;
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_promote(MemberPromoteRequest::new(1)).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    /// }
    ///
    #[inline]
    pub async fn member_promote(
        &mut self,
        request: MemberPromoteRequest,
    ) -> Result<MemberPromoteResponse> {
        Ok(self
            .inner
            .member_promote(xlineapi::MemberPromoteRequest::from(request))
            .await?
            .into_inner())
    }

    /// Update an existing member in the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use xline_client::types::cluster::*;
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_update(MemberUpdateRequest::new(1, vec!["127.0.0.1:2379".to_owned()])).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    ///  }
    ///
    #[inline]
    pub async fn member_update(
        &mut self,
        request: MemberUpdateRequest,
    ) -> Result<MemberUpdateResponse> {
        Ok(self
            .inner
            .member_update(xlineapi::MemberUpdateRequest::from(request))
            .await?
            .into_inner())
    }

    /// List all members in the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use xline_client::types::cluster::*;
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_list(MemberListRequest::new(false)).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    /// }
    #[inline]
    pub async fn member_list(&mut self, request: MemberListRequest) -> Result<MemberListResponse> {
        Ok(self
            .inner
            .member_list(xlineapi::MemberListRequest::from(request))
            .await?
            .into_inner())
    }
}
