use std::sync::Arc;

use tonic::transport::Channel;

use crate::{error::Result, AuthService};
use xlineapi::{
    MemberAddResponse, MemberListResponse, MemberPromoteResponse, MemberRemoveResponse,
    MemberUpdateResponse,
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
    ///     let resp = client.member_add(["127.0.0.1:2380"], true).await?;
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
    pub async fn member_add<I: Into<String>>(
        &mut self,
        peer_urls: impl Into<Vec<I>>,
        is_learner: bool,
    ) -> Result<MemberAddResponse> {
        Ok(self
            .inner
            .member_add(xlineapi::MemberAddRequest {
                peer_ur_ls: peer_urls.into().into_iter().map(Into::into).collect(),
                is_learner,
            })
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
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_remove(1).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    ///  }
    ///
    #[inline]
    pub async fn member_remove(&mut self, id: u64) -> Result<MemberRemoveResponse> {
        Ok(self
            .inner
            .member_remove(xlineapi::MemberRemoveRequest { id })
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
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_promote(1).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    /// }
    ///
    #[inline]
    pub async fn member_promote(&mut self, id: u64) -> Result<MemberPromoteResponse> {
        Ok(self
            .inner
            .member_promote(xlineapi::MemberPromoteRequest { id })
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
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_update(1, ["127.0.0.1:2379"]).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    ///  }
    ///
    #[inline]
    pub async fn member_update<I: Into<String>>(
        &mut self,
        id: u64,
        peer_urls: impl Into<Vec<I>>,
    ) -> Result<MemberUpdateResponse> {
        Ok(self
            .inner
            .member_update(xlineapi::MemberUpdateRequest {
                id,
                peer_ur_ls: peer_urls.into().into_iter().map(Into::into).collect(),
            })
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
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let resp = client.member_list(false).await?;
    ///
    ///     println!("members: {:?}", resp.members);
    ///
    ///     Ok(())
    /// }
    #[inline]
    pub async fn member_list(&mut self, linearizable: bool) -> Result<MemberListResponse> {
        Ok(self
            .inner
            .member_list(xlineapi::MemberListRequest { linearizable })
            .await?
            .into_inner())
    }
}
