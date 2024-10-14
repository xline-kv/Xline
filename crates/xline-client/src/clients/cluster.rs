use std::sync::Arc;

use curp::rpc::WaitLearnerResponse;
use futures::{Stream, StreamExt};
use tonic::transport::Channel;

use crate::{error::Result, AuthService, CurpClient};
use xlineapi::{
    MemberAddResponse, MemberListResponse, MemberPromoteResponse, MemberRemoveResponse,
    MemberUpdateResponse,
};

/// Client for Cluster operations.
#[derive(Clone)]
#[non_exhaustive]
pub struct ClusterClient {
    /// Inner client
    #[cfg(not(madsim))]
    inner: xlineapi::ClusterClient<AuthService<Channel>>,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient>,
    /// Inner client
    #[cfg(madsim)]
    inner: xlineapi::ClusterClient<Channel>,
}

impl std::fmt::Debug for ClusterClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterClient")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ClusterClient {
    /// Create a new cluster client
    #[inline]
    #[must_use]
    pub fn new(curp_client: Arc<CurpClient>, channel: Channel, token: Option<String>) -> Self {
        Self {
            inner: xlineapi::ClusterClient::new(AuthService::new(
                channel,
                token.and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            curp_client,
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

    /// Wait for learners to be added to the cluster.
    ///
    /// # Arguments
    ///
    /// * `node_ids` - An iterator of node IDs to wait for.
    ///
    /// # Errors
    ///
    /// Returns an error if the request could not be sent or if the response is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions, clients::LearnerStatus};
    /// use anyhow::Result;
    /// use futures::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .cluster_client();
    ///     let mut stream = client.wait_learner(vec![1, 2, 3]).await?;
    ///
    ///     while let Some(Ok(status)) = stream.next().await {
    ///         match status {
    ///             LearnerStatus::Pending { node_id, index } => {
    ///                 println!("Learner node {} is pending with index {}", node_id, index);
    ///             }
    ///             LearnerStatus::Ready => {
    ///                 println!("Learner node is ready");
    ///             }
    ///         }
    ///     }
    ///
    ///     // all learners are up-to-date
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn wait_learner<Ids: IntoIterator<Item = u64>>(
        &mut self,
        node_ids: Ids,
    ) -> Result<Box<dyn Stream<Item = Result<LearnerStatus>> + Send + Unpin>> {
        let stream = self
            .curp_client
            .wait_learner(node_ids.into_iter().collect())
            .await?;
        let stream_mapped = Box::into_pin(stream).map(|r| r.map(Into::into).map_err(Into::into));

        Ok(Box::new(stream_mapped))
    }
}

#[allow(clippy::exhaustive_enums)] // only two states
#[derive(Debug, Clone, Copy)]
/// Represents the state of a learner
pub enum LearnerStatus {
    /// The learner node is pending and not yet ready.
    Pending {
        /// The id of the node
        node_id: u64,
        /// The current replicated log index of the node
        index: u64,
    },
    /// The learner node is up-to-date.
    Ready,
}

impl From<WaitLearnerResponse> for LearnerStatus {
    #[inline]
    fn from(resp: WaitLearnerResponse) -> Self {
        if resp.current_idx == resp.latest_idx {
            return LearnerStatus::Ready;
        }
        LearnerStatus::Pending {
            node_id: resp.node_id,
            index: resp.current_idx,
        }
    }
}
