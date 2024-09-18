use std::sync::Arc;

use xlineapi::command::CurpClient;

use crate::error::Result;

/// Client for member operations.
#[derive(Clone)]
pub struct MemberClient {
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient>,
}

impl MemberClient {
    /// New `MemberClient`
    #[inline]
    pub(crate) fn new(curp_client: Arc<CurpClient>) -> Self {
        Self { curp_client }
    }

    /// Adds some learners to the cluster.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use xline_client::{Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .member_client();
    ///
    ///     let ids = client
    ///         .add_learner(vec!["10.0.0.4:2379".to_owned(), "10.0.0.5:2379".to_owned()])
    ///         .await?;
    ///
    ///     println!("got node ids of new learners: {ids:?}");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn add_learner(&self, nodes: Vec<Node>) -> Result<Vec<u64>> {
        self.curp_client
            .add_learner(nodes.into_iter().map(Into::into).collect())
            .await
            .map_err(Into::into)
    }

    /// Removes some learners from the cluster.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use xline_client::{Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // the name and address of all curp members
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .member_client();
    ///
    ///     client.remove_learner(vec![0, 1, 2]).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn remove_learner(&self, ids: Vec<u64>) -> Result<()> {
        self.curp_client
            .remove_learner(ids)
            .await
            .map_err(Into::into)
    }
}

impl std::fmt::Debug for MemberClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberClient").finish()
    }
}

/// Represents a node in the cluster with its associated metadata.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Node {
    /// Name of the node.
    pub name: String,
    /// List of URLs used for peer-to-peer communication.
    pub peer_urls: Vec<String>,
    /// List of URLs used for client communication.
    pub client_urls: Vec<String>,
}

impl Node {
    /// Creates a new `Node`
    #[inline]
    #[must_use]
    pub fn new<N, A, AS>(name: N, peer_urls: AS, client_urls: AS) -> Self
    where
        N: AsRef<str>,
        A: AsRef<str>,
        AS: IntoIterator<Item = A>,
    {
        Self {
            name: name.as_ref().to_owned(),
            peer_urls: peer_urls
                .into_iter()
                .map(|s| s.as_ref().to_owned())
                .collect(),
            client_urls: client_urls
                .into_iter()
                .map(|s| s.as_ref().to_owned())
                .collect(),
        }
    }
}

impl From<Node> for curp::rpc::NodeMetadata {
    #[inline]
    fn from(node: Node) -> Self {
        curp::rpc::NodeMetadata {
            name: node.name,
            peer_urls: node.peer_urls,
            client_urls: node.client_urls,
        }
    }
}
