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
    pub async fn add_learner(&self, nodes: Vec<Node>) -> Result<()> {
        let changes = nodes.into_iter().map(Change::Add).map(Into::into).collect();
        self.curp_client
            .change_membership(changes)
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
        let changes = ids
            .into_iter()
            .map(Change::Remove)
            .map(Into::into)
            .collect();
        self.curp_client
            .change_membership(changes)
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
    /// The id of the node
    pub node_id: u64,
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
    pub fn new<N, A, AS>(id: u64, name: N, peer_urls: AS, client_urls: AS) -> Self
    where
        N: AsRef<str>,
        A: AsRef<str>,
        AS: IntoIterator<Item = A>,
    {
        Self {
            node_id: id,
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

impl From<Node> for curp::rpc::Node {
    #[inline]
    fn from(node: Node) -> Self {
        let meta = curp::rpc::NodeMetadata {
            name: node.name,
            peer_urls: node.peer_urls,
            client_urls: node.client_urls,
        };
        Self {
            node_id: node.node_id,
            meta: Some(meta),
        }
    }
}

/// Represents a change in cluster membership.
#[allow(variant_size_differences)]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Change {
    /// Adds a new learner.
    Add(Node),
    /// Removes a learner by its id.
    Remove(u64),
    /// Promotes a learner to voter
    Promote(u64),
    /// Demotes a voter to learner.
    Demote(u64),
}

impl From<Change> for curp::rpc::Change {
    #[inline]
    fn from(change: Change) -> Self {
        match change {
            Change::Add(node) => curp::rpc::Change::Add(node.into()),
            Change::Remove(id) => curp::rpc::Change::Remove(id),
            Change::Promote(id) => curp::rpc::Change::Promote(id),
            Change::Demote(id) => curp::rpc::Change::Demote(id),
        }
    }
}
