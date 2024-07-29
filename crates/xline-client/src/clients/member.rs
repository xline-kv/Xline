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
    pub async fn add_learner(&self, addrs: Vec<String>) -> Result<Vec<u64>> {
        self.curp_client
            .add_learner(addrs)
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
