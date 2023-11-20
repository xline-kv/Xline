/// Unary rpc client
mod unary;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::{stream::FuturesUnordered, StreamExt};
use utils::config::ClientConfig;

use crate::{
    members::ServerId,
    rpc::{
        connect::ConnectApi, protocol_client::ProtocolClient, ConfChange, FetchClusterRequest,
        FetchClusterResponse, Member, ReadState,
    },
};

/// The response of propose command, deserialized from [`crate::rpc::ProposeResponse`] or
/// [`crate::rpc::WaitSyncedResponse`].
#[allow(type_alias_bounds)] // that's not bad
type ProposeResponse<C: Command> = Result<(C::ER, Option<C::ASR>), C::Error>;

/// `ClientApi`, a higher wrapper for `ConnectApi`, providing some methods for communicating to
/// the whole curp cluster. Automatically discovery curp server to update it's quorum.
#[async_trait]
pub trait ClientApi<C: Command> {
    /// The client error
    type Error;

    /// Get the local connection when the client is on the server node.
    fn local_connect(&self) -> Option<Arc<dyn ConnectApi>>;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &C,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<C>, Self::Error>;

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, Self::Error>;

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), Self::Error>;

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, Self::Error>;

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<FetchClusterResponse, Self::Error>;

    /// Fetch leader id
    #[inline]
    async fn fetch_leader_id(&self, linearizable: bool) -> Result<ServerId, Self::Error> {
        if linearizable {
            let resp = self.fetch_cluster(true).await?;
            return Ok(resp.leader_id.unwrap_or_else(|| {
                unreachable!("linearizable fetch cluster should return a leader id")
            }));
        }
        let resp = self.fetch_cluster(false).await?;
        if let Some(id) = resp.leader_id {
            return Ok(id);
        }
        // fallback to linearizable fetch
        self.fetch_leader_id(true).await
    }
}

/// Client builder to build a client
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    /// local server id
    local_server_id: Option<ServerId>,
    /// initial cluster version
    cluster_version: Option<u64>,
    /// client configuration
    config: ClientConfig,
    /// initial all members
    all_members: Option<HashMap<ServerId, Vec<String>>>,
}

impl ClientBuilder {
    /// Create a client builder
    #[inline]
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self {
            local_server_id: None,
            config,
            all_members: None,
            cluster_version: None,
        }
    }

    /// Set the local server id
    #[inline]
    #[must_use]
    pub fn local_server_id(&mut self, id: ServerId) -> &mut Self {
        self.local_server_id = Some(id);
        self
    }

    /// Set the initial cluster version
    #[inline]
    #[must_use]
    pub fn cluster_version(&mut self, cluster_version: u64) -> &mut Self {
        self.cluster_version = Some(cluster_version);
        self
    }

    /// Fetch initial all members from some endpoints if you do not know the whole members
    ///
    /// # Errors
    ///
    /// Return `tonic::Status` for connection failure or some server errors.
    #[inline]
    pub async fn fetch_all_members(
        &mut self,
        addrs: Vec<String>,
    ) -> Result<&mut Self, tonic::Status> {
        let propose_timeout = *self.config.propose_timeout();
        let mut futs: FuturesUnordered<_> = addrs
            .into_iter()
            .map(|mut addr| {
                if !addr.starts_with("http://") {
                    addr.insert_str(0, "http://");
                }
                async move {
                    let mut protocol_client = ProtocolClient::connect(addr).await.map_err(|e| {
                        tonic::Status::cancelled(format!("cannot connect to addr, error: {e}"))
                    })?;
                    let mut req = tonic::Request::new(FetchClusterRequest::default());
                    req.set_timeout(propose_timeout);
                    let fetch_cluster_res = protocol_client.fetch_cluster(req).await?.into_inner();
                    Ok::<FetchClusterResponse, tonic::Status>(fetch_cluster_res)
                }
            })
            .collect();
        let mut err = tonic::Status::invalid_argument("addrs is empty");
        while let Some(r) = futs.next().await {
            match r {
                Ok(r) => {
                    self.cluster_version = Some(r.cluster_version);
                    self.all_members = Some(r.into_members_addrs());
                    return Ok(self);
                }
                Err(e) => err = e,
            }
        }
        Err(err)
    }

    /// Set the initial all members
    #[inline]
    #[must_use]
    pub fn set_all_members(&mut self, all_members: HashMap<ServerId, Vec<String>>) -> &mut Self {
        self.all_members = Some(all_members);
        self
    }
}
