use std::{collections::HashMap, sync::Arc, time::Duration};

use curp_external_api::cmd::Command;
use futures::{future, FutureExt, StreamExt};
use parking_lot::RwLock;
use tonic::Response;
use tracing::warn;
use utils::parking_lot_lock::RwLockMap;

use crate::{
    quorum,
    rpc::{self, connect::ConnectApi, CurpError, FetchClusterRequest, FetchClusterResponse},
};

use super::cluster_state::{ClusterState, ClusterStateReady, ForEachServer};
use super::config::Config;

/// Connect to cluster
///
/// This is used to build a boxed closure that handles the `FetchClusterResponse` and returns
/// new connections.
pub(super) trait ConnectToCluster:
    Fn(&FetchClusterResponse) -> HashMap<u64, Arc<dyn ConnectApi>> + Send + Sync + 'static
{
    /// Clone the value
    fn clone_box(&self) -> Box<dyn ConnectToCluster>;
}

impl<T> ConnectToCluster for T
where
    T: Fn(&FetchClusterResponse) -> HashMap<u64, Arc<dyn ConnectApi>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    fn clone_box(&self) -> Box<dyn ConnectToCluster> {
        Box::new(self.clone())
    }
}

/// Fetch cluster implementation
pub(crate) struct Fetch {
    /// The fetch timeout
    timeout: Duration,
    /// Connect to the given fetch cluster response
    connect_to: Box<dyn ConnectToCluster>,
}

impl Clone for Fetch {
    fn clone(&self) -> Self {
        Self {
            timeout: self.timeout,
            connect_to: self.connect_to.clone_box(),
        }
    }
}

impl Fetch {
    /// Creates a new `Fetch`
    pub(crate) fn new<C: ConnectToCluster>(timeout: Duration, connect_to: C) -> Self {
        Self {
            timeout,
            connect_to: Box::new(connect_to),
        }
    }

    /// Fetch cluster and updates the current state
    pub(crate) async fn fetch_cluster(
        &self,
        state: impl ForEachServer,
    ) -> Result<(ClusterStateReady, FetchClusterResponse), CurpError> {
        /// Retry interval
        const FETCH_RETRY_INTERVAL: Duration = Duration::from_secs(1);
        loop {
            let resp = self
                .pre_fetch(&state)
                .await
                .ok_or(CurpError::internal("cluster not available"))?;
            let new_connects = (self.connect_to)(&resp);
            //let new_members = self.member_addrs(&resp);
            //let new_connects = self.connect_to(new_members);
            //let new_connects = self.override_connects(new_connects);
            let new_state = ClusterStateReady::new(
                resp.leader_id
                    .unwrap_or_else(|| unreachable!("leader id should be Some"))
                    .into(),
                resp.term,
                resp.cluster_version,
                new_connects,
            );
            if self.fetch_term(&new_state).await {
                return Ok((new_state, resp));
            }
            warn!("Fetch cluster failed, sleep for {FETCH_RETRY_INTERVAL:?}");
            tokio::time::sleep(FETCH_RETRY_INTERVAL).await;
        }
    }

    /// Fetch the term of the cluster. This ensures that the current leader is the latest.
    async fn fetch_term(&self, state: &ClusterStateReady) -> bool {
        let timeout = self.timeout;
        let term = state.term();
        let quorum = state.get_quorum(quorum);
        state
            .for_each_server(|c| async move {
                c.fetch_cluster(FetchClusterRequest { linearizable: true }, timeout)
                    .await
            })
            .filter_map(|r| future::ready(r.ok()))
            .map(Response::into_inner)
            .filter(move |resp| future::ready(resp.term == term))
            .take(quorum)
            .count()
            .map(move |t| t >= quorum)
            .await
    }

    /// Prefetch, send fetch cluster request to the cluster and get the
    /// config with the greatest quorum.
    async fn pre_fetch(&self, state: &impl ForEachServer) -> Option<FetchClusterResponse> {
        let timeout = self.timeout;
        let requests = state.for_each_server(|c| async move {
            c.fetch_cluster(FetchClusterRequest { linearizable: true }, timeout)
                .await
        });
        let responses: Vec<_> = requests
            .filter_map(|r| future::ready(r.ok()))
            .map(Response::into_inner)
            .collect()
            .await;
        responses
            .into_iter()
            .filter(|resp| resp.leader_id.is_some())
            .filter(|resp| !resp.members.is_empty())
            .max_by(|x, y| x.term.cmp(&y.term))
    }
}

impl std::fmt::Debug for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetch")
            .field("timeout", &self.timeout)
            .finish()
    }
}
