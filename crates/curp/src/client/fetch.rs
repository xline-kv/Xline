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

use super::cluster_state::ClusterState;
use super::config::Config;

/// Fetch cluster implementation
struct Fetch {
    /// The fetch config
    config: Config,
}

impl Fetch {
    /// Creates a new `Fetch`
    pub(crate) fn new(config: Config) -> Self {
        Self { config }
    }

    /// Fetch cluster and updates the current state
    pub(crate) async fn fetch_cluster(
        &self,
        state: ClusterState,
    ) -> Result<ClusterState, CurpError> {
        /// Retry interval
        const FETCH_RETRY_INTERVAL: Duration = Duration::from_secs(1);
        loop {
            let resp = self
                .pre_fetch(&state)
                .await
                .ok_or(CurpError::internal("cluster not available"))?;
            let new_members = self.member_addrs(&resp);
            let new_connects = self.connect_to(new_members);
            let new_state = ClusterState::new(
                resp.leader_id
                    .unwrap_or_else(|| unreachable!("leader id should be Some"))
                    .into(),
                resp.term,
                resp.cluster_version,
                new_connects,
            );
            if self.fetch_term(&new_state).await {
                return Ok(new_state);
            }
            warn!("Fetch cluster failed, sleep for {FETCH_RETRY_INTERVAL:?}");
            tokio::time::sleep(FETCH_RETRY_INTERVAL).await;
        }
    }

    /// Fetch the term of the cluster. This ensures that the current leader is the latest.
    async fn fetch_term(&self, state: &ClusterState) -> bool {
        let timeout = self.config.wait_synced_timeout();
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
    async fn pre_fetch(&self, state: &ClusterState) -> Option<FetchClusterResponse> {
        let timeout = self.config.wait_synced_timeout();
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

    /// Gets the member addresses to connect to
    fn member_addrs(&self, resp: &FetchClusterResponse) -> HashMap<u64, Vec<String>> {
        if self.config.is_raw_curp() {
            resp.clone().into_peer_urls()
        } else {
            resp.clone().into_client_urls()
        }
    }

    /// Connect to the given addrs
    fn connect_to(
        &self,
        new_members: HashMap<u64, Vec<String>>,
    ) -> HashMap<u64, Arc<dyn ConnectApi>> {
        new_members
            .into_iter()
            .map(|(id, addrs)| {
                let tls_config = self.config.tls_config().cloned();
                (id, rpc::connect(id, addrs, tls_config))
            })
            .collect()
    }
}
