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

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use futures::stream::FuturesUnordered;
    use tracing_test::traced_test;

    use crate::{
        client::{cluster_state::ForEachServer, config::Config, tests::init_mocked_connects},
        rpc::{connect::ConnectApi, CurpError, FetchClusterResponse, Member},
    };

    use super::Fetch;

    impl ForEachServer for HashMap<u64, Arc<dyn ConnectApi>> {
        fn for_each_server<R, F: futures::Future<Output = R>>(
            &self,
            f: impl FnMut(Arc<dyn ConnectApi>) -> F,
        ) -> FuturesUnordered<F> {
            self.values().cloned().map(f).collect()
        }
    }

    /// Create unary client for test
    fn init_fetch() -> Fetch {
        Fetch::new(Config::new(
            None,
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
            true,
        ))
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_unary_fetch_clusters_serializable() {
        let connects = init_mocked_connects(3, |_id, conn| {
            conn.expect_fetch_cluster().returning(|_req, _timeout| {
                Ok(tonic::Response::new(FetchClusterResponse {
                    leader_id: Some(0.into()),
                    term: 1,
                    cluster_id: 123,
                    members: vec![
                        Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                        Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                        Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                    ],
                    cluster_version: 1,
                }))
            });
        });
        let fetch = init_fetch();
        let (_, res) = fetch.fetch_cluster(connects).await.unwrap();
        assert_eq!(
            res.into_peer_urls(),
            HashMap::from([
                (0, vec!["A0".to_owned()]),
                (1, vec!["A1".to_owned()]),
                (2, vec!["A2".to_owned()])
            ])
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unary_fetch_clusters_linearizable() {
        let connects = init_mocked_connects(5, |id, conn| {
            conn.expect_fetch_cluster()
                .return_once(move |_req, _timeout| {
                    let resp = match id {
                        0 => FetchClusterResponse {
                            leader_id: Some(0.into()),
                            term: 2,
                            cluster_id: 123,
                            members: vec![
                                Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                                Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                                Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                                Member::new(3, "S3", vec!["A3".to_owned()], [], false),
                                Member::new(4, "S4", vec!["A4".to_owned()], [], false),
                            ],
                            cluster_version: 1,
                        },
                        1 | 4 => FetchClusterResponse {
                            leader_id: Some(0.into()),
                            term: 2,
                            cluster_id: 123,
                            members: vec![], // linearizable read from follower returns empty members
                            cluster_version: 1,
                        },
                        2 => FetchClusterResponse {
                            leader_id: None,
                            term: 23, // abnormal term
                            cluster_id: 123,
                            members: vec![],
                            cluster_version: 1,
                        },
                        3 => FetchClusterResponse {
                            leader_id: Some(3.into()), // imagine this node is a old leader
                            term: 1,                   // with the old term
                            cluster_id: 123,
                            members: vec![
                                Member::new(0, "S0", vec!["B0".to_owned()], [], false),
                                Member::new(1, "S1", vec!["B1".to_owned()], [], false),
                                Member::new(2, "S2", vec!["B2".to_owned()], [], false),
                                Member::new(3, "S3", vec!["B3".to_owned()], [], false),
                                Member::new(4, "S4", vec!["B4".to_owned()], [], false),
                            ],
                            cluster_version: 1,
                        },
                        _ => unreachable!("there are only 5 nodes"),
                    };
                    Ok(tonic::Response::new(resp))
                });
        });
        let fetch = init_fetch();
        let (_, res) = fetch.fetch_cluster(connects).await.unwrap();
        assert_eq!(
            res.into_peer_urls(),
            HashMap::from([
                (0, vec!["A0".to_owned()]),
                (1, vec!["A1".to_owned()]),
                (2, vec!["A2".to_owned()]),
                (3, vec!["A3".to_owned()]),
                (4, vec!["A4".to_owned()])
            ])
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unary_fetch_clusters_linearizable_failed() {
        let connects = init_mocked_connects(5, |id, conn| {
            conn.expect_fetch_cluster()
                .return_once(move |_req, _timeout| {
                    let resp = match id {
                        0 => FetchClusterResponse {
                            leader_id: Some(0.into()),
                            term: 2,
                            cluster_id: 123,
                            members: vec![
                                Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                                Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                                Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                                Member::new(3, "S3", vec!["A3".to_owned()], [], false),
                                Member::new(4, "S4", vec!["A4".to_owned()], [], false),
                            ],
                            cluster_version: 1,
                        },
                        1 => FetchClusterResponse {
                            leader_id: Some(0.into()),
                            term: 2,
                            cluster_id: 123,
                            members: vec![], // linearizable read from follower returns empty members
                            cluster_version: 1,
                        },
                        2 => FetchClusterResponse {
                            leader_id: None, // imagine this node is a disconnected candidate
                            term: 23,        // with a high term
                            cluster_id: 123,
                            members: vec![],
                            cluster_version: 1,
                        },
                        3 => FetchClusterResponse {
                            leader_id: Some(3.into()), // imagine this node is a old leader
                            term: 1,                   // with the old term
                            cluster_id: 123,
                            members: vec![
                                Member::new(0, "S0", vec!["B0".to_owned()], [], false),
                                Member::new(1, "S1", vec!["B1".to_owned()], [], false),
                                Member::new(2, "S2", vec!["B2".to_owned()], [], false),
                                Member::new(3, "S3", vec!["B3".to_owned()], [], false),
                                Member::new(4, "S4", vec!["B4".to_owned()], [], false),
                            ],
                            cluster_version: 1,
                        },
                        4 => FetchClusterResponse {
                            leader_id: Some(3.into()), // imagine this node is a old follower of old leader(3)
                            term: 1,                   // with the old term
                            cluster_id: 123,
                            members: vec![],
                            cluster_version: 1,
                        },
                        _ => unreachable!("there are only 5 nodes"),
                    };
                    Ok(tonic::Response::new(resp))
                });
        });
        let fetch = init_fetch();
        let err = fetch.fetch_cluster(connects).await.unwrap_err();
        // only server(0, 1)'s responses are valid, less than majority quorum(3), got a
        // mocked RpcTransport to retry
        assert_eq!(err, CurpError::RpcTransport(()));
    }
}
