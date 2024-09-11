use std::{collections::HashMap, sync::Arc, time::Duration};

use curp_external_api::cmd::Command;
use futures::{future, FutureExt, StreamExt};
use parking_lot::RwLock;
use tonic::Response;
use tracing::warn;
use utils::parking_lot_lock::RwLockMap;

use crate::{
    quorum,
    rpc::{self, connect::ConnectApi, CurpError, FetchMembershipRequest, FetchMembershipResponse},
};

use super::cluster_state::{ClusterState, ClusterStateReady, ForEachServer};
use super::config::Config;

/// Connect to cluster
///
/// This is used to build a boxed closure that handles the `FetchClusterResponse` and returns
/// new connections.
pub(super) trait ConnectToCluster:
    Fn(&FetchMembershipResponse) -> HashMap<u64, Arc<dyn ConnectApi>> + Send + Sync + 'static
{
    /// Clone the value
    fn clone_box(&self) -> Box<dyn ConnectToCluster>;
}

impl<T> ConnectToCluster for T
where
    T: Fn(&FetchMembershipResponse) -> HashMap<u64, Arc<dyn ConnectApi>>
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

    #[cfg(test)]
    /// Creates a new `Fetch` fetch disabled
    pub(crate) fn new_disable() -> Self {
        Self {
            timeout: Duration::default(),
            connect_to: Box::new(|_| HashMap::default()),
        }
    }

    /// Fetch cluster and updates the current state
    pub(crate) async fn fetch_cluster(
        &self,
        state: impl ForEachServer,
    ) -> Result<(ClusterStateReady, FetchMembershipResponse), CurpError> {
        let resp = self
            .pre_fetch(&state)
            .await
            .ok_or(CurpError::internal("cluster not available"))?;
        let connects = (self.connect_to)(&resp);
        let new_state = ClusterStateReady::new_membership(
            resp.leader_id,
            resp.term,
            connects,
            resp.clone().into_membership(),
        );
        if self.fetch_term(&new_state).await {
            return Ok((new_state, resp));
        }

        Err(CurpError::internal("cluster not available"))
    }

    /// Fetch the term of the cluster. This ensures that the current leader is the latest.
    async fn fetch_term(&self, state: &ClusterStateReady) -> bool {
        let timeout = self.timeout;
        let term = state.term();
        let quorum = state.get_quorum(quorum);
        state
            .for_each_server(|c| async move {
                c.fetch_membership(FetchMembershipRequest {}, timeout).await
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
    async fn pre_fetch(&self, state: &impl ForEachServer) -> Option<FetchMembershipResponse> {
        let timeout = self.timeout;
        let requests = state.for_each_server(|c| async move {
            c.fetch_membership(FetchMembershipRequest {}, timeout).await
        });
        let responses: Vec<_> = requests
            .filter_map(|r| future::ready(r.ok()))
            .map(Response::into_inner)
            .collect()
            .await;
        responses
            .into_iter()
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
        rpc::{self, connect::ConnectApi, CurpError, FetchMembershipResponse, Member, Node},
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
    fn init_fetch(connects: HashMap<u64, Arc<dyn ConnectApi>>) -> Fetch {
        Fetch::new(Duration::from_secs(0), move |_| connects.clone())
    }

    fn build_membership_resp(
        leader_id: Option<u64>,
        term: u64,
        members: impl IntoIterator<Item = u64>,
    ) -> Result<tonic::Response<FetchMembershipResponse>, CurpError> {
        let leader_id = leader_id.ok_or(CurpError::leader_transfer("no current leader"))?;

        let members: Vec<_> = members.into_iter().collect();
        let nodes: Vec<Node> = members
            .clone()
            .into_iter()
            .map(|node_id| Node {
                node_id,
                addr: String::new(),
            })
            .collect();
        let qs = rpc::QuorumSet { set: members };

        let resp = FetchMembershipResponse {
            members: vec![qs],
            nodes,
            term,
            leader_id,
        };
        Ok(tonic::Response::new(resp))
    }

    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_unary_fetch_clusters_serializable() {
        let connects = init_mocked_connects(3, |_id, conn| {
            conn.expect_fetch_membership()
                .returning(|_req, _timeout| build_membership_resp(Some(0), 1, vec![0, 1, 2]));
        });
        let fetch = init_fetch(connects.clone());
        let (_, res) = fetch.fetch_cluster(connects).await.unwrap();
        assert_eq!(res.members[0].set, vec![0, 1, 2]);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unary_fetch_clusters_linearizable() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                0 => conn.expect_fetch_membership().returning(|_req, _timeout| {
                    build_membership_resp(Some(0), 2, vec![0, 1, 2, 3, 4])
                }),
                1 | 4 => conn
                    .expect_fetch_membership()
                    .returning(|_req, _timeout| build_membership_resp(Some(0), 2, vec![])),
                2 => conn
                    .expect_fetch_membership()
                    .returning(|_req, _timeout| build_membership_resp(None, 23, vec![])),
                3 => conn.expect_fetch_membership().returning(|_req, _timeout| {
                    build_membership_resp(Some(3), 1, vec![1, 2, 3, 4])
                }),
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let fetch = init_fetch(connects.clone());
        let (_, res) = fetch.fetch_cluster(connects).await.unwrap();

        assert_eq!(res.members[0].set, vec![0, 1, 2, 3, 4]);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_unary_fetch_clusters_linearizable_failed() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                0 => {
                    conn.expect_fetch_membership().returning(|_req, _timeout| {
                        build_membership_resp(Some(0), 2, vec![0, 1, 2, 3, 4])
                    });
                }
                1 => {
                    conn.expect_fetch_membership()
                        .returning(|_req, _timeout| build_membership_resp(Some(0), 2, vec![]));
                }
                2 => {
                    conn.expect_fetch_membership()
                        .returning(|_req, _timeout| build_membership_resp(None, 23, vec![]));
                }
                3 => {
                    conn.expect_fetch_membership().returning(|_req, _timeout| {
                        build_membership_resp(Some(3), 1, vec![0, 1, 2, 3, 4])
                    });
                }
                4 => {
                    conn.expect_fetch_membership()
                        .returning(|_req, _timeout| build_membership_resp(Some(3), 1, vec![]));
                }
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let fetch = init_fetch(connects.clone());
        // only server(0, 1)'s responses are valid, less than majority quorum(3).
        fetch.fetch_cluster(connects).await.unwrap_err();
    }
}
