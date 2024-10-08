use std::{collections::HashMap, sync::Arc, time::Duration};

use curp_external_api::cmd::Command;
use futures::{future, Future, FutureExt, StreamExt};
use parking_lot::RwLock;
use tonic::Response;
use tracing::warn;
use utils::parking_lot_lock::RwLockMap;

use crate::{
    quorum::{self, QuorumSet},
    rpc::{self, connect::ConnectApi, CurpError, FetchMembershipRequest, FetchMembershipResponse},
};

use super::cluster_state::{ClusterState, ClusterStateInit, ClusterStateReady, ForEachServer};
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
        state: impl Into<ClusterState>,
    ) -> Result<(ClusterStateReady, FetchMembershipResponse), CurpError> {
        let state = match state.into() {
            ClusterState::Init(state) => {
                let resp = self
                    .fetch_one(&state)
                    .await
                    .ok_or(CurpError::internal("cluster not available"))?;
                Self::build_cluster_state_from_response(self.connect_to.as_ref(), resp.clone())
            }
            ClusterState::Ready(state) => state,
        };

        let (fetch_leader, term_ok) =
            tokio::join!(self.fetch_from_leader(&state), self.fetch_term(state));

        if term_ok {
            return fetch_leader;
        }

        let (leader_state, resp) = fetch_leader?;
        if self.fetch_term(leader_state.clone()).await {
            return Ok((leader_state, resp));
        }

        Err(CurpError::internal("cluster not available"))
    }

    /// Fetch the term of the cluster. This ensures that the current leader is the latest.
    fn fetch_term(&self, state: ClusterStateReady) -> impl Future<Output = bool> {
        let timeout = self.timeout;
        let term = state.term();
        let fetch_membership = move |c: Arc<dyn ConnectApi>| async move {
            c.fetch_membership(FetchMembershipRequest {}, timeout).await
        };

        state.for_each_follower_with_quorum(
            fetch_membership,
            move |r| r.is_ok_and(|ok| ok.get_ref().term == term),
            |qs, ids| QuorumSet::is_quorum(qs, ids),
        )
    }

    /// Fetch cluster state from leader
    fn fetch_from_leader(
        &self,
        state: &ClusterStateReady,
    ) -> impl Future<Output = Result<(ClusterStateReady, FetchMembershipResponse), CurpError>> {
        let timeout = self.timeout;
        let connect_to = self.connect_to.clone_box();
        state.map_leader(|c| async move {
            let result = c.fetch_membership(FetchMembershipRequest {}, timeout).await;
            result.map(|resp| {
                let resp = resp.into_inner();
                let fetch_state =
                    Self::build_cluster_state_from_response(connect_to.as_ref(), resp.clone());
                (fetch_state, resp)
            })
        })
    }

    /// Sends fetch membership request to the cluster, and returns the first response
    async fn fetch_one(&self, state: &ClusterStateInit) -> Option<FetchMembershipResponse> {
        let timeout = self.timeout;
        let resps: Vec<_> = state
            .for_each_server(|c| async move {
                c.fetch_membership(FetchMembershipRequest {}, timeout).await
            })
            .collect()
            .await;

        resps
            .into_iter()
            .filter_map(Result::ok)
            .map(Response::into_inner)
            .max_by(|x, y| x.term.cmp(&y.term))
    }

    /// Build `ClusterStateReady` from `FetchMembershipResponse`
    fn build_cluster_state_from_response(
        connect_to: &dyn ConnectToCluster,
        resp: FetchMembershipResponse,
    ) -> ClusterStateReady {
        let connects = (connect_to)(&resp);
        ClusterStateReady::new(resp.leader_id, resp.term, connects, resp.into_membership())
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
        client::{
            cluster_state::{ClusterState, ClusterStateInit, ForEachServer},
            config::Config,
            tests::init_mocked_connects,
        },
        rpc::{
            self, connect::ConnectApi, CurpError, FetchMembershipResponse, Member, Node,
            NodeMetadata,
        },
    };

    use super::Fetch;

    impl From<HashMap<u64, Arc<dyn ConnectApi>>> for ClusterState {
        fn from(connects: HashMap<u64, Arc<dyn ConnectApi>>) -> Self {
            ClusterState::Init(ClusterStateInit::new(connects.into_values().collect()))
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
                meta: Some(NodeMetadata::default()),
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
