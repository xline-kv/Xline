use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
};

use futures::{stream::FuturesUnordered, Future, FutureExt, StreamExt};

use crate::{
    member::Membership,
    members::ServerId,
    quorum::QuorumSet,
    rpc::{connect::ConnectApi, connects, CurpError},
};

/// Take an async function and map to all server, returning `FuturesUnordered<F>`
pub(crate) trait ForEachServer {
    /// Take an async function and map to all server, returning `FuturesUnordered<F>`
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F>;
}

/// Cluster State
#[derive(Debug, Clone)]
pub(crate) enum ClusterState {
    /// Initial cluster state
    Init(ClusterStateInit),
    /// Ready cluster state
    Full(ClusterStateFull),
}

impl From<ClusterStateInit> for ClusterState {
    fn from(init: ClusterStateInit) -> Self {
        ClusterState::Init(init)
    }
}

impl From<ClusterStateFull> for ClusterState {
    fn from(ready: ClusterStateFull) -> Self {
        ClusterState::Full(ready)
    }
}

impl ForEachServer for ClusterState {
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        match *self {
            ClusterState::Init(ref init) => init.for_each_server(f),
            ClusterState::Full(ref ready) => ready.for_each_server(f),
        }
    }
}

/// The initial cluster state
///
/// The client must discover the cluster info before sending any propose
#[derive(Clone)]
pub(crate) struct ClusterStateInit {
    /// Member connects
    connects: Vec<Arc<dyn ConnectApi>>,
}

impl ClusterStateInit {
    /// Creates a new `ClusterStateInit`
    pub(crate) fn new(connects: Vec<Arc<dyn ConnectApi>>) -> Self {
        Self { connects }
    }
}

impl ForEachServer for ClusterStateInit {
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        self.connects.clone().into_iter().map(f).collect()
    }
}

impl std::fmt::Debug for ClusterStateInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterStateInit")
            .field("connects_len", &self.connects.len())
            .finish()
    }
}

/// The cluster state that is ready for client propose
#[derive(Clone, Default)]
pub(crate) struct ClusterStateFull {
    /// The membership state
    membership: Membership,
    /// Leader id.
    leader: ServerId,
    /// Term, initialize to 0, calibrated by the server.
    term: u64,
    /// Members' connect, calibrated by the server.
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
}

impl std::fmt::Debug for ClusterStateFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("leader", &self.leader)
            .field("term", &self.term)
            .field("connects", &self.connects.keys())
            .finish()
    }
}

impl ForEachServer for ClusterStateFull {
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        self.connects.values().map(Arc::clone).map(f).collect()
    }
}

impl ClusterStateFull {
    /// Creates a new `ClusterState`
    pub(crate) fn new(
        leader: ServerId,
        term: u64,
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
        membership: Membership,
    ) -> Self {
        Self {
            membership,
            leader,
            term,
            connects,
        }
    }

    /// Take an async function and map to the dedicated server, return None
    /// if the server can not found in local state
    pub(crate) fn map_server<R, F: Future<Output = Result<R, CurpError>>>(
        &self,
        id: ServerId,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> Option<F> {
        // If the leader id cannot be found in connects, it indicates that there is
        // an inconsistency between the client's local leader state and the cluster
        // state, then mock a `WrongClusterVersion` return to the outside.
        self.connects.get(&id).map(Arc::clone).map(f)
    }

    /// Take an async function and map to the dedicated server, return None
    /// if the server can not found in local state
    pub(crate) fn map_leader<R, F: Future<Output = Result<R, CurpError>>>(
        &self,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> F {
        // If the leader id cannot be found in connects, it indicates that there is
        // an inconsistency between the client's local leader state and the cluster
        // state, then mock a `WrongClusterVersion` return to the outside.
        f(Arc::clone(self.connects.get(&self.leader).unwrap_or_else(
            || unreachable!("leader should always exist"),
        )))
    }

    /// Take an async function and map to all server, returning `FuturesUnordered<F>`
    pub(crate) fn for_each_follower<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        self.connects
            .iter()
            .filter_map(|(id, conn)| (*id != self.leader).then_some(conn))
            .map(Arc::clone)
            .map(f)
            .collect()
    }

    /// Execute an operation on each follower, until a quorum is reached.
    ///
    /// Parameters:
    /// - f: Operation to execute on each follower's connection
    /// - filter: Function to filter on each response
    /// - quorum: Function to determine if a quorum is reached, use functions in `QuorumSet` trait
    ///
    /// Returns `true` if then given quorum is reached.
    pub(crate) async fn for_each_follower_with_quorum<R, Fut: Future<Output = R>, F, Q>(
        self,
        mut f: impl FnMut(Arc<dyn ConnectApi>) -> Fut,
        mut filter: F,
        mut expect_quorum: Q,
    ) -> bool
    where
        F: FnMut(R) -> bool,
        Q: FnMut(&dyn QuorumSet<Vec<u64>>, Vec<u64>) -> bool,
    {
        let qs = self.membership.as_joint();
        let leader_id = self.leader_id();

        #[allow(clippy::pattern_type_mismatch)]
        let stream: FuturesUnordered<_> = self
            .member_connects()
            .filter(|(id, _)| *id != leader_id)
            .map(|(id, conn)| f(Arc::clone(conn)).map(move |r| (id, r)))
            .collect();

        let mut filtered =
            stream.filter_map(|(id, r)| futures::future::ready(filter(r).then_some(id)));

        let mut ids = vec![leader_id];
        while let Some(id) = filtered.next().await {
            ids.push(id);
            if expect_quorum(&qs, ids.clone()) {
                return true;
            }
        }

        false
    }

    /// Gets member connects
    fn member_connects(&self) -> impl Iterator<Item = (u64, &Arc<dyn ConnectApi>)> {
        self.membership
            .members()
            .filter_map(|(id, _)| self.connects.get(&id).map(|c| (id, c)))
    }

    /// Returns the quorum size based on the given quorum function
    ///
    /// NOTE: Do not update the cluster in between an `for_each_xxx` and an `get_quorum`, which may
    /// lead to inconsistent quorum.
    pub(crate) fn get_quorum<Q: FnMut(usize) -> usize>(&self, mut quorum: Q) -> usize {
        let cluster_size = self.connects.len();
        quorum(cluster_size)
    }

    /// Returns the term of the cluster
    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    /// Returns the leader id
    pub(crate) fn leader_id(&self) -> u64 {
        self.leader
    }

    /// Calculates the cluster version
    ///
    /// The cluster version is a hash of the current `Membership`
    pub(crate) fn cluster_version(&self) -> u64 {
        self.membership.version()
    }

    /// Returns the membership of the state
    #[cfg(test)]
    pub(crate) fn membership(&self) -> &Membership {
        &self.membership
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use curp_test_utils::test_cmd::TestCommand;
    use tonic::Response;
    use tracing_test::traced_test;

    use crate::{
        client::tests::{build_default_membership, init_mocked_connects},
        rpc::{NodeMetadata, ProposeId, RecordRequest, RecordResponse},
    };

    use super::*;

    #[traced_test]
    #[tokio::test]
    async fn test_cluster_state_full_map_leader_ok() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                0 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: true }))
                    });
                }
                1 | 2 | 3 | 4 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: false }))
                    });
                }
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let req = RecordRequest::new(ProposeId::default(), &TestCommand::default());
        let membership = build_default_membership();
        let state = ClusterStateFull::new(0, 1, connects, membership);
        let conflict = state
            .map_leader(move |conn| async move { conn.record(req, Duration::from_secs(1)).await })
            .await
            .unwrap()
            .into_inner()
            .conflict;

        assert!(conflict);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_cluster_state_full_map_server_ok() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                2 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: true }))
                    });
                }
                0 | 1 | 3 | 4 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: false }))
                    });
                }
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let req = RecordRequest::new(ProposeId::default(), &TestCommand::default());
        let membership = build_default_membership();
        let state = ClusterStateFull::new(0, 1, connects, membership);
        let conflict = state
            .map_server(2, move |conn| async move {
                conn.record(req, Duration::from_secs(1)).await
            })
            .unwrap()
            .await
            .unwrap()
            .into_inner()
            .conflict;

        assert!(conflict);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_cluster_state_full_for_each_follower_ok() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                0 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: true }))
                    });
                }
                1 | 2 | 3 | 4 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: false }))
                    });
                }
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let req = RecordRequest::new(ProposeId::default(), &TestCommand::default());
        let membership = build_default_membership();
        let state = ClusterStateFull::new(0, 1, connects, membership);
        let conflicts: Vec<_> = state
            .for_each_follower({
                move |conn| {
                    let req = req.clone();
                    async move { conn.record(req, Duration::from_secs(1)).await }
                }
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap().into_inner().conflict)
            .collect();

        assert_eq!(conflicts.len(), 4);
        assert!(conflicts.into_iter().all(|c| !c));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_cluster_state_full_for_each_follower_with_quorum_ok() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                0 | 1 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: false }))
                    });
                }
                2 | 3 | 4 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: true }))
                    });
                }
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let req = RecordRequest::new(ProposeId::default(), &TestCommand::default());
        let membership = build_default_membership();
        let state = ClusterStateFull::new(0, 1, connects, membership);
        let record = move |conn: Arc<dyn ConnectApi>| {
            let req = req.clone();
            async move { conn.record(req, Duration::from_secs(1)).await }
        };

        let ok = state
            .for_each_follower_with_quorum(
                record,
                |res| res.is_ok_and(|resp| resp.get_ref().conflict),
                |qs, ids| QuorumSet::is_quorum(qs, ids),
            )
            .await;

        assert!(ok);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_cluster_state_full_for_each_server_ok() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                0 | 1 | 2 | 3 | 4 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: false }))
                    });
                }
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let req = RecordRequest::new(ProposeId::default(), &TestCommand::default());
        let membership = build_default_membership();
        let state = ClusterStateFull::new(0, 1, connects, membership);
        let record = move |conn: Arc<dyn ConnectApi>| {
            let req = req.clone();
            async move { conn.record(req, Duration::from_secs(1)).await }
        };

        let conflicts: Vec<_> = state.for_each_server(record).collect().await;
        assert_eq!(conflicts.len(), 5);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_cluster_state_init_for_each_server_ok() {
        let connects = init_mocked_connects(5, |id, conn| {
            match id {
                0 | 1 | 2 | 3 | 4 => {
                    conn.expect_record().returning(|_req, _timeout| {
                        Ok(Response::new(RecordResponse { conflict: false }))
                    });
                }
                _ => unreachable!("there are only 5 nodes"),
            };
        });
        let req = RecordRequest::new(ProposeId::default(), &TestCommand::default());
        let state = ClusterStateInit::new(connects.into_values().collect());
        let record = move |conn: Arc<dyn ConnectApi>| {
            let req = req.clone();
            async move { conn.record(req, Duration::from_secs(1)).await }
        };

        let conflicts: Vec<_> = state.for_each_server(record).collect().await;
        assert_eq!(conflicts.len(), 5);
    }
}
