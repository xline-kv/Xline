use std::{collections::HashMap, sync::Arc};

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
    Ready(ClusterStateReady),
}

impl ForEachServer for ClusterState {
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        match *self {
            ClusterState::Init(ref init) => init.for_each_server(f),
            ClusterState::Ready(ref ready) => ready.for_each_server(f),
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
pub(crate) struct ClusterStateReady {
    /// The membership state
    membership: Membership,
    /// Leader id.
    leader: ServerId,
    /// Term, initialize to 0, calibrated by the server.
    term: u64,
    /// Cluster version, initialize to 0, calibrated by the server.
    cluster_version: u64,
    /// Members' connect, calibrated by the server.
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
}

impl std::fmt::Debug for ClusterStateReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("leader", &self.leader)
            .field("term", &self.term)
            .field("cluster_version", &self.cluster_version)
            .field("connects", &self.connects.keys())
            .finish()
    }
}

impl ForEachServer for ClusterStateReady {
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        self.connects.values().map(Arc::clone).map(f).collect()
    }
}

impl ClusterStateReady {
    /// Creates a new `ClusterState`
    pub(crate) fn new(
        leader: ServerId,
        term: u64,
        cluster_version: u64,
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
    ) -> Self {
        Self {
            membership: Membership::default(), // FIXME: build initial membership config
            leader,
            term,
            cluster_version,
            connects,
        }
    }

    /// Creates a new `ClusterState`
    pub(crate) fn new_membership(
        leader: ServerId,
        term: u64,
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
        membership: Membership,
    ) -> Self {
        Self {
            membership,
            leader,
            term,
            cluster_version: 0,
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

    /// Returns the cluster version
    pub(crate) fn cluster_version(&self) -> u64 {
        self.cluster_version
    }

    /// Returns the leader id
    pub(crate) fn leader_id(&self) -> u64 {
        self.leader
    }
}
