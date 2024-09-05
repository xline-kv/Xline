use std::{collections::HashMap, sync::Arc};

use futures::{stream::FuturesUnordered, Future};

use crate::{
    members::ServerId,
    rpc::{connect::ConnectApi, CurpError},
};

/// The cluster state
///
/// The client must discover the cluster info before sending any propose
#[derive(Default, Clone)]
pub(crate) struct ClusterState {
    /// Leader id.
    leader: ServerId,
    /// Term, initialize to 0, calibrated by the server.
    term: u64,
    /// Cluster version, initialize to 0, calibrated by the server.
    cluster_version: u64,
    /// Members' connect, calibrated by the server.
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
}

impl std::fmt::Debug for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("leader", &self.leader)
            .field("term", &self.term)
            .field("cluster_version", &self.cluster_version)
            .field("connects", &self.connects.keys())
            .finish()
    }
}

impl ClusterState {
    /// Creates a new `ClusterState`
    pub(crate) fn new(
        leader: ServerId,
        term: u64,
        cluster_version: u64,
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
    ) -> Self {
        Self {
            leader,
            term,
            cluster_version,
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
            || unreachable!("leader connect should always exists"),
        )))
    }

    /// Take an async function and map to all server, returning `FuturesUnordered<F>`
    pub(crate) fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        self.connects.values().map(Arc::clone).map(f).collect()
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
}
