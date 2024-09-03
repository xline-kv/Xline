use std::{collections::HashMap, sync::Arc};

use futures::{stream::FuturesUnordered, Future};

use crate::{
    members::ServerId,
    rpc::{connect::ConnectApi, CurpError},
};

/// The cluster state
///
/// The client must discover the cluster info before sending any propose
struct ClusterState {
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
    ) -> Option<F> {
        // If the leader id cannot be found in connects, it indicates that there is
        // an inconsistency between the client's local leader state and the cluster
        // state, then mock a `WrongClusterVersion` return to the outside.
        self.connects.get(&self.leader).map(Arc::clone).map(f)
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

    /// Updates the current leader
    fn update_leader(&mut self, leader: ServerId, term: u64) {
        self.leader = leader;
        self.term = term;
    }

    /// Updates the cluster
    fn update_cluster(
        &mut self,
        cluster_version: u64,
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
    ) {
        self.cluster_version = cluster_version;
        self.connects = connects;
    }
}
