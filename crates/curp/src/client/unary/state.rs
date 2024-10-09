use std::{collections::HashMap, sync::Arc};

use crate::{members::ServerId, rpc::connect::ConnectApi};

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
