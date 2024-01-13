use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::RwLock;
use tracing::debug;

use crate::{
    members::ServerId,
    rpc::{
        self,
        connect::{BypassedConnect, ConnectApi},
        FetchClusterResponse, Protocol,
    },
};

/// Reference to state
pub(super) type StateRef = Arc<RwLock<State>>;

/// Client state
pub(super) struct State {
    /// Leader id. At the beginning, we may not know who the leader is.
    pub(super) leader: Option<ServerId>,
    /// Local server id
    pub(super) local_server: Option<ServerId>,
    /// Term, initialize to 0, calibrated by the server.
    pub(super) term: u64,
    /// Cluster version, initialize to 0, calibrated by the server.
    pub(super) cluster_version: u64,
    /// Members' connect
    pub(super) connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("leader", &self.leader)
            .field("local_server", &self.local_server)
            .field("term", &self.term)
            .field("cluster_version", &self.cluster_version)
            .field("connects", &self.connects.keys())
            .finish()
    }
}

impl State {
    /// For test
    #[cfg(test)]
    pub(super) fn new_ref(
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
        local_server: Option<ServerId>,
        leader: Option<ServerId>,
        term: u64,
        cluster_version: u64,
    ) -> StateRef {
        Arc::new(RwLock::new(Self {
            leader,
            local_server,
            term,
            cluster_version,
            connects,
        }))
    }

    /// Get the local connect
    pub(super) fn local_connect(&self) -> Option<Arc<dyn ConnectApi>> {
        let id = self.local_server?;
        self.connects.get(&id).map(Arc::clone)
    }

    /// Update leader
    pub(super) fn check_and_update_leader(
        &mut self,
        leader_id: Option<ServerId>,
        term: u64,
    ) -> bool {
        match self.term.cmp(&term) {
            Ordering::Less => {
                // reset term only when the resp has leader id to prevent:
                // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                if let Some(new_leader_id) = leader_id {
                    debug!("client term updates to {}", term);
                    debug!("client leader id updates to {new_leader_id}");
                    self.term = term;
                    self.leader = Some(new_leader_id);
                }
            }
            Ordering::Equal => {
                if let Some(new_leader_id) = leader_id {
                    if self.leader.is_none() {
                        debug!("client leader id updates to {new_leader_id}");
                        self.leader = Some(new_leader_id);
                    }
                    assert_eq!(
                        self.leader,
                        Some(new_leader_id),
                        "there should never be two leader in one term"
                    );
                }
            }
            Ordering::Greater => {
                debug!("ignore old term({}) from server", term);
                return false;
            }
        }
        true
    }

    /// Update client state based on [`FetchClusterResponse`]
    pub(super) async fn check_and_update(
        &mut self,
        res: &FetchClusterResponse,
    ) -> Result<(), tonic::transport::Error> {
        if !Self::check_and_update_leader(self, res.leader_id, res.term) {
            return Ok(());
        }
        if self.cluster_version == res.cluster_version {
            debug!(
                "ignore cluster version({}) from server",
                res.cluster_version
            );
            return Ok(());
        }

        debug!("client cluster version updated to {}", res.cluster_version);
        self.cluster_version = res.cluster_version;

        let mut new_members = res.clone().into_members_addrs();

        let old_ids = self.connects.keys().copied().collect::<HashSet<_>>();
        let new_ids = new_members.keys().copied().collect::<HashSet<_>>();

        let diffs = &old_ids ^ &new_ids;
        let sames = &old_ids & &new_ids;

        for diff in diffs {
            if let Entry::Vacant(e) = self.connects.entry(diff) {
                let addrs = new_members
                    .remove(&diff)
                    .unwrap_or_else(|| unreachable!("{diff} must in new member addrs"));
                debug!("client connects to a new server({diff}), address({addrs:?})");
                let new_conn = rpc::connect(diff, addrs).await?;
                let _ig = e.insert(new_conn);
            } else {
                debug!("client removes old server({diff})");
                let _ig = self.connects.remove(&diff);
            }
        }
        for same in sames {
            let conn = self
                .connects
                .get(&same)
                .unwrap_or_else(|| unreachable!("{same} must in old connects"));
            let addrs = new_members
                .remove(&same)
                .unwrap_or_else(|| unreachable!("{same} must in new member addrs"));
            conn.update_addrs(addrs).await?;
        }

        Ok(())
    }
}

/// Builder for state
#[derive(Debug, Clone)]
pub(super) struct StateBuilder {
    /// All members (required)
    all_members: HashMap<ServerId, Vec<String>>,
    /// Initial leader state (optional)
    leader_state: Option<(ServerId, u64)>,
    /// Initial cluster version (optional)
    cluster_version: Option<u64>,
}

impl StateBuilder {
    /// Create a state builder
    pub(super) fn new(all_members: HashMap<ServerId, Vec<String>>) -> Self {
        Self {
            all_members,
            leader_state: None,
            cluster_version: None,
        }
    }

    /// Set the leader state (optional)
    pub(super) fn set_leader_state(&mut self, id: ServerId, term: u64) {
        self.leader_state = Some((id, term));
    }

    /// Set the cluster version (optional)
    pub(super) fn set_cluster_version(&mut self, cluster_version: u64) {
        self.cluster_version = Some(cluster_version);
    }

    /// Build the state with local server
    pub(super) async fn build_bypassed<P: Protocol>(
        mut self,
        local_server_id: ServerId,
        local_server: P,
    ) -> Result<State, tonic::transport::Error> {
        debug!("client bypassed server({local_server_id})");

        let _ig = self.all_members.remove(&local_server_id);
        let mut connects: HashMap<_, _> = rpc::connects(self.all_members.clone()).await?.collect();
        let __ig = connects.insert(
            local_server_id,
            Arc::new(BypassedConnect::new(local_server_id, local_server)),
        );

        Ok(State {
            leader: self.leader_state.map(|state| state.0),
            local_server: Some(local_server_id),
            term: self.leader_state.map_or(0, |state| state.1),
            cluster_version: self.cluster_version.unwrap_or_default(),
            connects,
        })
    }

    /// Build the state
    pub(super) async fn build(self) -> Result<State, tonic::transport::Error> {
        let connects: HashMap<_, _> = rpc::connects(self.all_members.clone()).await?.collect();
        Ok(State {
            leader: self.leader_state.map(|state| state.0),
            local_server: None,
            term: self.leader_state.map_or(0, |state| state.1),
            cluster_version: self.cluster_version.unwrap_or_default(),
            connects,
        })
    }
}
