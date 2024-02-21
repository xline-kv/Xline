use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{atomic::AtomicU64, Arc},
};

use event_listener::Event;
use futures::{stream::FuturesUnordered, Future};
use tokio::sync::RwLock;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing::{debug, info};
#[cfg(madsim)]
use utils::ClientTlsConfig;

use crate::{
    members::ServerId,
    rpc::{
        self,
        connect::{BypassedConnect, ConnectApi},
        CurpError, FetchClusterResponse, Protocol,
    },
};

/// The client state
#[derive(Debug)]
pub(super) struct State {
    /// Mutable state
    mutable: RwLock<StateMut>,
    /// Immutable state
    immutable: StateStatic,
    /// The client id. Separated from `mutable` because the client ID will be updated in the background.
    client_id: Arc<AtomicU64>,
}

/// Immutable client state, could be cloned
#[derive(Debug, Clone)]
struct StateStatic {
    /// is current client send request to raw curp server
    is_raw_curp: bool,
    /// Local server id, should be initialized on startup
    local_server: Option<ServerId>,
    /// Notifier of leader update
    leader_notifier: Arc<Event>,
    /// Client tls config
    tls_config: Option<ClientTlsConfig>,
}

/// Mutable client state
struct StateMut {
    /// Leader id. At the beginning, we may not know who the leader is.
    leader: Option<ServerId>,
    /// Term, initialize to 0, calibrated by the server.
    term: u64,
    /// Cluster version, initialize to 0, calibrated by the server.
    cluster_version: u64,
    /// Members' connect, calibrated by the server.
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
}

impl std::fmt::Debug for StateMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("leader", &self.leader)
            .field("term", &self.term)
            .field("cluster_version", &self.cluster_version)
            .field("connects", &self.connects.keys())
            .finish()
    }
}

impl State {
    /// For test
    #[cfg(test)]
    pub(super) fn new_arc(
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
        local_server: Option<ServerId>,
        leader: Option<ServerId>,
        term: u64,
        cluster_version: u64,
        tls_config: Option<ClientTlsConfig>,
    ) -> Arc<Self> {
        Arc::new(Self {
            mutable: RwLock::new(StateMut {
                leader,
                term,
                cluster_version,
                connects,
            }),
            immutable: StateStatic {
                local_server,
                leader_notifier: Arc::new(Event::new()),
                tls_config,
                is_raw_curp: true,
            },
            client_id: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Get the leader notifier
    pub(super) fn leader_notifier(&self) -> &Event {
        &self.immutable.leader_notifier
    }

    /// Clone a reference to client id
    pub(super) fn clone_client_id(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.client_id)
    }

    /// Get the client id
    pub(super) fn client_id(&self) -> u64 {
        self.client_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Generate client id if it does not exist when it is the leader
    pub(crate) async fn check_gen_local_client_id(&self) {
        let local_server_id = self.immutable.local_server;
        let leader_id = self.leader_id().await;
        if local_server_id != leader_id {
            return;
        }
        if self.client_id.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            let id = rand::random();
            self.client_id
                .store(id, std::sync::atomic::Ordering::Relaxed);
            info!("generate client id({id}) locally for bypassed client");
        }
    }

    /// Get the local server connection
    pub(super) async fn local_connect(&self) -> Option<Arc<dyn ConnectApi>> {
        let id = self.immutable.local_server?;
        self.mutable.read().await.connects.get(&id).map(Arc::clone)
    }

    /// Get the local server id
    pub(super) fn local_server_id(&self) -> Option<ServerId> {
        self.immutable.local_server
    }

    /// Get the cluster version
    pub(super) async fn cluster_version(&self) -> u64 {
        self.mutable.read().await.cluster_version
    }

    /// Get the cached leader id
    pub(super) async fn leader_id(&self) -> Option<ServerId> {
        self.mutable.read().await.leader
    }

    /// Take an async function and map to the dedicated server, return `Err(CurpError:WrongClusterVersion(()))`
    /// if the server can not found in local state
    pub(super) async fn map_server<R, F: Future<Output = Result<R, CurpError>>>(
        &self,
        id: ServerId,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> Result<R, CurpError> {
        let conn = {
            // If the leader id cannot be found in connects, it indicates that there is
            // an inconsistency between the client's local leader state and the cluster
            // state, then mock a `WrongClusterVersion` return to the outside.
            self.mutable
                .read()
                .await
                .connects
                .get(&id)
                .map(Arc::clone)
                .ok_or_else(CurpError::wrong_cluster_version)?
        };
        f(conn).await
    }

    /// Take an async function and map to all server, returning `FuturesUnordered<F>`
    pub(super) async fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> FuturesUnordered<F> {
        self.mutable
            .read()
            .await
            .connects
            .values()
            .map(Arc::clone)
            .map(f)
            .collect()
    }

    /// Inner check and update leader
    fn check_and_update_leader_inner(
        &self,
        state: &mut StateMut,
        leader_id: Option<ServerId>,
        term: u64,
    ) -> bool {
        match state.term.cmp(&term) {
            Ordering::Less => {
                // reset term only when the resp has leader id to prevent:
                // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                if let Some(new_leader_id) = leader_id {
                    info!("client term updates to {term}, client leader id updates to {new_leader_id}");
                    state.term = term;
                    state.leader = Some(new_leader_id);
                    self.immutable.leader_notifier.notify(usize::MAX);
                }
            }
            Ordering::Equal => {
                if let Some(new_leader_id) = leader_id {
                    if state.leader.is_none() {
                        info!("client leader id updates to {new_leader_id}");
                        state.leader = Some(new_leader_id);
                        self.immutable.leader_notifier.notify(usize::MAX);
                    }
                    assert_eq!(
                        state.leader,
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

    /// Update leader
    pub(super) async fn check_and_update_leader(
        &self,
        leader_id: Option<ServerId>,
        term: u64,
    ) -> bool {
        let mut state = self.mutable.write().await;
        self.check_and_update_leader_inner(&mut state, leader_id, term)
    }

    /// Update client state based on [`FetchClusterResponse`]
    pub(super) async fn check_and_update(
        &self,
        res: &FetchClusterResponse,
    ) -> Result<(), tonic::transport::Error> {
        let mut state = self.mutable.write().await;
        if !self.check_and_update_leader_inner(&mut state, res.leader_id, res.term) {
            return Ok(());
        }
        if state.cluster_version == res.cluster_version {
            debug!(
                "ignore cluster version({}) from server",
                res.cluster_version
            );
            return Ok(());
        }

        info!("client cluster version updated to {}", res.cluster_version);
        state.cluster_version = res.cluster_version;

        let mut new_members = if self.immutable.is_raw_curp {
            res.clone().into_peer_urls()
        } else {
            res.clone().into_client_urls()
        };
        let old_ids = state.connects.keys().copied().collect::<HashSet<_>>();
        let new_ids = new_members.keys().copied().collect::<HashSet<_>>();

        let diffs = &old_ids ^ &new_ids;
        let sames = &old_ids & &new_ids;

        for diff in diffs {
            if let Entry::Vacant(e) = state.connects.entry(diff) {
                let addrs = new_members
                    .remove(&diff)
                    .unwrap_or_else(|| unreachable!("{diff} must in new member addrs"));
                debug!("client connects to a new server({diff}), address({addrs:?})");
                let new_conn = rpc::connect(diff, addrs, self.immutable.tls_config.clone()).await?;
                let _ig = e.insert(new_conn);
            } else {
                debug!("client removes old server({diff})");
                let _ig = state.connects.remove(&diff);
            }
        }
        for same in sames {
            let conn = state
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
    /// Client Tls config
    tls_config: Option<ClientTlsConfig>,
    /// is current client send request to raw curp server
    is_raw_curp: bool,
}

impl StateBuilder {
    /// Create a state builder
    pub(super) fn new(
        all_members: HashMap<ServerId, Vec<String>>,
        tls_config: Option<ClientTlsConfig>,
    ) -> Self {
        Self {
            all_members,
            leader_state: None,
            cluster_version: None,
            tls_config,
            is_raw_curp: false,
        }
    }

    /// Set is raw curp
    pub(super) fn set_is_raw_curp(&mut self, is_raw_curp: bool) {
        self.is_raw_curp = is_raw_curp;
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
        let mut connects: HashMap<_, _> =
            rpc::connects(self.all_members.clone(), self.tls_config.as_ref())
                .await?
                .collect();
        let __ig = connects.insert(
            local_server_id,
            Arc::new(BypassedConnect::new(local_server_id, local_server)),
        );

        Ok(State {
            mutable: RwLock::new(StateMut {
                leader: self.leader_state.map(|state| state.0),
                term: self.leader_state.map_or(0, |state| state.1),
                cluster_version: self.cluster_version.unwrap_or_default(),
                connects,
            }),
            immutable: StateStatic {
                local_server: Some(local_server_id),
                leader_notifier: Arc::new(Event::new()),
                tls_config: self.tls_config.take(),
                is_raw_curp: self.is_raw_curp,
            },
            client_id: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Build the state
    pub(super) async fn build(self) -> Result<State, tonic::transport::Error> {
        let connects: HashMap<_, _> =
            rpc::connects(self.all_members.clone(), self.tls_config.as_ref())
                .await?
                .collect();
        Ok(State {
            mutable: RwLock::new(StateMut {
                leader: self.leader_state.map(|state| state.0),
                term: self.leader_state.map_or(0, |state| state.1),
                cluster_version: self.cluster_version.unwrap_or_default(),
                connects,
            }),
            immutable: StateStatic {
                local_server: None,
                leader_notifier: Arc::new(Event::new()),
                tls_config: self.tls_config,
                is_raw_curp: self.is_raw_curp,
            },
            client_id: Arc::new(AtomicU64::new(0)),
        })
    }
}
