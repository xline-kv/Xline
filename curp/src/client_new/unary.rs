use std::{
    cmp::Ordering,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use dashmap::DashMap;
use event_listener::Event;
use futures::{stream::FuturesUnordered, Future, Stream, StreamExt};
use itertools::Itertools;
use parking_lot::RwLock;
use tonic::Response;
use tracing::{debug, warn};
use utils::config::ClientConfig;

use crate::{
    members::ServerId,
    rpc::{
        connect::{BypassedConnect, ConnectApi},
        ConfChange, FetchClusterRequest, FetchClusterResponse, Member, Protocol, ReadState,
    },
};

use super::{errors::*, ClientApi};

/// Leader state of a client
#[derive(Debug, Default)]
struct LeaderState {
    /// Current leader
    leader: Option<ServerId>,
    /// Current term
    term: u64,
    /// When a new leader is set, notify
    leader_notify: Arc<Event>,
}

/// Unary send rpc only once
pub(super) struct Unary {
    cluster_version: AtomicU64,
    config: ClientConfig,
    connects: DashMap<ServerId, Arc<dyn ConnectApi>>,
    leader_state: RwLock<LeaderState>,
    local_server_id: Option<ServerId>,
}

impl Unary {
    /// Get cluster version
    fn cluster_version(&self) -> u64 {
        self.cluster_version
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bypass a server by providing a Protocol implementation, see [crate::server::Rpc]
    fn bypass<P: Protocol>(&self, id: ServerId, server: P) {
        self.connects
            .insert(id, Arc::new(BypassedConnect::new(id, server)));
    }

    /// Get the super quorum
    fn super_quorum(&self) -> usize {
        let nodes = self.connects.len();
        let fault_tolerance = nodes.wrapping_div(2);
        fault_tolerance
            .wrapping_add(fault_tolerance.wrapping_add(1).wrapping_div(2))
            .wrapping_add(1)
    }

    /// Get the majority quorum
    fn majority_quorum(&self) -> usize {
        let nodes = self.connects.len();
        nodes.wrapping_div(2).wrapping_add(1)
    }

    /// Give a handle `f` to apply to all servers concurrently and return a stream to poll result one by one.
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> impl Stream<Item = R> {
        let connects = self
            .connects
            .iter()
            .map(|connect| Arc::clone(&connect))
            .collect_vec();
        connects.into_iter().map(f).collect::<FuturesUnordered<F>>()
    }

    /// Get a handle `f` and return the future to apply `f` on the leader
    /// NOTICE: for_leader should never be invoked in [ClientApi::fetch_cluster]
    /// `for_leader` might call `fetch_leader_id`, `fetch_leader_id` might call
    /// `fetch_cluster`, finally result in stack over flow
    async fn for_leader<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> Result<R, ()> {
        let leader_id = match self.leader_state.read().leader {
            Some(id) => id,
            None => self.fetch_leader_id(false).await?,
        };
        let connect = self
            .connects
            .get(&leader_id)
            .unwrap_or_else(|| unreachable!("leader_id {leader_id} does not contains in connects"));
        let res = f(Arc::clone(&connect)).await;
        Ok(res)
    }
}

#[async_trait]
impl<C: Command> ClientApi<C> for Unary {
    /// Get the local connection when the client is on the server node.
    fn local_connect(&self) -> Option<Arc<dyn ConnectApi>> {
        self.local_server_id
            .and_then(|id| self.connects.get(&id).map(|connect| Arc::clone(&connect)))
    }

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: C,
        use_fast_path: bool,
    ) -> Result<(C::ER, Option<C::ASR>), ProposeCmdError> {
        Err(ProposeCmdError {})
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, ProposeConfChangeError> {
        Err(ProposeConfChangeError {})
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), ProposeShutdownError> {
        Err(ProposeShutdownError {})
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, FetchReadStateError> {
        Err(FetchReadStateError {})
    }

    /// Send fetch cluster requests to all servers
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(
        &self,
        linearizable: bool,
    ) -> Result<FetchClusterResponse, FetchClusterError> {
        let timeout = *self.config.propose_timeout();
        if !linearizable {
            // firstly, try to fetch the local server
            if let Some(connect) = <Unary as ClientApi<C>>::local_connect(self) {
                /// Fake timeout, in fact, local connect should only be bypassed, so the timeout is unused.
                const FETCH_TIMEOUT: Duration = Duration::from_secs(1);

                let resp = connect
                    .fetch_cluster(FetchClusterRequest::default(), FETCH_TIMEOUT)
                    .await
                    .unwrap_or_else(|e| {
                        unreachable!(
                            "fetch cluster from local connect should never failed, err {e}"
                        )
                    })
                    .into_inner();
                return Ok(resp);
            }
        }
        // then fetch the whole cluster
        let mut responses = self.for_each_server(|conn| async move {
            (
                conn.id(),
                conn.fetch_cluster(FetchClusterRequest { linearizable }, timeout)
                    .await
                    .map(Response::into_inner),
            )
        });

        let mut max_term = 0;
        let mut res = None;
        let mut ok_cnt = 0;

        while let Some((id, resp)) = responses.next().await {
            let inner = match resp {
                Ok(r) => r,
                Err(e) => {
                    warn!("fetch cluster from {} failed, {:?}", id, e);
                    continue;
                }
            };

            #[allow(clippy::integer_arithmetic)]
            match max_term.cmp(&inner.term) {
                Ordering::Less => {
                    max_term = inner.term;
                    if !inner.members.is_empty() {
                        res = Some(inner);
                    }
                    ok_cnt = 1;
                }
                Ordering::Equal => {
                    if !inner.members.is_empty() {
                        res = Some(inner);
                    }
                    ok_cnt += 1;
                }
                Ordering::Greater => {}
            }

            if ok_cnt >= self.majority_quorum() {
                break;
            }
        }

        if let Some(res) = res {
            let mut state = self.leader_state.write();
            debug!("Fetch cluster succeeded, result: {res:?}");
            state.check_and_update(res.leader_id, res.term);
            return Ok(res);
        }

        Err(FetchClusterError {})
    }
}
