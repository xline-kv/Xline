#![allow(unused)] // TODO: remove

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    ops::AddAssign,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, Future, Stream, StreamExt};
use itertools::Itertools;
use parking_lot::RwLock;
use tonic::Response;
use tracing::{debug, warn};

use crate::{
    members::ServerId,
    rpc::{
        self,
        connect::{BypassedConnect, ConnectApi},
        ConfChange, CurpError, FetchClusterRequest, FetchClusterResponse, FetchReadStateRequest,
        Member, ProposeConfChangeRequest, ProposeId, ProposeRequest, Protocol, ReadState,
        ShutdownRequest, WaitSyncedRequest,
    },
};

use super::{retry::LeaderStateUpdate, ClientApi, ProposeResponse};

/// Leader state of a client
#[derive(Debug, Default)]
struct LeaderState {
    /// Current leader
    leader: Option<ServerId>,
    /// Current term
    term: u64,
}

impl LeaderState {
    /// Check the term and leader id, update the state if needed.
    fn check_and_update(&mut self, leader_id: Option<u64>, term: u64) {
        match self.term.cmp(&term) {
            Ordering::Less => {
                // reset term only when the resp has leader id to prevent:
                // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                if let Some(new_leader_id) = leader_id {
                    debug!("client term updates to {term}");
                    self.term = term;
                    debug!("client leader id updates to {new_leader_id}");
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
                debug!("ignore old term({term}) from server");
            }
        }
    }
}

/// Cluster state of a client, thread safe
struct ClusterState {
    /// Cluster version
    version: AtomicU64,
    /// Cluster connects
    connects: DashMap<ServerId, Arc<dyn ConnectApi>>,
}

impl Debug for ClusterState {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterState")
            .field("version", &self.version)
            .field(
                "connects",
                &self.connects.iter().map(|v| *v.key()).collect_vec(),
            )
            .finish()
    }
}

impl ClusterState {
    /// Check the cluster version and update the connects if needed.
    async fn check_and_update(
        &self,
        cluster_version: u64,
        member_addrs: HashMap<ServerId, Vec<String>>,
    ) -> Result<(), tonic::transport::Error> {
        // Optimistic lock to keep there is only one thread `check_and_update`
        // FIXME: is these ordering options correct?
        let update = self.version.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |cur| (cluster_version > cur).then_some(cluster_version),
        );
        if update.is_err() {
            return Ok(());
        }
        // TODO: if the above code can ensure that only one thread enters,
        // can `DashMap` be replaced with some unsafe codes to improve performance?
        let old_ids = self
            .connects
            .iter()
            .map(|c| *c.key())
            .collect::<HashSet<_>>();
        let new_ids = member_addrs.keys().copied().collect::<HashSet<_>>();
        let diffs = &old_ids ^ &new_ids;
        for diff in diffs {
            if self.connects.contains_key(&diff) {
                debug!("client remove old server {diff}");
                let _ig = self.connects.remove(&diff);
            } else {
                let addrs = member_addrs
                    .get(&diff)
                    .unwrap_or_else(|| unreachable!("{diff} must in new member addrs"))
                    .clone();
                let new_conn = rpc::connect(diff, addrs).await?;
                let _ig = self.connects.insert(diff, new_conn);
            }
        }
        let sames = &old_ids & &new_ids;
        for same in sames {
            let conn = self
                .connects
                .get(&same)
                .unwrap_or_else(|| unreachable!("{same} must in old connects"));
            let addrs = member_addrs
                .get(&same)
                .unwrap_or_else(|| unreachable!("{same} must in new member addrs"))
                .clone();
            conn.update_addrs(addrs).await?;
        }
        Ok(())
    }
}

/// The unary client config
#[derive(Debug)]
pub(super) struct UnaryConfig {
    /// The rpc timeout of a propose request
    propose_timeout: Duration,
    /// The rpc timeout of a 2-RTT request, usually takes longer than propose timeout
    /// Default to 2 * propose_timeout, The recommended the values is within
    /// (propose_timeout, 2 * propose_timeout].
    wait_synced_timeout: Duration,
}

impl UnaryConfig {
    /// Create a unary config
    fn new(propose_timeout: Duration) -> Self {
        Self {
            propose_timeout,
            wait_synced_timeout: propose_timeout * 2,
        }
    }

    /// Create a unary config
    fn new_full(propose_timeout: Duration, wait_synced_timeout: Duration) -> Self {
        Self {
            propose_timeout,
            wait_synced_timeout,
        }
    }
}

/// The unary client send rpc only once
#[derive(Debug)]
pub(super) struct Unary<C: Command> {
    /// Cluster state
    cluster_state: ClusterState,
    /// Leader state
    leader_state: RwLock<LeaderState>,
    /// Local server id
    local_server_id: Option<ServerId>,
    /// Unary config
    config: UnaryConfig,
    /// marker
    phantom: PhantomData<C>,
}

impl<C: Command> Unary<C> {
    /// Get cluster version.
    fn cluster_version(&self) -> u64 {
        self.cluster_state
            .version
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bypass a server by providing a Protocol implementation, see [`crate::server::Rpc`].
    fn bypass<P: Protocol>(&self, id: ServerId, server: P) {
        let _ig = self
            .cluster_state
            .connects
            .insert(id, Arc::new(BypassedConnect::new(id, server)));
    }

    /// Give a handle `f` to apply to all servers *concurrently* and return a stream to poll result one by one.
    fn for_each_server<R, F: Future<Output = R>>(
        &self,
        f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> (usize, impl Stream<Item = R>) {
        let connects = self
            .cluster_state
            .connects
            .iter()
            .map(|connect| Arc::clone(&connect))
            .collect_vec();
        (
            connects.len(), // quorum calculated here to keep quorum = stream.len()
            connects.into_iter().map(f).collect::<FuturesUnordered<F>>(),
        )
    }

    /// Get a handle `f` and return the future to apply `f` on the leader.
    /// NOTICE:
    /// The leader might be outdate if the local `leader_state` is stale.
    /// `for_leader` should never be invoked in [`ClientApi::fetch_cluster`]
    /// `for_leader` might call `fetch_leader_id`, `fetch_leader_id` might call
    /// `fetch_cluster`, finally result in stack over flow.
    async fn for_leader<R, F: Future<Output = R>>(
        &self,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> Result<R, CurpError> {
        let cached_leader = self.leader_state.read().leader;
        let leader_id = match cached_leader {
            Some(id) => id,
            None => <Unary<C> as ClientApi<C>>::fetch_leader_id(self, false).await?,
        };
        // If the leader id cannot be found in connects, it indicates that there is
        // an inconsistency between the client's local leader state and the cluster
        // state, then mock a `WrongClusterVersion` return to the outside.
        let connect = self
            .cluster_state
            .connects
            .get(&leader_id)
            .ok_or_else(CurpError::wrong_cluster_version)?;
        let res = f(Arc::clone(&connect)).await;
        Ok(res)
    }

    /// Send proposal to all servers
    async fn fast_round(
        &self,
        propose_id: ProposeId,
        cmd: &C,
    ) -> Result<Result<C::ER, C::Error>, CurpError> {
        let req = ProposeRequest::new(propose_id, cmd, self.cluster_version());
        let timeout = self.config.propose_timeout;

        let (quorum, mut responses) = self.for_each_server(|conn| {
            let req_c = req.clone();
            async move { (conn.id(), conn.propose(req_c, timeout).await) }
        });
        let super_quorum = super_quorum(quorum);

        let mut err = None;
        let mut execute_result: Option<C::ER> = None;
        let mut ok_cnt = 0;

        while let Some((id, resp)) = responses.next().await {
            let resp = match resp {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("propose cmd({propose_id}) to server({id}) error: {e:?}");
                    if e.return_early() {
                        return Err(e);
                    }
                    err = Some(e);
                    continue;
                }
            };
            let deserialize_res = resp.map_result::<C, _, Result<(), C::Error>>(|res| {
                let er = match res {
                    Ok(er) => er,
                    Err(cmd_err) => return Err(cmd_err),
                };
                if let Some(er) = er {
                    assert!(execute_result.is_none(), "should not set exe result twice");
                    execute_result = Some(er);
                }
                ok_cnt.add_assign(1);
                Ok(())
            });
            let dr = match deserialize_res {
                Ok(dr) => dr,
                Err(ser_err) => {
                    warn!("serialize error: {ser_err}");
                    // We blame this error to the server, although it may be a local error.
                    // We need to retry as same as a server error.
                    err = Some(CurpError::from(ser_err));
                    continue;
                }
            };
            if let Err(cmd_err) = dr {
                // got a command execution error early, abort the next requests and return the cmd error
                return Ok(Err(cmd_err));
            }
            // if the propose meets the super quorum and we got the execute result,
            // that means we can safely abort the next requests
            if ok_cnt >= super_quorum {
                if let Some(er) = execute_result {
                    debug!("fast round for cmd({}) succeed", propose_id);
                    return Ok(Ok(er));
                }
            }
        }

        if let Some(err) = err {
            return Err(err);
        }

        // We will at least send the request to the leader if no `WrongClusterVersion` returned.
        // If no errors occur, the leader should return the ER
        // If it is because the super quorum has not been reached, an error will definitely occur.
        unreachable!("leader should return ER if no error happens");
    }

    /// Wait synced result from server
    async fn slow_round(
        &self,
        propose_id: ProposeId,
    ) -> Result<Result<(C::ASR, C::ER), C::Error>, CurpError> {
        let timeout = self.config.wait_synced_timeout;
        let req = WaitSyncedRequest::new(propose_id, self.cluster_version());
        let resp = self
            .for_leader(|conn| async move { conn.wait_synced(req, timeout).await })
            .await??
            .into_inner();
        let synced_res = resp.map_result::<C, _, _>(|res| res).map_err(|ser_err| {
            warn!("serialize error: {ser_err}");
            // Same as fast round, we blame the server for the serializing error.
            CurpError::from(ser_err)
        })?;
        Ok(synced_res)
    }

    /// Get the client id
    #[allow(clippy::unused_async)] // TODO: grant a client id from server
    async fn get_client_id(&self) -> Result<u64, CurpError> {
        Ok(rand::random())
    }

    /// New a seq num and record it
    #[allow(clippy::unused_self)] // TODO: implement request tracker
    fn new_seq_num(&self) -> u64 {
        rand::random()
    }

    /// Generate a new propose id
    async fn gen_propose_id(&self) -> Result<ProposeId, CurpError> {
        let client_id = self.get_client_id().await?;
        let seq_num = self.new_seq_num();
        Ok(ProposeId(client_id, seq_num))
    }
}

#[async_trait]
impl<C: Command> ClientApi<C> for Unary<C> {
    /// The error is generated from server
    type Error = CurpError;

    /// Get the local connection when the client is on the server node.
    fn local_connect(&self) -> Option<Arc<dyn ConnectApi>> {
        self.local_server_id.and_then(|id| {
            self.cluster_state
                .connects
                .get(&id)
                .map(|connect| Arc::clone(&connect))
        })
    }

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(&self, cmd: &C, use_fast_path: bool) -> Result<ProposeResponse<C>, CurpError> {
        let propose_id = self.gen_propose_id().await?;

        tokio::pin! {
            let fast_round = self.fast_round(propose_id, cmd);
            let slow_round = self.slow_round(propose_id);
        }

        let res: ProposeResponse<C> = if use_fast_path {
            match futures::future::select(fast_round, slow_round).await {
                futures::future::Either::Left((fast_result, slow_round)) => match fast_result {
                    Ok(er) => er.map(|e| (e, None)),
                    Err(err) => {
                        if err.return_early() {
                            return Err(err);
                        }
                        // fallback to slow round if fast round failed
                        let sr = slow_round.await?;
                        sr.map(|(asr, er)| (er, Some(asr)))
                    }
                },
                futures::future::Either::Right((slow_result, fast_round)) => match slow_result {
                    Ok(er) => er.map(|(asr, e)| (e, Some(asr))),
                    Err(err) => {
                        if err.return_early() {
                            return Err(err);
                        }
                        let fr = fast_round.await?;
                        fr.map(|er| (er, None))
                    }
                },
            }
        } else {
            let (fr, sr) = futures::future::join(fast_round, slow_round).await;
            if let Err(err) = fr {
                if err.return_early() {
                    return Err(err);
                }
            }
            sr?.map(|(asr, er)| (er, Some(asr)))
        };

        Ok(res)
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, CurpError> {
        let propose_id = self.gen_propose_id().await?;
        let req = ProposeConfChangeRequest::new(propose_id, changes, self.cluster_version());
        let timeout = self.config.wait_synced_timeout;
        let members = self
            .for_leader(|conn| async move { conn.propose_conf_change(req, timeout).await })
            .await??
            .into_inner()
            .members;
        Ok(members)
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), CurpError> {
        let propose_id = self.gen_propose_id().await?;
        let req = ShutdownRequest::new(propose_id, self.cluster_version());
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .for_leader(|conn| async move { conn.shutdown(req, timeout).await })
            .await??;
        Ok(())
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, CurpError> {
        // Same as fast_round, we blame the serializing error to the server even
        // thought it is the local error
        let req = FetchReadStateRequest::new(cmd, self.cluster_version()).map_err(|ser_err| {
            warn!("serializing error: {ser_err}");
            CurpError::from(ser_err)
        })?;
        let timeout = self.config.wait_synced_timeout;
        let state = self
            .for_leader(|conn| async move { conn.fetch_read_state(req, timeout).await })
            .await??
            .into_inner()
            .read_state
            .unwrap_or_else(|| unreachable!("read_state must be set in fetch read state response"));
        Ok(state)
    }

    /// Send fetch cluster requests to all servers
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<FetchClusterResponse, CurpError> {
        let timeout = self.config.wait_synced_timeout;
        if !linearizable {
            // firstly, try to fetch the local server
            if let Some(connect) = <Unary<C> as ClientApi<C>>::local_connect(self) {
                /// local timeout, in fact, local connect should only be bypassed, so the timeout maybe unused.
                const FETCH_LOCAL_TIMEOUT: Duration = Duration::from_secs(1);

                let resp = connect
                    .fetch_cluster(FetchClusterRequest::default(), FETCH_LOCAL_TIMEOUT)
                    .await
                    .unwrap_or_else(|e| {
                        unreachable!(
                            "fetch cluster from local connect should never failed, err {e:?}"
                        )
                    })
                    .into_inner();
                return Ok(resp);
            }
        }
        // then fetch the whole cluster
        let (quorum, mut responses) = self.for_each_server(|conn| async move {
            (
                conn.id(),
                conn.fetch_cluster(FetchClusterRequest { linearizable }, timeout)
                    .await
                    .map(Response::into_inner),
            )
        });
        let majority_quorum = majority_quorum(quorum);

        let mut max_term = 0;
        let mut res = None;
        let mut ok_cnt = 0;
        let mut err = None;

        while let Some((id, resp)) = responses.next().await {
            let inner = match resp {
                Ok(r) => r,
                Err(e) => {
                    warn!("fetch cluster from {} failed, {:?}", id, e);
                    if e.return_early() {
                        return Err(e);
                    }
                    err = Some(e);
                    continue;
                }
            };

            #[allow(clippy::integer_arithmetic)]
            match max_term.cmp(&inner.term) {
                Ordering::Less => {
                    if !inner.members.is_empty() {
                        max_term = inner.term;
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

            if ok_cnt >= majority_quorum {
                break;
            }
        }
        if let Some(res) = res {
            debug!("fetch cluster succeeded, result: {res:?}");
            if let Err(e) = self
                .cluster_state
                .check_and_update(res.cluster_version, res.clone().into_members_addrs())
                .await
            {
                warn!("update to a new cluster state failed, error {e}");
            } else {
                // FIXME: The updates of leader and cluster states are not atomic,
                // which may result in the leader id not existing in the members
                // for a while.
                self.leader_state
                    .write()
                    .check_and_update(res.leader_id, res.term);
            }
            return Ok(res);
        }
        if let Some(err) = err {
            return Err(err);
        }

        unreachable!("At least one server will return `members` or a connection error has occurred. Leaders should not return empty members.")
    }
}

impl<C: Command> LeaderStateUpdate for Unary<C> {
    /// Update leader
    fn update_leader(&self, leader_id: Option<ServerId>, term: u64) {
        self.leader_state.write().check_and_update(leader_id, term);
    }
}

/// Calculate the super quorum
fn super_quorum(quorum: usize) -> usize {
    let fault_tolerance = quorum.wrapping_div(2);
    fault_tolerance
        .wrapping_add(fault_tolerance.wrapping_add(1).wrapping_div(2))
        .wrapping_add(1)
}

/// Calculate the majority quorum
fn majority_quorum(quorum: usize) -> usize {
    quorum.wrapping_div(2).wrapping_add(1)
}
