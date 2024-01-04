use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap, HashSet},
    marker::PhantomData,
    ops::AddAssign,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::{stream::FuturesUnordered, Future, Stream, StreamExt};
use tokio::sync::RwLock;
use tonic::Response;
use tracing::{debug, warn};

use crate::{
    members::ServerId,
    rpc::{
        self,
        connect::{BypassedConnect, ConnectApi},
        ConfChange, CurpError, CurpErrorPriority, FetchClusterRequest, FetchClusterResponse,
        FetchReadStateRequest, Member, ProposeConfChangeRequest, ProposeId, ProposeRequest,
        Protocol, PublishRequest, ReadState, ShutdownRequest, WaitSyncedRequest,
    },
};

use super::{ClientApi, LeaderStateUpdate, ProposeResponse};

/// Client state
struct State {
    /// Leader id. At the beginning, we may not know who the leader is.
    leader: Option<ServerId>,
    /// Term, initialize to 0, calibrated by the server.
    term: u64,
    /// Cluster version, initialize to 0, calibrated by the server.
    cluster_version: u64,
    /// Members' connect
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateInner")
            .field("leader", &self.leader)
            .field("term", &self.term)
            .field("cluster_version", &self.cluster_version)
            .field("connects", &self.connects.keys())
            .finish()
    }
}

/// Unary builder
pub(super) struct UnaryBuilder {
    /// All members (required)
    all_members: HashMap<ServerId, Vec<String>>,
    /// Unary config (required)
    config: UnaryConfig,
    /// Leader state (optional)
    leader_state: Option<(ServerId, u64)>,
    /// Cluster version (optional)
    cluster_version: Option<u64>,
}

impl UnaryBuilder {
    /// Create unary builder
    pub(super) fn new(all_members: HashMap<ServerId, Vec<String>>, config: UnaryConfig) -> Self {
        Self {
            all_members,
            config,
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

    /// Inner build
    fn build_with_connects<C: Command>(
        self,
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
        local_server_id: Option<ServerId>,
    ) -> Unary<C> {
        let state = State {
            leader: self.leader_state.map(|state| state.0),
            term: self.leader_state.map_or(0, |state| state.1),
            cluster_version: self.cluster_version.unwrap_or_default(),
            connects,
        };
        Unary {
            state: RwLock::new(state),
            local_server_id,
            config: self.config,
            phantom: PhantomData,
        }
    }

    /// Build the unary client with local server
    pub(super) async fn build_bypassed<C: Command, P: Protocol>(
        mut self,
        local_server_id: ServerId,
        local_server: P,
    ) -> Result<Unary<C>, tonic::transport::Error> {
        debug!("client bypassed server({local_server_id})");

        let _ig = self.all_members.remove(&local_server_id);
        let mut connects: HashMap<_, _> = rpc::connects(self.all_members.clone()).await?.collect();
        let __ig = connects.insert(
            local_server_id,
            Arc::new(BypassedConnect::new(local_server_id, local_server)),
        );

        Ok(self.build_with_connects(connects, Some(local_server_id)))
    }

    /// Build the unary client
    pub(super) async fn build<C: Command>(self) -> Result<Unary<C>, tonic::transport::Error> {
        let connects: HashMap<_, _> = rpc::connects(self.all_members.clone()).await?.collect();
        Ok(self.build_with_connects(connects, None))
    }
}

/// The unary client config
#[derive(Debug)]
pub(super) struct UnaryConfig {
    /// The rpc timeout of a propose request
    propose_timeout: Duration,
    /// The rpc timeout of a 2-RTT request, usually takes longer than propose timeout
    /// The recommended the values is within (propose_timeout, 2 * propose_timeout].
    wait_synced_timeout: Duration,
}

impl UnaryConfig {
    /// Create a unary config
    pub(super) fn new(propose_timeout: Duration, wait_synced_timeout: Duration) -> Self {
        Self {
            propose_timeout,
            wait_synced_timeout,
        }
    }
}

/// The unary client
#[derive(Debug)]
pub(super) struct Unary<C: Command> {
    /// Client state
    state: RwLock<State>,
    /// Local server id
    local_server_id: Option<ServerId>,
    /// Unary config
    config: UnaryConfig,
    /// marker
    phantom: PhantomData<C>,
}

impl<C: Command> Unary<C> {
    /// Used in tests, all connects are mocked
    #[cfg(test)]
    pub(super) fn new(
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
        local_server_id: Option<ServerId>,
        leader_id: Option<ServerId>,
        term: Option<u64>,
    ) -> Self {
        Self {
            state: RwLock::new(State {
                leader: leader_id,
                term: term.unwrap_or_default(),
                cluster_version: 0,
                connects,
            }),
            local_server_id,
            config: UnaryConfig {
                propose_timeout: Duration::from_secs(0),
                wait_synced_timeout: Duration::from_secs(0),
            },
            phantom: PhantomData,
        }
    }

    /// Update leader
    fn check_and_update_leader(state: &mut State, leader_id: Option<ServerId>, term: u64) -> bool {
        match state.term.cmp(&term) {
            Ordering::Less => {
                // reset term only when the resp has leader id to prevent:
                // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                if let Some(new_leader_id) = leader_id {
                    debug!("client term updates to {}", term);
                    debug!("client leader id updates to {new_leader_id}");
                    state.term = term;
                    state.leader = Some(new_leader_id);
                }
            }
            Ordering::Equal => {
                if let Some(new_leader_id) = leader_id {
                    if state.leader.is_none() {
                        debug!("client leader id updates to {new_leader_id}");
                        state.leader = Some(new_leader_id);
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

    /// Update client state based on [`FetchClusterResponse`]
    async fn check_and_update(
        &self,
        res: &FetchClusterResponse,
    ) -> Result<(), tonic::transport::Error> {
        let mut state = self.state.write().await;
        if !Self::check_and_update_leader(&mut state, res.leader_id, res.term) {
            return Ok(());
        }
        if state.cluster_version == res.cluster_version {
            debug!(
                "ignore cluster version({}) from server",
                res.cluster_version
            );
            return Ok(());
        }

        debug!("client cluster version updated to {}", res.cluster_version);
        state.cluster_version = res.cluster_version;

        let mut new_members = res.clone().into_members_addrs();

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
                let new_conn = rpc::connect(diff, addrs).await?;
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

    /// Get cluster version.
    async fn cluster_version(&self) -> u64 {
        self.state.read().await.cluster_version
    }

    /// Give a handle `f` to apply to all servers *concurrently* and return a stream to poll result one by one.
    async fn for_each_server<R, F: Future<Output = R>>(
        &self,
        mut f: impl FnMut(Arc<dyn ConnectApi>) -> F,
    ) -> (usize, impl Stream<Item = R>) {
        let connects = self
            .state
            .read()
            .await
            .connects
            .values()
            .map(Arc::clone)
            .map(|connect| f(connect))
            .collect::<FuturesUnordered<F>>();
        // size calculated here to keep size = stream.len(), otherwise Non-atomic read operation on the `connects` may result in inconsistency.
        let size = connects.len();
        (size, connects)
    }

    /// Get a handle `f` and return the future to apply `f` on the leader.
    /// NOTICE:
    /// The leader might be outdate if the local `leader_state` is stale.
    /// `map_leader` should never be invoked in [`ClientApi::fetch_cluster`]
    /// `map_leader` might call `fetch_leader_id`, `fetch_cluster`, finally
    /// result in stack overflow.
    async fn map_leader<R, F: Future<Output = R>>(
        &self,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> Result<R, CurpError> {
        let cached_leader = self.state.read().await.leader;
        let leader_id = match cached_leader {
            Some(id) => id,
            None => <Unary<C> as ClientApi>::fetch_leader_id(self, false).await?,
        };
        // If the leader id cannot be found in connects, it indicates that there is
        // an inconsistency between the client's local leader state and the cluster
        // state, then mock a `WrongClusterVersion` return to the outside.
        let connect = self
            .state
            .read()
            .await
            .connects
            .get(&leader_id)
            .map(Arc::clone)
            .ok_or_else(CurpError::wrong_cluster_version)?;
        let res = f(connect).await;
        Ok(res)
    }

    /// Send proposal to all servers
    pub(super) async fn fast_round(
        &self,
        propose_id: ProposeId,
        cmd: &C,
    ) -> Result<Result<C::ER, C::Error>, CurpError> {
        let req = ProposeRequest::new(propose_id, cmd, self.cluster_version().await);
        let timeout = self.config.propose_timeout;

        let (size, mut responses) = self
            .for_each_server(|conn| {
                let req_c = req.clone();
                async move { (conn.id(), conn.propose(req_c, timeout).await) }
            })
            .await;
        let super_quorum = super_quorum(size);

        let mut err: Option<CurpError> = None;
        let mut execute_result: Option<C::ER> = None;
        let mut ok_cnt = 0;

        while let Some((id, resp)) = responses.next().await {
            let resp = match resp {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("propose cmd({propose_id}) to server({id}) error: {e:?}");
                    if e.priority() == CurpErrorPriority::ReturnImmediately {
                        return Err(e);
                    }
                    if let Some(old_err) = err.as_ref() {
                        if old_err.priority() <= e.priority() {
                            err = Some(e);
                        }
                    } else {
                        err = Some(e);
                    }
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
        // Otherwise, there is no leader in the cluster state currently, return wrong cluster version
        // and attempt to retrieve the cluster state again.
        Err(CurpError::wrong_cluster_version())
    }

    /// Wait synced result from server
    pub(super) async fn slow_round(
        &self,
        propose_id: ProposeId,
    ) -> Result<Result<(C::ASR, C::ER), C::Error>, CurpError> {
        let timeout = self.config.wait_synced_timeout;
        let req = WaitSyncedRequest::new(propose_id, self.cluster_version().await);
        let resp = self
            .map_leader(|conn| async move { conn.wait_synced(req, timeout).await })
            .await??
            .into_inner();
        let synced_res = resp.map_result::<C, _, _>(|res| res).map_err(|ser_err| {
            warn!("serialize error: {ser_err}");
            // Same as fast round, we blame the server for the serializing error.
            CurpError::from(ser_err)
        })?;
        debug!("slow round for cmd({}) succeed", propose_id);
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
impl<C: Command> ClientApi for Unary<C> {
    /// The error is generated from server
    type Error = CurpError;

    /// The command type
    type Cmd = C;

    /// Get the local connection when the client is on the server node.
    async fn local_connect(&self) -> Option<Arc<dyn ConnectApi>> {
        let id = self.local_server_id?;
        self.state.read().await.connects.get(&id).map(Arc::clone)
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
                        if err.priority() > CurpErrorPriority::Low {
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
                        if err.priority() > CurpErrorPriority::Low {
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
                if err.priority() > CurpErrorPriority::Low {
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
        let req = ProposeConfChangeRequest::new(propose_id, changes, self.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let members = self
            .map_leader(|conn| async move { conn.propose_conf_change(req, timeout).await })
            .await??
            .into_inner()
            .members;
        Ok(members)
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), CurpError> {
        let propose_id = self.gen_propose_id().await?;
        let req = ShutdownRequest::new(propose_id, self.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.shutdown(req, timeout).await })
            .await??;
        Ok(())
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
    ) -> Result<(), Self::Error> {
        let propose_id = self.gen_propose_id().await?;
        let req = PublishRequest::new(propose_id, node_id, node_name);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.publish(req, timeout).await })
            .await??;
        Ok(())
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, CurpError> {
        // Same as fast_round, we blame the serializing error to the server even
        // thought it is the local error
        let req =
            FetchReadStateRequest::new(cmd, self.cluster_version().await).map_err(|ser_err| {
                warn!("serializing error: {ser_err}");
                CurpError::from(ser_err)
            })?;
        let timeout = self.config.wait_synced_timeout;
        let state = self
            .map_leader(|conn| async move { conn.fetch_read_state(req, timeout).await })
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
            if let Some(connect) = <Unary<C> as ClientApi>::local_connect(self).await {
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
                debug!("fetch local cluster {resp:?}");

                return Ok(resp);
            }
        }
        // then fetch the whole cluster
        let (size, mut responses) = self
            .for_each_server(|conn| async move {
                (
                    conn.id(),
                    conn.fetch_cluster(FetchClusterRequest { linearizable }, timeout)
                        .await
                        .map(Response::into_inner),
                )
            })
            .await;
        let quorum = quorum(size);

        let mut max_term = 0;
        let mut res = None;
        let mut ok_cnt = 0;
        let mut err: Option<CurpError> = None;

        while let Some((id, resp)) = responses.next().await {
            let inner = match resp {
                Ok(r) => r,
                Err(e) => {
                    warn!("fetch cluster from {} failed, {:?}", id, e);
                    if e.priority() == CurpErrorPriority::ReturnImmediately {
                        return Err(e);
                    }
                    if let Some(old_err) = err.as_ref() {
                        if old_err.priority() <= e.priority() {
                            err = Some(e);
                        }
                    } else {
                        err = Some(e);
                    }
                    continue;
                }
            };
            // Ignore the response of a node that doesn't know who the leader is.
            if inner.leader_id.is_some() {
                #[allow(clippy::integer_arithmetic)]
                match max_term.cmp(&inner.term) {
                    Ordering::Less => {
                        max_term = inner.term;
                        if !inner.members.is_empty() {
                            res = Some(inner);
                        }
                        // reset ok count to 1
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
            }
            // first check quorum
            if ok_cnt >= quorum {
                // then check if we got the response
                if let Some(res) = res {
                    debug!("fetch cluster succeeded, result: {res:?}");
                    if let Err(e) = self.check_and_update(&res).await {
                        warn!("update to a new cluster state failed, error {e}");
                    }
                    return Ok(res);
                }
                debug!("fetch cluster quorum ok, but members are empty");
            }
            debug!("fetch cluster from {id} success");
        }

        if let Some(err) = err {
            return Err(err);
        }

        // It seems that the max term has not reached the majority here. Mock a transport error and return it to the external to retry.
        return Err(CurpError::RpcTransport(()));
    }
}

#[async_trait]
impl<C: Command> LeaderStateUpdate for Unary<C> {
    /// Update leader
    async fn update_leader(&self, leader_id: Option<ServerId>, term: u64) -> bool {
        let mut state_w = self.state.write().await;
        Self::check_and_update_leader(&mut state_w, leader_id, term)
    }
}

/// Calculate the super quorum
fn super_quorum(size: usize) -> usize {
    let fault_tolerance = size.wrapping_div(2);
    fault_tolerance
        .wrapping_add(fault_tolerance.wrapping_add(1).wrapping_div(2))
        .wrapping_add(1)
}

/// Calculate the quorum
fn quorum(size: usize) -> usize {
    size.wrapping_div(2).wrapping_add(1)
}
