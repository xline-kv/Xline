use std::{
    cmp::Ordering, collections::HashMap, fmt::Debug, iter, marker::PhantomData, sync::Arc,
    time::Duration,
};

use curp_external_api::cmd::PbSerializeError;
use dashmap::DashMap;
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use parking_lot::RwLock;
use tokio::time::timeout;
use tracing::{debug, instrument, warn};
use utils::{config::ClientTimeout, parking_lot_lock::RwLockMap};

use crate::{
    cmd::{Command, ProposeId},
    error::{
        ClientBuildError, CommandProposeError, CommandSyncError, ProposeError, RpcError, SyncError,
    },
    members::ServerId,
    rpc::{
        self, connect::ConnectApi, protocol_client::ProtocolClient, FetchClusterRequest,
        FetchClusterResponse, FetchLeaderRequest, FetchReadStateRequest, ProposeRequest,
        ReadState as PbReadState, SyncResult, WaitSyncedRequest,
    },
    LogIndex,
};

/// Protocol client
#[derive(derive_builder::Builder)]
#[builder(build_fn(skip), name = "Builder")]
pub struct Client<C: Command> {
    /// local server id. Only use in an inner client.
    #[builder(field(type = "Option<ServerId>"), setter(custom))]
    local_server_id: Option<ServerId>,
    /// Current leader and term
    #[builder(setter(skip))]
    state: RwLock<State>,
    /// All servers's `Connect`
    #[builder(setter(skip))]
    connects: DashMap<ServerId, Arc<dyn ConnectApi>>,
    /// Curp client timeout settings
    timeout: ClientTimeout,
    /// To keep Command type
    #[builder(setter(skip))]
    phantom: PhantomData<C>,
}

impl<C: Command> Builder<C> {
    /// Set local server id.
    #[inline]
    pub fn local_server_id(&mut self, value: ServerId) -> &mut Self {
        self.local_server_id = Some(value);
        self
    }

    /// Build client from all members
    /// # Errors
    /// Return error when meet rpc error or missing some arguments
    #[inline]
    pub async fn build_from_all_members(
        &self,
        all_members: HashMap<ServerId, String>,
    ) -> Result<Client<C>, ClientBuildError> {
        let Some(timeout) = self.timeout else {
            return Err(ClientBuildError::invalid_aurguments("timeout is required"));
        };
        let connects = rpc::connect(all_members).await.collect();
        let client = Client::<C> {
            local_server_id: self.local_server_id,
            state: RwLock::new(State::new(None, 0)),
            timeout,
            connects,
            phantom: PhantomData,
        };
        Ok(client)
    }

    /// Fetch cluster from server
    async fn fetch_cluster(
        &self,
        addrs: Vec<String>,
        propose_timeout: Duration,
    ) -> Result<FetchClusterResponse, ClientBuildError> {
        let mut futs: FuturesUnordered<_> = addrs
            .into_iter()
            .map(|mut addr| {
                if !addr.starts_with("http://") {
                    addr.insert_str(0, "http://");
                }
                async move {
                    let mut protocol_client = ProtocolClient::connect(addr).await?;
                    let mut req = tonic::Request::new(FetchClusterRequest::new());
                    req.set_timeout(propose_timeout);
                    let fetch_cluster_res = protocol_client.fetch_cluster(req).await?.into_inner();
                    Ok::<FetchClusterResponse, ClientBuildError>(fetch_cluster_res)
                }
            })
            .collect();
        let mut err = ClientBuildError::invalid_aurguments("addrs is empty");
        while let Some(r) = futs.next().await {
            match r {
                Ok(r) => {
                    return Ok(r);
                }
                Err(e) => err = e,
            }
        }
        Err(err)
    }

    /// Build client from addresses, this method will fetch all members from servers
    /// # Errors
    /// Return error when meet rpc error or missing some arguments
    #[inline]
    pub async fn build_from_addrs(
        &self,
        addrs: Vec<String>,
    ) -> Result<Client<C>, ClientBuildError> {
        let Some(timeout) = self.timeout else {
            return Err(ClientBuildError::invalid_aurguments("timeout is required"));
        };
        let res = self
            .fetch_cluster(addrs.clone(), *timeout.propose_timeout())
            .await?;
        let connects = rpc::connect(res.all_members).await.collect();
        let client = Client::<C> {
            local_server_id: self.local_server_id,
            state: RwLock::new(State::new(res.leader_id, res.term)),
            timeout,
            connects,
            phantom: PhantomData,
        };
        Ok(client)
    }
}

impl<C: Command> Debug for Client<C> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &self.state)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// State of a client
#[derive(Debug, Default)]
struct State {
    /// Current leader
    leader: Option<ServerId>,
    /// Current term
    term: u64,
    /// When a new leader is set, notify
    leader_notify: Arc<Event>,
}

impl State {
    /// Create the initial client state
    fn new(leader: Option<ServerId>, term: u64) -> Self {
        Self {
            leader,
            term,
            leader_notify: Arc::new(Event::new()),
        }
    }

    /// Set the leader and notify all the waiters
    fn set_leader(&mut self, id: ServerId) {
        debug!("client update its leader to {id}");
        self.leader = Some(id);
        self.leader_notify.notify(usize::MAX);
    }

    /// Update to the newest term and reset local cache
    fn update_to_term(&mut self, term: u64) {
        debug_assert!(self.term <= term, "the client's term {} should not be greater than the given term {} when update the term", self.term, term);
        self.term = term;
        self.leader = None;
    }
}

/// Read state of a command
#[derive(Debug)]
#[non_exhaustive]
pub enum ReadState {
    /// need to wait other proposals
    Ids(Vec<ProposeId>),
    /// need to wait the commit index
    CommitIndex(LogIndex),
}

impl<C> Client<C>
where
    C: Command + 'static,
{
    /// Client builder
    #[inline]
    #[must_use]
    pub fn builder() -> Builder<C> {
        Builder::default()
    }

    /// The fast round of Curp protocol
    /// It broadcast the requests to all the curp servers.
    #[instrument(skip_all)]
    async fn fast_round(
        &self,
        cmd_arc: Arc<C>,
    ) -> Result<(Option<<C as Command>::ER>, bool), CommandProposeError<C>> {
        debug!("fast round for cmd({}) started", cmd_arc.id());
        let req = ProposeRequest::new(cmd_arc.as_ref());

        let connects = self
            .connects
            .iter()
            .map(|connect| Arc::clone(&connect))
            .collect_vec();
        let mut rpcs: FuturesUnordered<_> = connects
            .iter()
            .zip(iter::repeat(req))
            .map(|(connect, req_cloned)| {
                connect.propose(req_cloned, *self.timeout.propose_timeout())
            })
            .collect();

        let mut ok_cnt: usize = 0;
        let mut execute_result: Option<C::ER> = None;
        let superquorum = superquorum(self.connects.len());
        while let Some(resp_result) = rpcs.next().await {
            let resp = match resp_result {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("Propose error: {}", e);
                    continue;
                }
            };
            self.state.map_write(|mut state| {
                match state.term.cmp(&resp.term()) {
                    Ordering::Less => {
                        // reset term only when the resp has leader id to prevent:
                        // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                        // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                        if let Some(leader_id) = resp.leader_id {
                            state.update_to_term(resp.term());
                            state.set_leader(leader_id);
                            execute_result = None;
                        }
                    }
                    Ordering::Equal => {
                        if let Some(leader_id) = resp.leader_id {
                            if state.leader.is_none() {
                                state.set_leader(leader_id);
                            }
                            assert_eq!(
                                state.leader,
                                Some(leader_id),
                                "there should never be two leader in one term"
                            );
                        }
                    }
                    Ordering::Greater => {}
                }
            });
            resp.map_or_else::<C, _, _, _>(
                |res| {
                    if let Some(er) = res.transpose()? {
                        assert!(execute_result.is_none(), "should not set exe result twice");
                        execute_result = Some(er);
                    }
                    ok_cnt = ok_cnt.wrapping_add(1);
                    Ok(())
                },
                |err| {
                    warn!("Propose error: {}", err);
                    Ok(())
                },
            )
            .map_err(Into::<ProposeError>::into)?
            .map_err(|e| CommandProposeError::Execute(e))?;
            if (ok_cnt >= superquorum) && execute_result.is_some() {
                debug!("fast round for cmd({}) succeed", cmd_arc.id());
                return Ok((execute_result, true));
            }
        }
        Ok((execute_result, false))
    }

    /// The slow round of Curp protocol
    #[instrument(skip_all)]
    async fn slow_round(
        &self,
        cmd: Arc<C>,
    ) -> Result<(<C as Command>::ASR, <C as Command>::ER), CommandProposeError<C>> {
        debug!("slow round for cmd({}) started", cmd.id());
        let retry_timeout = *self.timeout.retry_timeout();
        loop {
            // fetch leader id
            let leader_id = self.get_leader_id().await;

            debug!("wait synced request sent to {}", leader_id);

            let resp = match self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .wait_synced(
                    WaitSyncedRequest::new(cmd.id()),
                    *self.timeout.wait_synced_timeout(),
                )
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("wait synced rpc error: {e}");
                    // it's quite likely that the leader has crashed, then we should wait for some time and fetch the leader again
                    tokio::time::sleep(retry_timeout).await;
                    self.resend_propose(Arc::clone(&cmd), None).await?;
                    continue;
                }
            };

            match resp.into::<C>().map_err(Into::<ProposeError>::into)? {
                SyncResult::Success { er, asr } => {
                    debug!("slow round for cmd({}) succeeded", cmd.id());
                    return Ok((asr, er));
                }
                SyncResult::Error(CommandSyncError::Sync(SyncError::Redirect(server_id, term))) => {
                    let new_leader = server_id.and_then(|id| {
                        self.state.map_write(|mut state| {
                            (state.term <= term).then(|| {
                                state.leader = Some(id);
                                state.term = term;
                                id
                            })
                        })
                    });
                    self.resend_propose(Arc::clone(&cmd), new_leader).await?; // resend the propose to the new leader
                }
                SyncResult::Error(CommandSyncError::Sync(e)) => {
                    return Err(ProposeError::SyncedError(SyncError::Other(e.to_string())).into());
                }
                SyncResult::Error(CommandSyncError::Execute(e)) => {
                    return Err(CommandProposeError::Execute(e));
                }
                SyncResult::Error(CommandSyncError::AfterSync(e)) => {
                    return Err(CommandProposeError::AfterSync(e));
                }
            }
        }
    }

    /// Resend the propose only to the leader. This is used when leader changes and we need to ensure that the propose is received by the new leader.
    async fn resend_propose(
        &self,
        cmd: Arc<C>,
        mut new_leader: Option<ServerId>,
    ) -> Result<(), ProposeError> {
        loop {
            tokio::time::sleep(*self.timeout.retry_timeout()).await;

            let leader_id = if let Some(id) = new_leader.take() {
                id
            } else {
                self.fetch_leader().await
            };
            debug!("resend propose to {leader_id}");

            let resp = self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .propose(
                    ProposeRequest::new(cmd.as_ref()),
                    *self.timeout.propose_timeout(),
                )
                .await;

            let resp = match resp {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if let Some(rpc::ExeResult::Error(ref e)) = resp.exe_result {
                        let err: ProposeError = e
                            .clone()
                            .propose_error
                            .ok_or(PbSerializeError::EmptyField)?
                            .try_into()?;
                        if matches!(err, ProposeError::Duplicated) {
                            return Ok(());
                        }
                    }
                    resp
                }
                Err(e) => {
                    // if the propose fails again, need to fetch the leader and try again
                    warn!("failed to resend propose, {e}");
                    tokio::time::sleep(*self.timeout.retry_timeout()).await;
                    continue;
                }
            };

            let mut state_w = self.state.write();

            let resp_term = resp.term();
            match state_w.term.cmp(&resp_term) {
                Ordering::Less => {
                    // reset term only when the resp has leader id to prevent:
                    // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                    // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                    if let Some(id) = resp.leader_id {
                        state_w.update_to_term(resp_term);
                        let done = id == leader_id;
                        state_w.set_leader(leader_id);
                        if done {
                            return Ok(());
                        }
                    }
                }
                Ordering::Equal => {
                    if let Some(id) = resp.leader_id {
                        let done = id == leader_id;
                        debug_assert!(
                            state_w.leader.as_ref().map_or(true, |leader| leader == &id),
                            "there should never be two leader in one term"
                        );
                        if state_w.leader.is_none() {
                            state_w.set_leader(id);
                        }
                        if done {
                            return Ok(());
                        }
                    }
                }
                Ordering::Greater => {}
            }
        }
    }

    /// Send fetch leader requests to all servers until there is a leader
    /// Note: The fetched leader may still be outdated
    async fn fetch_leader(&self) -> ServerId {
        loop {
            let connects = self.all_connects();
            let mut rpcs: FuturesUnordered<_> = connects
                .iter()
                .map(|connect| async {
                    (
                        connect.id(),
                        connect
                            .fetch_leader(FetchLeaderRequest::new(), *self.timeout.retry_timeout())
                            .await,
                    )
                })
                .collect();
            let mut max_term = 0;
            let mut leader = None;

            let mut ok_cnt = 0;
            #[allow(clippy::integer_arithmetic)]
            let majority_cnt = connects.len() / 2 + 1;
            while let Some((id, resp)) = rpcs.next().await {
                let resp = match resp {
                    Ok(resp) => resp.into_inner(),
                    Err(e) => {
                        warn!("fetch leader from {} failed, {:?}", id, e);
                        continue;
                    }
                };
                if let Some(leader_id) = resp.leader_id {
                    #[allow(clippy::integer_arithmetic)]
                    match max_term.cmp(&resp.term) {
                        Ordering::Less => {
                            max_term = resp.term;
                            leader = Some(leader_id);
                            ok_cnt = 1;
                        }
                        Ordering::Equal => {
                            leader = Some(leader_id);
                            ok_cnt += 1;
                        }
                        Ordering::Greater => {}
                    }
                }
                if ok_cnt >= majority_cnt {
                    break;
                }
            }

            if let Some(leader) = leader {
                let mut state = self.state.write();
                debug!("Fetch leader succeeded, leader set to {}", leader);
                state.term = max_term;
                state.set_leader(leader);
                return leader;
            }

            // wait until the election is completed
            // TODO: let user configure it according to average leader election cost
            tokio::time::sleep(*self.timeout.retry_timeout()).await;
        }
    }

    /// Get leader id from the state or fetch it from servers
    #[inline]
    pub async fn get_leader_id(&self) -> ServerId {
        let notify = Arc::clone(&self.state.read().leader_notify);
        let retry_timeout = *self.timeout.retry_timeout();
        loop {
            if let Some(id) = self.state.read().leader {
                return id;
            }
            if timeout(retry_timeout, notify.listen()).await.is_err() {
                break self.fetch_leader().await;
            }
        }
    }

    /// Propose the request to servers, if use_fast_path is false, it will wait for the synced index
    /// # Errors
    ///   `CommandProposeError::Execute` if execution error is met
    ///   `CommandProposeError::AfterSync` error met while syncing logs to followers
    /// # Panics
    ///   If leader index is out of bound of all the connections, panic
    #[inline]
    #[instrument(skip_all, fields(cmd_id=%cmd.id()))]
    #[allow(clippy::type_complexity)] // This type is not complex
    pub async fn propose(
        &self,
        cmd: C,
        use_fast_path: bool,
    ) -> Result<(C::ER, Option<C::ASR>), CommandProposeError<C>> {
        let cmd_arc = Arc::new(cmd);
        let fast_round = self.fast_round(Arc::clone(&cmd_arc));
        let slow_round = self.slow_round(cmd_arc);
        if use_fast_path {
            pin_mut!(fast_round);
            pin_mut!(slow_round);

            // Wait for the fast and slow round at the same time
            match futures::future::select(fast_round, slow_round).await {
                futures::future::Either::Left((fast_result, slow_round)) => {
                    let (fast_er, success) = fast_result?;
                    if success {
                        #[allow(clippy::unwrap_used)]
                        // when success is true fast_er must be Some
                        Ok((fast_er.unwrap(), None))
                    } else {
                        let (_asr, er) = slow_round.await?;
                        Ok((er, None))
                    }
                }
                futures::future::Either::Right((slow_result, fast_round)) => match slow_result {
                    Ok((asr, er)) => Ok((er, Some(asr))),
                    Err(e) => {
                        if let Ok((Some(er), true)) = fast_round.await {
                            return Ok((er, None));
                        }
                        Err(e)
                    }
                },
            }
        } else {
            #[allow(clippy::integer_arithmetic)] // tokio framework triggers
            let (_fast_result, slow_result) = tokio::join!(fast_round, slow_round);

            match slow_result {
                Ok((asr, er)) => Ok((er, Some(asr))),
                Err(e) => Err(e),
            }
        }
    }

    /// Fetch Read state from leader
    /// # Errors
    ///   `ProposeError::EncodingError` encoding error met while deserializing the propose id
    #[inline]
    pub async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, ProposeError> {
        let retry_timeout = *self.timeout.retry_timeout();
        loop {
            let leader_id = self.get_leader_id().await;
            debug!("fetch read state request sent to {}", leader_id);
            let resp = match self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .fetch_read_state(
                    FetchReadStateRequest::new(cmd)?,
                    *self.timeout.wait_synced_timeout(),
                )
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("fetch read state rpc error: {e}");
                    tokio::time::sleep(retry_timeout).await;
                    continue;
                }
            };
            let pb_state = resp
                .read_state
                .unwrap_or_else(|| unreachable!("read state should be some"));
            let state = match pb_state {
                PbReadState::CommitIndex(i) => ReadState::CommitIndex(i),
                PbReadState::Ids(i) => ReadState::Ids(
                    i.ids
                        .into_iter()
                        .map(|id| bincode::deserialize(&id))
                        .collect::<bincode::Result<Vec<ProposeId>>>()?,
                ),
            };
            return Ok(state);
        }
    }

    /// Fetch the current leader id and term from the curp server where is on the same node.
    /// Note that this method should not be invoked by an outside client.
    #[inline]
    async fn fetch_local_leader_info(&self) -> Result<(Option<ServerId>, u64), RpcError> {
        if let Some(local_server) = self.local_server_id {
            let resp = self
                .get_connect(local_server)
                .unwrap_or_else(|| unreachable!("self id {} not found", local_server))
                .fetch_leader(FetchLeaderRequest::new(), *self.timeout.retry_timeout())
                .await?
                .into_inner();
            Ok((resp.leader_id, resp.term))
        } else {
            unreachable!("The outer client shouldn't invoke fetch_local_leader_info");
        }
    }

    /// Fetch the current leader id without cache
    #[inline]
    pub async fn get_leader_id_from_curp(&self) -> ServerId {
        if let Ok((Some(leader_id), _term)) = self.fetch_local_leader_info().await {
            return leader_id;
        }
        self.fetch_leader().await
    }

    /// Get the connect by server id
    fn get_connect(&self, id: ServerId) -> Option<Arc<dyn ConnectApi>> {
        self.connects.get(&id).map(|c| Arc::clone(&c))
    }

    /// Get all connects
    fn all_connects(&self) -> Vec<Arc<dyn ConnectApi>> {
        self.connects.iter().map(|c| Arc::clone(&c)).collect()
    }
}

/// Get the superquorum for curp protocol
/// Although curp can proceed with f + 1 available replicas, it needs f + 1 + (f + 1)/2 replicas
/// (for superquorum of witenesses) to use 1 RTT operations. With less than superquorum replicas,
/// clients must ask masters to commit operations in f + 1 replicas before returning result.(2 RTTs).
#[inline]
fn superquorum(nodes: usize) -> usize {
    let fault_tolerance = nodes.wrapping_div(2);
    fault_tolerance
        .wrapping_add(fault_tolerance.wrapping_add(1).wrapping_div(2))
        .wrapping_add(1)
}

#[cfg(test)]
mod tests {
    use curp_test_utils::test_cmd::TestCommand;

    use super::*;

    #[test]
    fn superquorum_should_work() {
        assert_eq!(superquorum(1), 1);
        assert_eq!(superquorum(11), 9);
        assert_eq!(superquorum(97), 73);
        assert_eq!(superquorum(31), 24);
        assert_eq!(superquorum(59), 45);
    }

    #[tokio::test]
    async fn client_builder_should_return_err_when_arguments_invalid() {
        let res = Client::<TestCommand>::builder()
            .timeout(ClientTimeout::default())
            .build_from_all_members(HashMap::from([(123, "addr".to_owned())]))
            .await;
        assert!(res.is_ok());

        let res = Client::<TestCommand>::builder()
            .local_server_id(123)
            .build_from_addrs(vec!["addr".to_owned()])
            .await;
        assert!(matches!(res, Err(ClientBuildError::InvalidArguments(_))));
    }
}
