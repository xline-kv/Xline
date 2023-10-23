use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Debug,
    iter,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use dashmap::DashMap;
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use parking_lot::RwLock;
use tokio::time::timeout;
use tracing::{debug, instrument, warn};
use utils::config::ClientConfig;

use crate::{
    cmd::{Command, ProposeId},
    error::{ClientBuildError, ClientError, ERROR_LABEL},
    members::ServerId,
    rpc::{
        self, connect::ConnectApi, protocol_client::ProtocolClient, FetchClusterRequest,
        FetchClusterResponse, FetchReadStateRequest, ProposeConfChangeRequest, ProposeRequest,
        ReadState as PbReadState, ShutdownRequest, SyncResult, WaitSyncedRequest,
    },
    ConfChange, ConfChangeError, LogIndex, Member, ProposeError,
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
    /// Cluster version
    #[builder(setter(skip))]
    cluster_version: AtomicU64,
    /// Curp client config settings
    config: ClientConfig,
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
        all_members: HashMap<ServerId, Vec<String>>,
        leader_id: Option<ServerId>,
    ) -> Result<Client<C>, ClientBuildError> {
        let Some(config) = self.config else {
            return Err(ClientBuildError::invalid_arguments("timeout is required"));
        };
        let connects = rpc::connect(all_members).await?.collect();
        let client = Client::<C> {
            local_server_id: self.local_server_id,
            state: RwLock::new(State::new(leader_id, 0)),
            config,
            connects,
            cluster_version: AtomicU64::new(0),
            phantom: PhantomData,
        };
        Ok(client)
    }

    /// Fetch cluster from server, return the first `FetchClusterResponse`
    async fn fast_fetch_cluster(
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
                    let mut req = tonic::Request::new(FetchClusterRequest::default());
                    req.set_timeout(propose_timeout);
                    let fetch_cluster_res = protocol_client.fetch_cluster(req).await?.into_inner();
                    Ok::<FetchClusterResponse, ClientBuildError>(fetch_cluster_res)
                }
            })
            .collect();
        let mut err = ClientBuildError::invalid_arguments("addrs is empty");
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

    /// Build client from addresses (could be incomplete), this method will fetch all members from servers
    /// # Errors
    /// Return error when meet rpc error or missing some arguments
    #[inline]
    pub async fn build_from_addrs(
        &self,
        addrs: Vec<String>,
    ) -> Result<Client<C>, ClientBuildError> {
        let Some(config) = self.config else {
            return Err(ClientBuildError::invalid_arguments("timeout is required"));
        };
        let res: FetchClusterResponse = self
            .fast_fetch_cluster(addrs.clone(), *config.propose_timeout())
            .await?;
        let client = Client::<C> {
            local_server_id: self.local_server_id,
            state: RwLock::new(State::new(res.leader_id, res.term)),
            config,
            cluster_version: AtomicU64::new(res.cluster_version),
            connects: rpc::connect(res.into_members_addrs()).await?.collect(),
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
            .field("timeout", &self.config)
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

    /// Check the term and leader id, update the state if needed
    fn check_and_update(&mut self, leader_id: Option<u64>, term: u64) {
        match self.term.cmp(&term) {
            Ordering::Less => {
                // reset term only when the resp has leader id to prevent:
                // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                if let Some(new_leader_id) = leader_id {
                    self.update_to_term(term);
                    self.set_leader(new_leader_id);
                }
            }
            Ordering::Equal => {
                if let Some(new_leader_id) = leader_id {
                    if self.leader.is_none() {
                        self.set_leader(new_leader_id);
                    }
                    assert_eq!(
                        self.leader,
                        Some(new_leader_id),
                        "there should never be two leader in one term"
                    );
                }
            }
            Ordering::Greater => {}
        }
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

/// Unpack `tonic::Status`
enum UnpackStatus {
    /// CurpServer is shutting down
    ShuttingDown,
    /// Indicate that the client sent a wait synced request to a non-leader
    Redirect(Option<ServerId>, u64),
    /// Transport error type
    Transport,
    /// Encode decode error type
    EncodeDecode,
    /// Storage error type
    Storage,
    /// IO error type
    IO,
    /// Internal error type
    Internal,
}

/// unpack `tonic::Status` and convert it to `UnpackStatus`
fn unpack_status(status: &tonic::Status) -> UnpackStatus {
    let meta = status.metadata();
    if let Some(label) = meta.get(ERROR_LABEL) {
        match label.to_str().unwrap_or_else(|err| {
            unreachable!("error-label should be always able to convert to str: {err:?}")
        }) {
            "shutting-down" => UnpackStatus::ShuttingDown,
            "transport" => UnpackStatus::Transport,
            "redirect" => {
                let (leader_id, term) = serde_json::from_slice(status.details()).unwrap_or_else(|err| unreachable!(" deserialize (leader_id, term) from status' detail should always success: {err:?}"));
                UnpackStatus::Redirect(leader_id, term)
            }
            "encode-decode" => UnpackStatus::EncodeDecode,
            "internal" => UnpackStatus::Internal,
            "storage" => UnpackStatus::Storage,
            "io" => UnpackStatus::IO,
            unsupported_label => {
                unreachable!("unsupported status label {unsupported_label}")
            }
        }
    } else {
        // This transport error comes from `tonic` framework
        UnpackStatus::Transport
    }
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

    /// Get cluster version
    fn cluster_version(&self) -> u64 {
        self.cluster_version
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The fast round of Curp protocol
    /// It broadcast the requests to all the curp servers.
    #[instrument(skip_all)]
    async fn fast_round(
        &self,
        cmd_arc: Arc<C>,
    ) -> Result<(Option<<C as Command>::ER>, bool), ClientError<C>> {
        debug!("fast round for cmd({}) started", cmd_arc.id());
        let req = ProposeRequest::new(cmd_arc.as_ref(), self.cluster_version());

        let connects = self
            .connects
            .iter()
            .map(|connect| Arc::clone(&connect))
            .collect_vec();
        let mut rpcs: FuturesUnordered<_> = connects
            .iter()
            .zip(iter::repeat(req))
            .map(|(connect, req_cloned)| {
                connect.propose(req_cloned, *self.config.propose_timeout())
            })
            .collect();

        let mut ok_cnt: usize = 0;
        let mut execute_result: Option<C::ER> = None;
        let superquorum = superquorum(self.connects.len());
        while let Some(resp_result) = rpcs.next().await {
            let resp = match resp_result {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    if let UnpackStatus::ShuttingDown = unpack_status(&e) {
                        return Err(ClientError::ShuttingDown);
                    }
                    warn!("Propose error: {}", e);
                    continue;
                }
            };
            self.state
                .write()
                .check_and_update(resp.leader_id, resp.term);
            resp.map_or_else::<C, _, _, Result<(), ClientError<C>>>(
                |res| {
                    if let Some(er) = res
                        .transpose()
                        .map_err(|e| ClientError::CommandError::<C>(e))?
                    {
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
            )??;
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
    ) -> Result<(<C as Command>::ASR, <C as Command>::ER), ClientError<C>> {
        debug!("slow round for cmd({}) started", cmd.id());
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            // fetch leader id
            let leader_id = match self.get_leader_id().await {
                Ok(id) => id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("wait synced request sent to {}", leader_id);

            let resp = match self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .wait_synced(
                    WaitSyncedRequest::new(cmd.id(), self.cluster_version()),
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("wait synced rpc error: {e}");
                    match unpack_status(&e) {
                        UnpackStatus::ShuttingDown => return Err(ClientError::ShuttingDown),
                        UnpackStatus::Transport => {
                            // it's quite likely that the leader has crashed, then we should wait for some time and fetch the leader again
                            tokio::time::sleep(retry_timeout.next_retry()).await;
                            self.resend_propose(Arc::clone(&cmd), None).await?;
                            continue;
                        }
                        UnpackStatus::Redirect(new_leader, term) => {
                            self.state.write().check_and_update(new_leader, term);
                            // resend the propose to the new leader
                            self.resend_propose(Arc::clone(&cmd), new_leader).await?;
                            continue;
                        }
                        UnpackStatus::EncodeDecode
                        | UnpackStatus::Storage
                        | UnpackStatus::IO
                        | UnpackStatus::Internal => return Err(e.into()),
                    }
                }
            };

            match SyncResult::<C>::try_from(resp).map_err(Into::<ClientError<C>>::into)? {
                SyncResult::Success { er, asr } => {
                    debug!("slow round for cmd({}) succeeded", cmd.id());
                    return Ok((asr, er));
                }
                SyncResult::Error(e) => {
                    return Err(ClientError::CommandError(e));
                }
            }
        }
        Err(ClientError::Timeout)
    }

    /// The shutdown rpc of curp protocol
    #[instrument(skip_all)]
    pub async fn shutdown(&self) -> Result<(), ClientError<C>> {
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let leader_id = match self.get_leader_id().await {
                Ok(leader_id) => leader_id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("shutdown request sent to {}", leader_id);
            if let Err(e) = self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .shutdown(
                    ShutdownRequest::default(),
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                warn!("shutdown rpc error: {e}");
                match unpack_status(&e) {
                    UnpackStatus::ShuttingDown => return Err(ClientError::ShuttingDown),
                    UnpackStatus::Redirect(new_leader, term) => {
                        self.state.write().check_and_update(new_leader, term);
                        warn!("shutdown: redirect to new leader {new_leader:?}, term is {term}",);
                        continue;
                    }
                    UnpackStatus::Transport
                    | UnpackStatus::EncodeDecode
                    | UnpackStatus::Storage
                    | UnpackStatus::IO
                    | UnpackStatus::Internal => {
                        tokio::time::sleep(retry_timeout.next_retry()).await;
                        continue;
                    }
                }
            };
            return Ok(());
        }
        Err(ClientError::Timeout)
    }

    /// Resend the propose only to the leader. This is used when leader changes and we need to ensure that the propose is received by the new leader.
    async fn resend_propose(
        &self,
        cmd: Arc<C>,
        mut new_leader: Option<ServerId>,
    ) -> Result<(), ClientError<C>> {
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            tokio::time::sleep(retry_timeout.next_retry()).await;

            let leader_id = if let Some(id) = new_leader.take() {
                id
            } else {
                match self.fetch_leader().await {
                    Ok(id) => id,
                    Err(e) => {
                        warn!("failed to fetch leader, {e}");
                        continue;
                    }
                }
            };
            debug!("resend propose to {leader_id}");

            let resp = self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .propose(
                    ProposeRequest::new(cmd.as_ref(), self.cluster_version()),
                    *self.config.propose_timeout(),
                )
                .await;

            let resp = match resp {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    if let Some(rpc::ExeResult::Error(e)) = resp.exe_result {
                        let err: ProposeError = e.into();
                        if matches!(err, ProposeError::Duplicated) {
                            return Ok(());
                        }
                    }
                    resp
                }
                Err(e) => {
                    // if the propose fails again, need to fetch the leader and try again
                    warn!("failed to resend propose, {e}");
                    continue;
                }
            };

            let mut state_w = self.state.write();

            match state_w.term.cmp(&resp.term) {
                Ordering::Less => {
                    // reset term only when the resp has leader id to prevent:
                    // If a server loses contact with its leader, it will update its term for election. Since other servers are all right, the election will not succeed.
                    // But if the client learns about the new term and updates its term to it, it will never get the true leader.
                    if let Some(id) = resp.leader_id {
                        state_w.update_to_term(resp.term);
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
        Err(ClientError::Timeout)
    }

    /// Send fetch cluster requests to all servers
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    /// # Errors
    ///   `ClientError<C>::Timeout` if timeout
    #[inline]
    async fn fetch_cluster(
        &self,
        linearizable: bool,
    ) -> Result<FetchClusterResponse, ClientError<C>> {
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let connects = self.all_connects();
            let timeout = retry_timeout.next_retry();
            let mut rpcs: FuturesUnordered<_> = connects
                .iter()
                .map(|connect| async {
                    (
                        connect.id(),
                        connect
                            .fetch_cluster(FetchClusterRequest { linearizable }, timeout)
                            .await,
                    )
                })
                .collect();
            let mut max_term = 0;
            let mut res = None;

            let mut ok_cnt = 0;
            #[allow(clippy::integer_arithmetic)]
            let majority_cnt = connects.len() / 2 + 1;
            while let Some((id, resp)) = rpcs.next().await {
                let inner = match resp {
                    Ok(resp) => resp.into_inner(),
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
                if ok_cnt >= majority_cnt {
                    break;
                }
            }

            if let Some(res) = res {
                let mut state = self.state.write();
                debug!("Fetch cluster succeeded, result: {res:?}");
                state.check_and_update(res.leader_id, res.term);
                return Ok(res);
            }

            // wait until the election is completed
            // TODO: let user configure it according to average leader election cost
            tokio::time::sleep(timeout).await;
        }
        Err(ClientError::Timeout)
    }

    /// Send fetch leader requests to all servers until there is a leader
    /// Note: The fetched leader may still be outdated
    async fn fetch_leader(&self) -> Result<ServerId, ClientError<C>> {
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let res = self.fetch_cluster(false).await?;
            if let Some(leader_id) = res.leader_id {
                return Ok(leader_id);
            }
        }
        // This timeout is a bit different. It refers to the situation where
        // multiple attempts to fetch the cluster are successful, but there
        // is no leader id (very rare).
        Err(ClientError::Timeout)
    }

    /// Get leader id from the state or fetch it from servers
    /// # Errors
    /// `ClientError::Timeout` if timeout
    #[inline]
    pub async fn get_leader_id(&self) -> Result<ServerId, ClientError<C>> {
        let notify = Arc::clone(&self.state.read().leader_notify);
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            if let Some(id) = self.state.read().leader {
                return Ok(id);
            }
            if timeout(retry_timeout.next_retry(), notify.listen())
                .await
                .is_err()
            {
                return self.fetch_leader().await;
            }
        }
        Err(ClientError::Timeout)
    }

    /// Propose the request to servers, if use_fast_path is false, it will wait for the synced index
    /// # Errors
    ///   `ClientError::Execute` if execution error is met
    ///   `ClientError::AfterSync` error met while syncing logs to followers
    /// # Panics
    ///   If leader index is out of bound of all the connections, panic
    #[inline]
    #[instrument(skip_all, fields(cmd_id=%cmd.id()))]
    #[allow(clippy::type_complexity)] // This type is not complex
    pub async fn propose(
        &self,
        cmd: C,
        use_fast_path: bool,
    ) -> Result<(C::ER, Option<C::ASR>), ClientError<C>> {
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

    /// Propose the conf change request to servers
    #[instrument(skip_all)]
    pub async fn propose_conf_change(
        &self,
        propose_id: ProposeId,
        changes: Vec<ConfChange>,
    ) -> Result<Result<Vec<Member>, ConfChangeError>, ClientError<C>> {
        debug!(
            "propose_conf_change with propose_id({}) started",
            propose_id
        );
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let leader_id = match self.get_leader_id().await {
                Ok(leader_id) => leader_id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("propose_conf_change request sent to {}", leader_id);
            let resp = match self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .propose_conf_change(
                    ProposeConfChangeRequest::new(
                        propose_id,
                        changes.clone(),
                        self.cluster_version(),
                    ),
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("propose_conf_change rpc error: {e}");
                    match unpack_status(&e) {
                        UnpackStatus::ShuttingDown => return Err(ClientError::ShuttingDown),
                        UnpackStatus::Redirect(new_leader, term) => {
                            self.state.write().check_and_update(new_leader, term);
                            continue;
                        }
                        UnpackStatus::Transport
                        | UnpackStatus::EncodeDecode
                        | UnpackStatus::Storage
                        | UnpackStatus::IO
                        | UnpackStatus::Internal => {
                            tokio::time::sleep(retry_timeout.next_retry()).await;
                            continue;
                        }
                    }
                }
            };
            self.state
                .write()
                .check_and_update(resp.leader_id, resp.term);
            return match resp.error {
                Some(e) => {
                    warn!("propose conf change error: {:?}", e);
                    Ok(Err(e))
                }
                None => Ok(Ok(resp.members)),
            };
        }
        Err(ClientError::Timeout)
    }

    /// Fetch Read state from leader
    /// # Errors
    ///   `ClientError::EncodingError` encoding error met while deserializing the propose id
    #[inline]
    pub async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, ClientError<C>> {
        let mut retry_timeout = self.get_backoff();
        let retry_count = *self.config.retry_count();
        for _ in 0..retry_count {
            let leader_id = match self.get_leader_id().await {
                Ok(id) => id,
                Err(e) => {
                    warn!("failed to fetch leader, {e}");
                    continue;
                }
            };
            debug!("fetch read state request sent to {}", leader_id);
            let resp = match self
                .get_connect(leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .fetch_read_state(
                    FetchReadStateRequest::new(cmd, self.cluster_version())?,
                    *self.config.wait_synced_timeout(),
                )
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("fetch read state rpc error: {e}");
                    tokio::time::sleep(retry_timeout.next_retry()).await;
                    continue;
                }
            };
            let pb_state = resp
                .read_state
                .unwrap_or_else(|| unreachable!("read state should be some"));
            let state = match pb_state {
                PbReadState::CommitIndex(i) => ReadState::CommitIndex(i),
                PbReadState::Ids(i) => ReadState::Ids(i.ids.into_iter().map(Into::into).collect()),
            };
            return Ok(state);
        }
        Err(ClientError::Timeout)
    }

    /// Fetch the current cluster from the curp server where is on the same node.
    /// Note that this method should not be invoked by an outside client because
    /// we will fallback to fetch the full cluster for the response if fetching local
    /// failed.
    #[inline]
    async fn fetch_local_cluster(&self) -> Result<FetchClusterResponse, ClientError<C>> {
        if let Some(local_server) = self.local_server_id {
            let resp = self
                .get_connect(local_server)
                .unwrap_or_else(|| unreachable!("self id {} not found", local_server))
                .fetch_cluster(
                    FetchClusterRequest::default(),
                    *self.config.initial_retry_timeout(),
                )
                .await?
                .into_inner();
            Ok(resp)
        } else {
            unreachable!("The outer client shouldn't invoke fetch_local_leader_info");
        }
    }

    /// Fetch the current leader id without cache
    /// # Errors
    /// `ClientError::Timeout` if timeout
    #[inline]
    pub async fn get_leader_id_from_curp(&self) -> Result<ServerId, ClientError<C>> {
        if let Ok(FetchClusterResponse {
            leader_id: Some(leader_id),
            ..
        }) = self.fetch_local_cluster().await
        {
            return Ok(leader_id);
        }
        self.fetch_leader().await
    }

    /// Fetch the current cluster without cache
    /// # Errors
    /// `ClientError::Timeout` if timeout
    #[inline]
    pub async fn get_cluster_from_curp(
        &self,
        linearizable: bool,
    ) -> Result<FetchClusterResponse, ClientError<C>> {
        if linearizable {
            return self.fetch_cluster(true).await;
        }
        if let Ok(resp) = self.fetch_local_cluster().await {
            return Ok(resp);
        }
        self.fetch_cluster(false).await
    }

    /// Get the connect by server id
    fn get_connect(&self, id: ServerId) -> Option<Arc<dyn ConnectApi>> {
        self.connects.get(&id).map(|c| Arc::clone(&c))
    }

    /// Get all connects
    fn all_connects(&self) -> Vec<Arc<dyn ConnectApi>> {
        self.connects.iter().map(|c| Arc::clone(&c)).collect()
    }

    /// Get the client id
    ///
    /// # Errors
    ///
    ///   `ClientError::Timeout` if timeout
    #[allow(clippy::unused_async)] // TODO: grant a client id from server
    async fn get_client_id(&self) -> Result<u64, ClientError<C>> {
        Ok(rand::random())
    }

    /// New a seq num and record it
    #[allow(clippy::unused_self)] // TODO: implement request tracker
    fn new_seq_num(&self) -> u64 {
        0
    }

    /// Generate a propose id
    ///
    /// # Errors
    ///   `ClientError::Timeout` if timeout
    #[inline]
    pub async fn gen_propose_id(&self) -> Result<ProposeId, ClientError<C>> {
        let client_id = self.get_client_id().await?;
        let seq_num = self.new_seq_num();
        Ok(ProposeId(client_id, seq_num))
    }

    /// Get the initial backoff config
    fn get_backoff(&self) -> BackOff {
        BackOff::new(
            *self.config.initial_retry_timeout(),
            *self.config.max_retry_timeout(),
            *self.config.use_backoff(),
        )
    }
}

/// Generate timeout using exponential backoff algorithm
struct BackOff {
    /// Current timeout
    timeout: Duration,
    /// Max timeout
    max_timeout: Duration,
    /// Whether to use backoff
    use_backoff: bool,
}

impl BackOff {
    /// Creates a new `BackOff`
    fn new(initial_timeout: Duration, max_timeout: Duration, use_backoff: bool) -> Self {
        Self {
            timeout: initial_timeout,
            max_timeout,
            use_backoff,
        }
    }

    /// Get current timeout
    fn next_retry(&mut self) -> Duration {
        let current = self.timeout;
        if self.use_backoff {
            self.timeout = self
                .timeout
                .checked_mul(2)
                .unwrap_or(self.timeout)
                .min(self.max_timeout);
        }
        current
    }
}

/// Get the superquorum for curp protocol
/// Although curp can proceed with f + 1 available replicas, it needs f + 1 + (f + 1)/2 replicas
/// (for superquorum of witnesses) to use 1 RTT operations. With less than superquorum replicas,
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
            .config(ClientConfig::default())
            .build_from_all_members(HashMap::from([(123, vec!["addr".to_owned()])]), None)
            .await;
        assert!(res.is_ok());

        let res = Client::<TestCommand>::builder()
            .local_server_id(123)
            .build_from_addrs(vec!["addr".to_owned()])
            .await;
        assert!(matches!(res, Err(ClientBuildError::InvalidArguments(_))));
    }
}
