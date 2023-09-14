use crate::{
    cmd::{Command, ProposeId},
    error::{
        ClientBuildError, CommandProposeError, CommandSyncError, ProposeError, RpcError,
        WaitSyncError,
    },
    members::ServerId,
    rpc::{
        self, connect::ConnectApi, protocol_client::ProtocolClient, FetchClusterRequest,
        FetchClusterResponse, FetchLeaderRequest, FetchReadStateRequest, ProposeRequest,
        ProposeResponse, ReadState as PbReadState, SyncResult, WaitSyncedRequest,
    },
    LogIndex,
};
use async_stream::stream;
use clippy_utilities::OverflowArithmetic;
use dashmap::DashMap;
use event_listener::Event;
use futures::{
    stream::{FuturesUnordered, Stream},
    StreamExt,
};
use itertools::Itertools;
use parking_lot::RwLock;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt::Debug,
    iter,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument, warn};
use utils::{config::ClientTimeout, parking_lot_lock::RwLockMap};

type FastRoundStream<C> = Pin<
    Box<
        dyn Stream<
                Item = (
                    ProposeId,
                    Result<(Option<<C as Command>::ER>, bool), CommandProposeError<C>>,
                ),
            > + Send,
    >,
>;

type SlowRoundStream<C> = Pin<
    Box<
        dyn Stream<
                Item = (
                    ProposeId,
                    Result<(<C as Command>::ASR, <C as Command>::ER), CommandProposeError<C>>,
                ),
            > + Send,
    >,
>;

type ProposeResult<C> = (
    ProposeId,
    Result<(<C as Command>::ER, Option<<C as Command>::ASR>), CommandProposeError<C>>,
);
type ProposeStream<C> = Pin<Box<dyn Stream<Item = ProposeResult<C>> + Send>>;

/// Protocol client
#[derive(derive_builder::Builder)]
#[builder(build_fn(skip), name = "Builder")]
pub struct Client<C: Command> {
    /// local server id. Only use in an inner client.
    #[builder(field(type = "Option<ServerId>"), setter(custom))]
    local_server_id: Option<ServerId>,
    /// Current leader and term
    #[builder(setter(skip))]
    state: Arc<RwLock<State>>,
    /// All servers's `Connect`
    #[builder(setter(skip))]
    connects: DashMap<ServerId, Arc<dyn ConnectApi + 'static>>,
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
            state: Arc::new(RwLock::new(State::new(None, 0))),
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
            state: Arc::new(RwLock::new(State::new(res.leader_id, res.term))),
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
    #[allow(dead_code)]
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

    #[inline]
    async fn process_streams(
        connects: Vec<Arc<dyn ConnectApi>>,
        req: ProposeRequest,
        sender: Sender<ProposeResponse>,
        propose_timeout: Duration,
    ) {
        let mut fut_stream = connects
            .iter()
            .zip(iter::repeat(req))
            .map(|(connect, req_cloned)| {
                connect.propose(req_cloned, propose_timeout)
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(resp_stream) = fut_stream.next().await {
            let mut resp_stream = match resp_stream {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("Propose error: {}", e);
                    continue;
                }
            };
            let tx = sender.clone();
            let _handle = tokio::spawn(async move {
                while let Some(resp_result) = resp_stream.next().await {
                    match resp_result {
                        Ok(resp) => {
                            if let Err(_) = tx.send(resp).await {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("Propose error: {}", e);
                            continue;
                        }
                    };
                }
            });
        }
    }

    /// The fast round of Curp protocol
    /// It broadcast the requests to all the curp servers.
    #[instrument(skip_all)]
    async fn fast_round( 
        connects: Vec<Arc<dyn ConnectApi>>,
        cmds: Arc<Vec<C>>,
        state: Arc<RwLock<State>>,
        propose_timeout: Duration
    ) -> FastRoundStream<C> {
        // debug!("fast round for cmd({}) started", cmds_arc.id());
        let req = ProposeRequest::new(cmds.as_ref());
        let mut ballot_box: HashMap<
            ProposeId,
            (Option<C::ER>, u64, Option<ServerId>, usize, usize),
        > = HashMap::new();
        let mut reach_consensus: Option<ProposeId> = None;

        for cmd in cmds.iter() {
            assert!(
                ballot_box
                    .insert(cmd.id().clone(), (None, 0, None, 0, 0))
                    .is_none(),
                "cmd {} has been proposed twice",
                cmd.id()
            );
        }

        let total_nodes = connects.len();
        let (tx, mut rx) = channel(cmds.len().overflow_mul(connects.len()));
        _ = tokio::spawn(Self::process_streams(connects, req.clone(), tx, propose_timeout));
        let superquorum = superquorum(total_nodes);
        let fast_stream = stream! {
            while let Some(resp) = rx.recv().await {
                if let Some((execute_result, cmd_term, leader_id, ok_cnt, recv_cnt)) = ballot_box.get_mut(&resp.propose_id) {
                    *recv_cnt = recv_cnt.wrapping_add(1);
                    if resp.term < *cmd_term {
                        continue;
                    }
                    *cmd_term = std::cmp::max(*cmd_term, resp.term);
                    if resp.leader_id.is_some() {
                        *leader_id = resp.leader_id;
                    }

                    let res =  resp.map_or_else::<C, _, _, _>(
                        |res| {
                            if let Some(er) = res
                                .transpose()
                                .map_err(|e| CommandProposeError::Execute(e))?
                            {
                                assert!(execute_result.is_none(), "should not set exe result twice");
                                *execute_result = Some(er);
                            }
                            *ok_cnt = ok_cnt.wrapping_add(1);
                            Ok(())
                        },
                        |err| {
                            warn!("error: {}", err);
                            Ok(())
                        },
                    );

                    match res {
                        Ok(Ok(())) => {
                            if (*ok_cnt >= superquorum) && execute_result.is_some() {
                                reach_consensus = Some(resp.propose_id.clone());
                                debug!("fast round for cmd({}) succeed", &resp.propose_id);
                                yield (resp.propose_id.clone(), Ok((execute_result.clone(), true)));
                            }
                        },
                        Ok(Err(e)) => {
                            reach_consensus = Some(resp.propose_id.clone());
                            yield (resp.propose_id.clone(), Err(e));
                        }
                        Err(e) => {
                            // 此处的 error 可能是 EmptyField 或者 encode/decode Error
                            reach_consensus = Some(resp.propose_id.clone());
                            // PbSerializeError -> ProposeError -> CommandProposeError<C> 然后 yield 给用户，同时剔除 ballot_box 中这条 cmd 的记录
                            yield (resp.propose_id.clone(), Err(CommandProposeError::Propose(Into::<ProposeError>::into(e))));
                        }
                    }
                    if *recv_cnt == total_nodes {
                        yield (resp.propose_id.clone(), Ok((execute_result.clone(), false)));
                    }
                }
                if let Some(ref id) = reach_consensus {
                    let (_er, term, leader_id, _ok_cnt, _recv_cnt) = ballot_box.remove(id).expect("cmd doesn't exist in ballot_box");
                    state.map_write(|mut state_w| state_w.check_and_update(leader_id, term));
                    reach_consensus = None;
                }
            }
        };
        Box::pin(fast_stream)
    }

    /// The slow round of Curp protocol
    #[instrument(skip_all)]
    async fn slow_round(
        leader_connect: Arc<dyn ConnectApi>, 
        state: Arc<RwLock<State>>, 
        wait_synced_timeout: Duration,
        cmds_arc: Arc<Vec<C>>
    ) -> SlowRoundStream<C> {
        let propose_ids: Vec<_> = cmds_arc.iter().map(|cmd| cmd.id().clone()).collect();
        debug!("slow round for cmd({propose_ids:?}) started");
        let mut resp_stream = loop {
            // fetch leader id

            match leader_connect
                .wait_synced(
                    WaitSyncedRequest::new(&propose_ids),
                    wait_synced_timeout,
                )
                .await
            {
                Ok(resp) => break resp.into_inner(),
                Err(_e) => {
                    // it's quite likely that the leader has crashed, then we should wait for some time and fetch the leader again
                    // self.resend_propose(Arc::clone(&cmds_arc), None)
                    //     .await
                    //     .expect("resend_propose failed");
                    continue;
                }
            };
        };
        let resp_stream = stream! {
            while let Some(resp) = resp_stream.next().await {
                match resp {
                    Ok(wait_resp) => {
                        let propose_id = wait_resp.propose_id.clone();
                        match wait_resp.into::<C>().map_err(Into::<ProposeError>::into).unwrap() {
                            SyncResult::Success { er, asr } => {
                                debug!("slow round for cmd({}) succeeded", propose_id);
                                yield (propose_id, Ok((asr, er)));
                            }
                            SyncResult::Error(CommandSyncError::WaitSync(WaitSyncError::Redirect(
                                server_id,
                                term,
                            ))) => {
                                let new_leader = server_id.and_then(|id| {
                                    state.map_write(|mut state| {
                                        (state.term <= term).then(|| {
                                            state.leader = Some(id);
                                            state.term = term;
                                            id
                                        })
                                    })
                                });
                                yield (propose_id, Err(CommandProposeError::Propose(ProposeError::SyncedError(WaitSyncError::Redirect(new_leader, term)))));
                            }
                            SyncResult::Error(CommandSyncError::WaitSync(e)) => {
                                yield (propose_id,  Err(
                                    ProposeError::SyncedError(WaitSyncError::Other(e.to_string())).into(),
                                ));
                            }
                            SyncResult::Error(CommandSyncError::Execute(e)) => {
                                yield (propose_id,  Err(CommandProposeError::Execute(e)));
                            }
                            SyncResult::Error(CommandSyncError::AfterSync(e)) => {
                                yield (propose_id,  Err(CommandProposeError::AfterSync(e)));
                            }
                        }
                    },
                    Err(e) => {
                        // yield (propose_id, Err(CommandProposeError::Propose(ProposeError::SyncedError(WaitSyncError::Other(e.to_string())))));
                        unreachable!("error: {e}")
                    }
                }
            }
        };

        Box::pin(resp_stream)
    }

    /// Resend the propose only to the leader. This is used when leader changes and we need to ensure that the propose is received by the new leader.
    #[allow(dead_code)]
    async fn resend_propose(
        &self,
        _cmds_arc: Arc<Vec<C>>,
        _new_leader: Option<ServerId>,
    ) -> Result<(), ProposeError> {
        unimplemented!("it's hard to implement this function");
        // loop {
        //     tokio::time::sleep(*self.timeout.retry_timeout()).await;

        //     let leader_id = if let Some(id) = new_leader.take() {
        //         id
        //     } else {
        //         self.fetch_leader().await
        //     };
        //     debug!("resend propose to {leader_id}");

        //     let resp_stream = match self
        //         .get_connect(leader_id)
        //         .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
        //         .propose(
        //             ProposeRequest::new(cmds_arc.as_ref()),
        //             *self.timeout.propose_timeout(),
        //         )
        //         .await {
        //             Ok(resp) => resp.into_inner(),
        //             Err(e) => {
        //                 warn!("resend_propose error:{e}");
        //                 continue;
        //             }
        //         };

        //     while let Some(resp) = resp_stream.next().await {
        //         match resp {
        //             Ok(resp) => {
        //                 if let Some(rpc::ExeResult::Error(ref e)) = resp.exe_result {
        //                     let err: ProposeError = e
        //                         .clone()
        //                         .propose_error
        //                         .ok_or(PbSerializeError::EmptyField)?
        //                         .try_into()?;
        //                     if matches!(err, ProposeError::Duplicated) {
        //                         return Ok(());
        //                     }
        //                 }
        //                 resp
        //             }
        //             Err(e) => {
        //                 // if the propose fails again, need to fetch the leader and try again
        //                 warn!("failed to resend propose, {e}");
        //                 tokio::time::sleep(*self.timeout.retry_timeout()).await;
        //                 continue;
        //             }
        //         }

        //         let mut state_w = self.state.write();
        //         state_w.check_and_update(resp.leader_id, resp.term);

        //     }
        // }
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

    async fn fast_task(
        connects: Vec<Arc<dyn ConnectApi>>,
        cmds: Arc<Vec<C>>,
        state: Arc<RwLock<State>>,
        propose_timeout: Duration,
        use_fast_path: bool,
        fast_tx: Sender<ProposeId>,
        res_tx: Sender<ProposeResult<C>>,
    ) {
        let mut fast_stream = Self::fast_round(connects, cmds, state, propose_timeout).await;
        while let Some((id, resp)) = fast_stream.next().await {
            if use_fast_path {
                match resp {
                    Ok((exe_res, success)) => {
                        if success {
                            if let Err(e) = res_tx.send((id, Ok((exe_res.unwrap(), None)))).await {
                                unreachable!("propose channel has been closed unexpectedly: {e}");
                            }
                        } else {
                            // notify slow_task to wait the `ProposeId` after sync
                            if let Err(e) = fast_tx.send(id).await {
                                unreachable!("slow task channel has been closed unexpectedly: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        if let Err(e) = res_tx.send((id, Err(e))).await {
                            unreachable!("propose channel has been closed unexpectedly: {e}");
                        }
                    }
                }
            }
        }
    }

    async fn slow_task(
        leader_connect: Arc<dyn ConnectApi>, 
        state: Arc<RwLock<State>>,
        wait_synced_timeout: Duration,
        use_fast_path: bool,
        cmds: Arc<Vec<C>>,
        mut slow_rx: Receiver<ProposeId>,
        res_tx: Sender<ProposeResult<C>>,
    ) {
        let mut slow_stream = Self::slow_round(leader_connect, state, wait_synced_timeout, cmds).await;
        let mut result_buffer = HashMap::new();
        let mut need_wait = HashSet::new();
        if use_fast_path {
            loop {
                tokio::select! {
                    slow_result = slow_stream.next() => {
                        if let Some((id, resp)) = slow_result{
                            let result = resp.map(|(_asr, er)| (er, None));
                            if need_wait.remove(&id) {
                                res_tx.send((id, result)).await.expect("res_tx channel has been closed unexpectedly");
                            } else {
                                assert!(result_buffer.insert(id, result).is_none(), "proposal result cannot be inserted into result_buffer twice");
                            }
                        } else {
                            break;
                        }
                    },
                    Some(id) = slow_rx.recv() => {
                        if let Some(result) = result_buffer.remove(&id) {
                            res_tx.send((id, result)).await.expect("res_tx channel has been closed unexpectedly");
                        } else {
                            assert!(need_wait.insert(id), "proposal result cannot be inserted into result_buffer twice");
                        }
                    }
                }
            }
        } else {
            while let Some((id, resp)) = slow_stream.next().await {
                let result = resp.map(|(asr, er)| (er, Some(asr)));
                if let Err(e) = res_tx.send((id, result)).await {
                    unreachable!("propose channel has been closed unexpectedly: {e}");
                }
            }
        }
    }

    /// propose a single command
    #[inline]
    #[instrument(skip_all)]
    #[allow(clippy::type_complexity)] // This type is not complex
    pub async fn propose(
        &self,
        cmd: C,
        use_fast_path: bool,
    ) -> Result<(<C as Command>::ER, Option<<C as Command>::ASR>), CommandProposeError<C>> {
        let cmds = Arc::new(vec![cmd]);
        let mut propose_stream = self.propose_batch(cmds, use_fast_path).await;
        loop {
            if let Some((_id, result)) = propose_stream.next().await {
                return result;
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
    #[instrument(skip_all)]
    #[allow(clippy::type_complexity)] // This type is not complex
    pub async fn propose_batch(
        &self,
        cmds: Arc<Vec<C>>,
        use_fast_path: bool,
    ) -> ProposeStream<C> {
        let connects = self
            .connects
            .iter()
            .map(|connect| Arc::clone(&connect))
            .collect_vec();
        let state = Arc::clone(&self.state);
        let propose_timeout = *self.timeout.propose_timeout();
        let wait_synced_timeout = *self.timeout.wait_synced_timeout();

        let (fast_tx, slow_rx) = channel(32);
        let (res_tx, res_rx) = channel(32);
        _ = tokio::spawn(Self::fast_task(connects, Arc::clone(&cmds), Arc::clone(&state), propose_timeout, use_fast_path, fast_tx, res_tx.clone()));
        let leader_connect = loop {
            let leader_id = self.get_leader_id().await;
            debug!("wait synced request sent to {}", leader_id);
            if let Some(conn) = self.get_connect(leader_id) {
                break conn;
            } 
        };
        _ = tokio::spawn(Self::slow_task(leader_connect, state, wait_synced_timeout, use_fast_path, cmds, slow_rx, res_tx));
        Box::pin(ReceiverStream::new(res_rx))
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

/// Inner client pool
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct ClientPool<C: Command> {
    /// TODO: add a better name
    selector: AtomicUsize,
    /// the capacity of the client pool
    capacity: usize,
    /// client pool
    pool: Vec<Arc<Client<C>>>,
}

impl<C> ClientPool<C>
where
    C: Command + 'static,
{
    /// create a new client pool
    ///
    /// # Errors
    /// Return an `ClientBuilderError` when it failed to build a client
    #[inline]
    pub async fn new(
        capacity: usize,
        local_server_id: ServerId,
        client_cfg: ClientTimeout,
        all_members: HashMap<ServerId, String>,
    ) -> Result<Self, ClientBuildError> {
        let mut pool = Vec::new();
        for _i in 0..capacity {
            pool.push(Arc::new(
                Client::builder()
                    .local_server_id(local_server_id)
                    .timeout(client_cfg)
                    .build_from_all_members(all_members.clone())
                    .await?,
            ));
        }
        Ok(Self {
            selector: AtomicUsize::new(0),
            capacity,
            pool,
        })
    }

    /// get an client from the pool
    #[inline]
    pub fn get_client(&self) -> Arc<Client<C>> {
        #[allow(clippy::integer_arithmetic)]
        let index = self.selector.fetch_add(1, Relaxed) % self.capacity;
        if let Some(client) = self.pool.get(index) {
            Arc::clone(client)
        } else {
            unreachable!("Oops, the ClientPool[{index}] should not be empty");
        }
    }
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
