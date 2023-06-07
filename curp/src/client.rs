use std::{cmp::Ordering, collections::HashMap, fmt::Debug, iter, marker::PhantomData, sync::Arc};

use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use tokio::time::timeout;
use tracing::{debug, instrument, warn};
use utils::{config::ClientTimeout, parking_lot_lock::RwLockMap};

use crate::{
    cmd::{Command, ProposeId},
    error::ProposeError,
    rpc::{
        self, connect::ConnectApi, FetchLeaderRequest, FetchReadStateRequest, ProposeRequest,
        ReadState as PbReadState, SyncError, SyncResult, WaitSyncedRequest,
    },
    LogIndex, ServerId,
};

/// Protocol client
pub struct Client<C: Command> {
    /// local server id. Only use in an inner client.
    local_server_id: Option<ServerId>,
    /// Current leader and term
    state: RwLock<State>,
    /// All servers's `Connect`
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
    /// Curp client timeout settings
    timeout: ClientTimeout,
    /// To keep Command type
    phantom: PhantomData<C>,
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
#[derive(Debug)]
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
    fn new() -> Self {
        Self {
            leader: None,
            term: 0,
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
    /// Create a new protocol client based on the addresses
    #[inline]
    pub async fn new(
        self_id: Option<ServerId>,
        addrs: HashMap<ServerId, String>,
        timeout: ClientTimeout,
    ) -> Self {
        Self {
            local_server_id: self_id,
            state: RwLock::new(State::new()),
            connects: rpc::connect(addrs).await,
            timeout,
            phantom: PhantomData,
        }
    }

    /// The fast round of Curp protocol
    /// It broadcast the requests to all the curp servers.
    #[instrument(skip(self))]
    async fn fast_round(
        &self,
        cmd_arc: Arc<C>,
    ) -> Result<(Option<<C as Command>::ER>, bool), ProposeError> {
        let req = ProposeRequest::new(cmd_arc.as_ref())?;
        let mut rpcs: FuturesUnordered<_> = self
            .connects
            .values()
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
                        if let Some(ref leader_id) = resp.leader_id {
                            state.update_to_term(resp.term());
                            state.set_leader(leader_id.clone());
                            execute_result = None;
                        }
                    }
                    Ordering::Equal => {
                        if let Some(ref leader_id) = resp.leader_id {
                            if state.leader.is_none() {
                                state.set_leader(leader_id.clone());
                            }
                            assert_eq!(
                                state.leader.as_ref(),
                                Some(leader_id),
                                "there should never be two leader in one term"
                            );
                        }
                    }
                    Ordering::Greater => {}
                }
            });
            resp.map_or_else::<C, _, _, _>(
                |er| {
                    if let Some(er) = er {
                        assert!(execute_result.is_none(), "should not set exe result twice");
                        execute_result = Some(er);
                    }
                    ok_cnt = ok_cnt.wrapping_add(1);
                    Ok(())
                },
                |err| {
                    if let ProposeError::ExecutionError(_) = err {
                        // Only `ProposeError::ExecutionError` will be reported to upper function
                        return Err(err);
                    }
                    warn!("Propose error: {}", err);
                    Ok(())
                },
            )??;
            if (ok_cnt >= superquorum) && execute_result.is_some() {
                debug!("fast round succeeds");
                return Ok((execute_result, true));
            }
        }
        Ok((execute_result, false))
    }

    /// The slow round of Curp protocol
    #[instrument(skip(self))]
    async fn slow_round(
        &self,
        cmd: Arc<C>,
    ) -> Result<(<C as Command>::ASR, <C as Command>::ER), ProposeError> {
        let retry_timeout = *self.timeout.retry_timeout();
        loop {
            // fetch leader id
            let leader_id = self.get_leader_id().await;

            debug!("wait synced request sent to {}", leader_id);
            let resp = match self
                .connects
                .get(&leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .wait_synced(
                    WaitSyncedRequest::new(cmd.id())?,
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

            match resp.into::<C>()? {
                SyncResult::Success { er, asr } => {
                    debug!("slow round for cmd({}) succeeded", cmd.id());
                    return Ok((asr, er));
                }
                SyncResult::Error(SyncError::Redirect(new_leader, term)) => {
                    let new_leader = new_leader.and_then(|id| {
                        let mut state = self.state.write();
                        (state.term <= term).then(|| {
                            state.leader = Some(id.clone());
                            state.term = term;
                            id
                        })
                    });
                    self.resend_propose(Arc::clone(&cmd), new_leader).await?; // resend the propose to the new leader
                }
                SyncResult::Error(SyncError::Timeout) => {
                    return Err(ProposeError::SyncedError("wait sync timeout".to_owned()));
                }
                SyncResult::Error(e) => {
                    return Err(ProposeError::SyncedError(format!("{e:?}")));
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
                .connects
                .get(&leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .propose(
                    ProposeRequest::new(cmd.as_ref())?,
                    *self.timeout.propose_timeout(),
                )
                .await;

            let resp = match resp {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    if matches!(e, ProposeError::Duplicated) {
                        return Ok(());
                    }
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
            let mut rpcs: FuturesUnordered<_> = self
                .connects
                .values()
                .map(|connect| async {
                    (
                        connect.id().clone(),
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
            let majority_cnt = self.connects.len() / 2 + 1;
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
                state.set_leader(leader.clone());
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
            if let Some(id) = self.state.read().leader.clone() {
                return id;
            }
            if timeout(retry_timeout, notify.listen()).await.is_err() {
                break self.fetch_leader().await;
            }
        }
    }

    /// Propose the request to servers
    /// # Errors
    ///   `ProposeError::ExecutionError` if execution error is met
    ///   `ProposeError::SyncedError` error met while syncing logs to followers
    /// # Panics
    ///   If leader index is out of bound of all the connections, panic
    #[inline]
    #[allow(clippy::too_many_lines)] // FIXME: split to smaller functions
    pub async fn propose(&self, cmd: C) -> Result<C::ER, ProposeError> {
        let cmd_arc = Arc::new(cmd);
        let fast_round = self.fast_round(Arc::clone(&cmd_arc));
        let slow_round = self.slow_round(cmd_arc);

        pin_mut!(fast_round);
        pin_mut!(slow_round);

        // Wait for the fast and slow round at the same time
        match futures::future::select(fast_round, slow_round).await {
            futures::future::Either::Left((fast_result, slow_round)) => {
                let (fast_er, success) = fast_result?;
                if success {
                    #[allow(clippy::unwrap_used)]
                    // when success is true fast_er must be Some
                    Ok(fast_er.unwrap())
                } else {
                    let (_asr, er) = slow_round.await?;
                    Ok(er)
                }
            }
            futures::future::Either::Right((slow_result, fast_round)) => match slow_result {
                Ok((_asr, er)) => Ok(er),
                Err(e) => {
                    if let Ok((Some(er), true)) = fast_round.await {
                        return Ok(er);
                    }
                    Err(e)
                }
            },
        }
    }

    /// Propose a command and wait for the synced index
    /// # Errors
    ///   `ProposeError::SyncedError` error met while syncing logs to followers
    ///   `ProposeError::RpcError` rpc error met, usually it's network error
    ///
    /// # Panics
    ///   If leader index is out of bound of all the connections, panic
    #[inline]
    #[allow(clippy::else_if_without_else)] // the else is redundant
    pub async fn propose_indexed(&self, cmd: C) -> Result<(C::ER, C::ASR), ProposeError> {
        let cmd_arc = Arc::new(cmd);
        let fast_round = self.fast_round(Arc::clone(&cmd_arc));
        let slow_round = self.slow_round(cmd_arc);

        #[allow(clippy::integer_arithmetic)] // tokio framework triggers
        let (_fast_result, slow_result) = tokio::join!(fast_round, slow_round);

        match slow_result {
            Ok((asr, er)) => Ok((er, asr)),
            Err(e) => Err(e),
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
                .connects
                .get(&leader_id)
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
    async fn fetch_local_leader_info(&self) -> Result<(Option<ServerId>, u64), ProposeError> {
        if let Some(ref local_server) = self.local_server_id {
            let resp = self
                .connects
                .get(local_server)
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
    use super::*;

    #[test]
    fn superquorum_should_work() {
        assert_eq!(superquorum(11), 9);
        assert_eq!(superquorum(97), 73);
        assert_eq!(superquorum(31), 24);
        assert_eq!(superquorum(59), 45);
    }
}
