use std::{
    cmp::Ordering, collections::HashMap, fmt::Debug, iter, marker::PhantomData, sync::Arc,
    time::Duration,
};

use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use tokio::{sync::broadcast, time::timeout};
use tracing::{debug, instrument, warn};
use utils::parking_lot_lock::RwLockMap;

use crate::{
    cmd::Command,
    error::ProposeError,
    message::{ServerId, TermNum},
    rpc::{
        connect::{Connect, ConnectInterface},
        FetchLeaderRequest, ProposeRequest, SyncError, SyncResult, WaitSyncedRequest,
    },
};

/// Propose request default timeout
static PROPOSE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
/// Protocol client
pub struct Client<C: Command> {
    /// Current leader and term
    state: RwLock<State>,
    /// All servers addresses including leader address
    connects: Vec<Arc<Connect>>,
    /// To keep Command type
    phatom: PhantomData<C>,
}

/// State of a client
#[derive(Debug)]
struct State {
    /// Current leader
    leader: Option<ServerId>,
    /// Current term
    term: TermNum,
    /// When a new leader is set, notify
    leader_notify: Arc<Event>,
    /// Send leader changes
    leader_tx: broadcast::Sender<ServerId>,
}

impl State {
    /// Create the initial client state
    fn new() -> Self {
        let (leader_tx, _) = broadcast::channel(1);
        Self {
            leader: None,
            term: 0,
            leader_notify: Arc::new(Event::new()),
            leader_tx,
        }
    }

    /// Set the leader and notify a waiter
    fn set_leader(&mut self, id: ServerId) {
        debug!("client update its leader to {id}");
        if self.leader.as_ref().map_or(true, |prev_id| prev_id != &id) {
            let _ignored = self.leader_tx.send(id.clone()).ok(); // it's ok to have no receiver
        }
        self.leader = Some(id);
        self.leader_notify.notify(1);
    }

    /// Update to the newest term and reset local cache
    fn update_to_term(&mut self, term: TermNum) {
        debug_assert!(self.term <= term);
        self.term = term;
        self.leader = None;
    }
}

impl<C> Client<C>
where
    C: Command + 'static,
{
    /// Timeout
    // TODO: make it configurable
    const TIMEOUT: Duration = Duration::from_secs(1);

    /// Wait Synced Timeout
    // TODO: make it configurable
    const WAIT_SYNCED_TIMEOUT: Duration = Duration::from_secs(2);

    /// Create a new protocol client based on the addresses
    #[inline]
    pub async fn new(addrs: HashMap<ServerId, String>) -> Self {
        Self {
            state: RwLock::new(State::new()),
            connects: Connect::try_connect(addrs).await,
            phatom: PhantomData,
        }
    }

    /// The fast round of Curp protocol
    /// It broadcast the requests to all the curp servers.
    #[instrument(skip(self))]
    async fn fast_round(
        &self,
        cmd_arc: Arc<C>,
    ) -> Result<(Option<<C as Command>::ER>, bool), ProposeError> {
        let max_fault = self.connects.len().wrapping_div(2);
        let req = ProposeRequest::new_from_rc(cmd_arc)?;
        let mut rpcs: FuturesUnordered<_> = self
            .connects
            .iter()
            .zip(iter::repeat(req))
            .map(|(connect, req_cloned)| connect.propose(req_cloned, PROPOSE_TIMEOUT))
            .collect();

        let mut ok_cnt: usize = 0;
        let mut execute_result: Option<C::ER> = None;
        let major_cnt = max_fault
            .wrapping_add(max_fault.wrapping_add(1).wrapping_div(2))
            .wrapping_add(1);
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
            if (ok_cnt >= major_cnt) && execute_result.is_some() {
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
        let notify = Arc::clone(&self.state.read().leader_notify);

        loop {
            // fetch leader id
            let leader_id = loop {
                if let Some(id) = self.state.read().leader.clone() {
                    break id;
                }
                if timeout(Self::TIMEOUT, notify.listen()).await.is_err() {
                    // maybe the fast path fails to set the leader, need to fetch leader proactively
                    self.fetch_leader().await;
                }
            };

            debug!("wait synced request sent to {}", leader_id);
            let resp = match self
                .connects
                .iter()
                .find(|conn| conn.id() == &leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .wait_synced(WaitSyncedRequest::new(cmd.id())?, Self::WAIT_SYNCED_TIMEOUT)
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("wait synced rpc error: {e}");
                    // maybe fail due to timeout, then we don't know who is the leader
                    self.fetch_leader().await;
                    continue;
                }
            };

            match resp.into::<C>()? {
                SyncResult::Success { er, asr } => {
                    debug!("slow round for cmd {:?} succeeded", cmd.id());
                    return Ok((asr, er));
                }
                SyncResult::Error(SyncError::Redirect(new_leader, term)) => {
                    if let Some(new_leader_id) = new_leader {
                        let mut state = self.state.write();
                        if state.term <= term {
                            state.leader = Some(new_leader_id);
                            state.term = term;
                            continue; // redirect succeeds, try again
                        }
                    }
                    // redirect failed: the client should fetch leader itself
                    self.fetch_leader().await;
                }
                SyncResult::Error(SyncError::Timeout) => {
                    return Err(ProposeError::SyncedError("wait sync timeout".to_owned()));
                }
                SyncResult::Error(e) => {
                    return Err(ProposeError::SyncedError(format!("{:?}", e)));
                }
            }
        }
    }

    /// Send fetch leader requests to all servers until there is a leader
    /// Note: The fetched leader may still be outdated
    async fn fetch_leader(&self) {
        loop {
            let mut rpcs: FuturesUnordered<_> = self
                .connects
                .iter()
                .map(|connect| async {
                    (
                        connect.id().clone(),
                        connect
                            .fetch_leader(FetchLeaderRequest::new(), Self::TIMEOUT)
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
                state.set_leader(leader);
                return;
            }

            // wait until the election is completed
            // TODO: let user configure it according to average leader election cost
            tokio::time::sleep(Self::RETRY_INTERVAL).await;
        }
    }

    /// Retry interval
    const RETRY_INTERVAL: Duration = Duration::from_micros(500);

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
    ///   `ProposeError::ProtocolError` execution result is not got from the two requests
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

    /// Get the current leader.
    #[inline]
    pub fn leader(&self) -> Option<ServerId> {
        self.state.read().leader.clone()
    }

    /// Get the receiver for leader changes
    #[inline]
    pub fn leader_rx(&self) -> broadcast::Receiver<ServerId> {
        self.state.read().leader_tx.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn get_connects(&self) -> &[Arc<Connect>] {
        self.connects.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::unwrap_used)]
    #[tokio::test]
    async fn will_get_notify_on_leader_changes() {
        let mut state = State::new();
        let mut rx = state.leader_tx.subscribe();

        state.set_leader("S1".to_owned());
        assert_eq!(rx.recv().await.unwrap().as_str(), "S1");

        state.set_leader("S2".to_owned());
        state.set_leader("S3".to_owned());
        assert!(rx.recv().await.is_err());
        assert_eq!(rx.recv().await.unwrap().as_str(), "S3");
    }
}
