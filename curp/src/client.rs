use crate::{
    cmd::Command,
    error::ProposeError,
    message::TermNum,
    rpc::{
        self, Connect, FetchLeaderRequest, ProposeRequest, SyncError, SyncResult, WaitSyncedRequest,
    },
    ServerId,
};
use event_listener::Event;
use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::{collections::HashMap, fmt::Debug, iter, marker::PhantomData, sync::Arc, time::Duration};
use tracing::{debug, instrument, warn};

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
    /// Leader set event
    leader_notify: Arc<Event>,
}

impl State {
    /// Create a client state
    fn new() -> Self {
        Self {
            leader: None,
            term: 0,
            leader_notify: Arc::new(Event::new()),
        }
    }

    /// Set the leader and notify a waiter
    fn set_leader(&mut self, id: ServerId) {
        debug!("client set leader to {id}");
        self.leader = Some(id);
        self.leader_notify.notify(1);
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
            state: RwLock::new(State::new()), // null
            connects: rpc::try_connect(
                // Addrs must start with "http" to communicate with the server
                addrs
                    .into_iter()
                    .map(|(id, addr)| {
                        if addr.starts_with("http") {
                            (id, addr)
                        } else {
                            (id, format!("http://{addr}"))
                        }
                    })
                    .collect(),
                #[cfg(test)]
                Arc::new(AtomicBool::new(true)),
            )
            .await,
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
            .zip(iter::repeat_with(|| req.clone()))
            .map(|(connect, req_cloned)| async {
                (
                    connect.id.clone(),
                    connect.propose(req_cloned, PROPOSE_TIMEOUT).await,
                )
            })
            .collect();

        let mut ok_cnt: usize = 0;
        let mut execute_result: Option<C::ER> = None;
        let major_cnt = max_fault
            .wrapping_add(max_fault.wrapping_add(1).wrapping_div(2))
            .wrapping_add(1);
        while let Some((id, resp_result)) = rpcs.next().await {
            let resp = match resp_result {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("Propose error: {}", e);
                    continue;
                }
            };
            let term_valid = {
                let mut state = self.state.write();
                let valid = match resp.term() {
                    t if t > state.term => {
                        // state reset
                        ok_cnt = 0;
                        state.term = resp.term();
                        execute_result = None;
                        true
                    }
                    t if t < state.term => false,
                    _ => true,
                };
                if resp.is_leader && resp.term == state.term {
                    state.set_leader(id);
                }
                valid
            };
            if term_valid {
                resp.map_or_else::<C, _, _, _>(
                    |er| {
                        if let Some(er) = er {
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
            }
            if (ok_cnt >= major_cnt) && execute_result.is_some() {
                debug!("fast round succeeded");
                return Ok((execute_result, true));
            }
        }
        Ok((execute_result, false))
    }

    /// The slow round of Curp protocol
    #[instrument(skip(self))]
    async fn slow_round(
        &self,
        cmd_arc: Arc<C>,
    ) -> Result<(<C as Command>::ASR, Option<<C as Command>::ER>), ProposeError> {
        let notify = Arc::clone(&self.state.read().leader_notify);

        loop {
            let req = WaitSyncedRequest::new(cmd_arc.id())?;

            // fetch leader id
            let leader_id = loop {
                if let Some(id) = self.state.read().leader.clone() {
                    break id;
                }
                notify.listen().await;
            };

            debug!("wait synced request sent to {}", leader_id);
            let resp = match self
                .connects
                .iter()
                .find(|conn| conn.id == leader_id)
                .unwrap_or_else(|| unreachable!("leader {leader_id} not found"))
                .wait_synced(req, Self::WAIT_SYNCED_TIMEOUT)
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("wait synced rpc error: {e}");
                    self.fetch_leader().await;
                    continue;
                }
            };

            match resp.into_result::<C>()? {
                SyncResult::Success { asr, er } => {
                    debug!("slow round succeeded");
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
                        connect.id.clone(),
                        connect
                            .fetch_leader(FetchLeaderRequest::new(), Self::TIMEOUT)
                            .await,
                    )
                })
                .collect();
            let mut max_term = 0;
            let mut leader = None;

            while let Some((id, resp)) = rpcs.next().await {
                let resp = match resp {
                    Ok(resp) => resp.into_inner(),
                    Err(e) => {
                        warn!("fetch leader from {} failed, {:?}", id, e);
                        continue;
                    }
                };
                if resp.term > max_term {
                    max_term = resp.term;
                    leader = None;
                }
                if let Some(leader_id) = resp.leader_id {
                    if resp.term >= max_term {
                        leader = Some(leader_id);
                    }
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
            tokio::time::sleep(Duration::from_micros(500)).await;
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
                    let slow_result = slow_round.await?;
                    if let (_, Some(slow_er)) = slow_result {
                        return Ok(slow_er);
                    }
                    if let Some(er) = fast_er {
                        return Ok(er);
                    }
                    Err(ProposeError::ProtocolError(
                        "There's no execution result from both fast and slow round".to_owned(),
                    ))
                }
            }
            futures::future::Either::Right((slow_result, fast_round)) => match slow_result {
                Ok(slow_er_option) => {
                    if let (_, Some(slow_er)) = slow_er_option {
                        return Ok(slow_er);
                    }
                    if let (Some(er), _) = fast_round.await? {
                        Ok(er)
                    } else {
                        Err(ProposeError::ProtocolError(
                            "There's no execution result from both fast and slow round".to_owned(),
                        ))
                    }
                }
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
        let (fast_result, slow_result) = tokio::join!(fast_round, slow_round);

        let fast_result_option = fast_result?.0;

        match slow_result {
            Ok((asr, er_option)) => {
                if let Some(er) = er_option {
                    return Ok((er, asr));
                } else if let Some(er) = fast_result_option {
                    return Ok((er, asr));
                }
                Err(ProposeError::ProtocolError(
                    "There's no execution result from both fast and slow round".to_owned(),
                ))
            }
            Err(e) => Err(e),
        }
    }
}
