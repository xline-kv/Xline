use std::{fmt::Debug, iter, marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use futures::{pin_mut, stream::FuturesUnordered, StreamExt};
use tracing::warn;

use crate::{
    cmd::Command,
    error::ProposeError,
    rpc::{self, Connect, ProposeRequest, WaitSyncedRequest},
};

/// Propose request default timeout
static PROPOSE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
/// Protocol client
pub struct Client<C: Command> {
    /// Leader index in the connections
    leader: usize,
    /// All servers addresses including leader address
    connects: Vec<Arc<Connect>>,
    /// To keep Command type
    phatom: PhantomData<C>,
}

impl<C> Client<C>
where
    C: Command + 'static,
{
    /// Create a new protocol client based on the addresses
    #[inline]
    pub async fn new(leader: usize, addrs: Vec<SocketAddr>) -> Self {
        Self {
            leader,
            connects: rpc::try_connect(
                // Addrs must start with "http" to communicate with the server
                addrs
                    .into_iter()
                    .map(|addr| {
                        let addr_str = addr.to_string();
                        if addr_str.starts_with("http") {
                            addr_str
                        } else {
                            format!("http://{addr_str}")
                        }
                    })
                    .collect(),
            )
            .await,
            phatom: PhantomData,
        }
    }

    /// The fast round of Curp protocol
    /// It broadcast the requests to all the curp servers.
    async fn fast_round(
        &self,
        cmd_arc: Arc<C>,
    ) -> Result<<C as Command>::ER, Option<<C as Command>::ER>> {
        let max_fault = self.connects.len().wrapping_div(2);
        let rpcs = self
            .connects
            .iter()
            .zip(iter::repeat_with(|| Arc::clone(&cmd_arc)))
            .map(|(connect, cmd_cloned)| async move {
                connect
                    .propose(ProposeRequest::new_from_rc(cmd_cloned)?, PROPOSE_TIMEOUT)
                    .await
            });
        let mut rpcs: FuturesUnordered<_> = rpcs.collect();

        let mut ok_cnt: usize = 0;
        let mut max_term = 0;
        let mut execute_result: Option<C::ER> = None;
        let major_cnt = max_fault
            .wrapping_add(max_fault.wrapping_add(1).wrapping_div(2))
            .wrapping_add(1);
        while ok_cnt < major_cnt || execute_result.is_none() {
            if rpcs
                .next()
                .await
                .map(|resp_result| {
                    resp_result
                        .map_err(|e| {
                            warn!("rpc error when sending `Propose` request, {e}");
                        })
                        .map(|resp| {
                            let resp = resp.into_inner();
                            match resp.term() {
                                t if t > max_term => {
                                    // state reset
                                    ok_cnt = 0;
                                    max_term = resp.term();
                                    execute_result = None;
                                    true
                                }
                                t if t < max_term => false,
                                _ => true,
                            }
                            .then(|| {
                                resp.map_or_else::<C, _, _, _>(
                                    |er: Option<C::ER>| {
                                        if let Some(er) = er {
                                            execute_result = Some(er);
                                        }
                                        ok_cnt = ok_cnt.wrapping_add(1);
                                    },
                                    |err| {
                                        // ignore error
                                        warn!("`ProposeError` met, {err}");
                                    },
                                )
                            })
                        })
                })
                .is_none()
            {
                // All requests have got responses
                break;
            }
        }
        if ok_cnt >= major_cnt {
            if let Some(er) = execute_result {
                return Ok(er);
            }
        }
        Err(execute_result)
    }

    /// The slow round of Curp protocol
    async fn slow_round(
        &self,
        cmd_arc: Arc<C>,
    ) -> Result<(<C as Command>::ASR, Option<<C as Command>::ER>), ProposeError> {
        #[allow(clippy::panic)]
        match self
            .connects
            .get(self.leader)
            .unwrap_or_else(|| {
                panic!(
                    "leader is out of bound, leader index: {}, total connect count: {}",
                    self.leader,
                    self.connects.len()
                )
            })
            .get()
            .await?
            .wait_synced(tonic::Request::new(WaitSyncedRequest::new(cmd_arc.id())?))
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                resp.map_success_error::<C, _, _, _>(
                    |(index, er)| {
                        if let Some(er) = er {
                            Ok((index, Some(er)))
                        } else {
                            Ok((index, None))
                        }
                    },
                    |e| Err(ProposeError::SyncedError(e)),
                )
            }
            Err(e) => Err(ProposeError::SyncedError(format!(
                "Sending `WaitSyncedResponse` rpc error: {e}"
            ))),
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
            futures::future::Either::Left((fast_result, slow_round)) => match fast_result {
                Ok(er) => Ok(er),
                Err(fast_er) => {
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
            },
            futures::future::Either::Right((slow_result, fast_round)) => match slow_result {
                Ok(slow_er_option) => {
                    if let (_, Some(slow_er)) = slow_er_option {
                        return Ok(slow_er);
                    }

                    match fast_round.await {
                        Ok(er) => Ok(er),
                        Err(er_option) => {
                            if let Some(er) = er_option {
                                return Ok(er);
                            }
                            Err(ProposeError::ProtocolError(
                                "There's no execution result from both fast and slow round"
                                    .to_owned(),
                            ))
                        }
                    }
                }
                Err(e) => {
                    if let Ok(er) = fast_round.await {
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

        let fast_result_option = match fast_result {
            Ok(er) => Some(er),
            Err(er_option) => er_option,
        };

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
