use std::{fmt::Debug, iter, marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use tracing::{trace, warn};

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

    /// Propose the request to servers
    /// # Errors
    ///   `ProposeError::ExecutionError` if execution error is met
    ///   `ProposeError::SyncedError` error met while syncing logs to followers
    /// # Panics
    ///   If leader index is out of bound of all the connections, panic
    #[inline]
    #[allow(clippy::too_many_lines)] // FIXME: split to smaller functions
    pub async fn propose(&self, cmd: C) -> Result<C::ER, ProposeError> {
        let ft = self.connects.len().wrapping_div(2);

        let cmd_arc = Arc::new(cmd);
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
        let major_cnt = ft
            .wrapping_add(ft.wrapping_add(1).wrapping_div(2))
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
                    |(_index, er)| {
                        if let Some(er) = er {
                            Ok(er)
                        } else if let Some(er_from_propose) = execute_result {
                            Ok(er_from_propose)
                        } else {
                            Err(ProposeError::ProtocolError(
                                "synced response should contain execution result".to_owned(),
                            ))
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

    /// Propose a command and wait for the synced index
    /// # Errors
    ///   `ProposeError::SyncedError` error met while syncing logs to followers
    ///   `ProposeError::RpcError` rpc error met, usually it's network error
    ///   `ProposeError::ProtocolError` execution result is not got from the two requests
    ///
    /// # Panics
    ///   If leader index is out of bound of all the connections, panic
    #[inline]
    pub async fn propose_indexed(&self, cmd: C) -> Result<(C::ER, C::ASR), ProposeError> {
        #[allow(clippy::panic)]
        let client = self.connects.get(self.leader).unwrap_or_else(|| {
            panic!(
                "leader is out of bound, leader index: {}, total connect count: {}",
                self.leader,
                self.connects.len()
            )
        });
        let execute_result = client
            .propose(ProposeRequest::new(&cmd)?, PROPOSE_TIMEOUT)
            .await?
            .into_inner()
            .map_or_else::<C, _, _, _>(
                |er_option: Option<C::ER>| {
                    er_option
                        .ok_or_else(|| {
                            ProposeError::ProtocolError(
                                "leader should contain execution result".to_owned(),
                            )
                        })
                        .map(Some)
                },
                |err| {
                    // ignore conflict error
                    if matches!(err, ProposeError::KeyConflict) {
                        trace!("`ProposeError` met, {err}");
                        return Ok(None);
                    }
                    Err(err)
                },
            )??;

        client
            .wait_synced(WaitSyncedRequest::new(cmd.id())?)
            .await?
            .into_inner()
            .map_success_error::<C, _, _, _>(
                |(asr, er)| {
                    Ok(match (execute_result, er) {
                        (None, None) => unreachable!("execute result should exist"),
                        (Some(er), _) | (_, Some(er)) => (er, asr),
                    })
                },
                |err_msg| Err(ProposeError::ProtocolError(err_msg)),
            )
    }
}
