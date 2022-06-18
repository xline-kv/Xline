use std::{fmt::Debug, marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use madsim::net::Endpoint;
use tracing::{error, warn};

use crate::{
    cmd::Command,
    error::ProposeError,
    message::{Propose, WaitSynced, WaitSyncedResponse},
};

/// Propose request default timeout
static PROPOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// Protocol client
pub struct Client<C: Command> {
    /// Client endpoint to send requests
    ep: Arc<Endpoint>,
    /// Leader address
    #[allow(dead_code)] // should use in propose synced
    leader: SocketAddr,
    /// All servers addresses including leader address
    addrs: Vec<SocketAddr>,
    /// To keep Command type
    phatom: PhantomData<C>,
}

impl<C> Debug for Client<C>
where
    C: Command,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("addrs", &self.addrs)
            .field("phatom", &self.phatom)
            .finish()
    }
}

impl<C> Client<C>
where
    C: Command + 'static,
{
    /// Create a new protocol client based on the addresses
    #[inline]
    pub async fn new(leader: &SocketAddr, addrs: &[SocketAddr]) -> Option<Self> {
        let ep = Endpoint::bind("127.0.0.1:0")
            .await
            .map_err(|e| {
                error!("initial endpoint error: {e}");
            })
            .ok()?;

        Some(Self {
            ep: Arc::new(ep),
            leader: *leader,
            addrs: addrs.to_owned(),
            phatom: PhantomData::<C>::default(),
        })
    }

    /// Propose the request to servers
    /// # Errors
    ///   `ProposeError::ExecutionError` if execution error is met
    ///   `ProposeError::SyncedError` error met while syncing logs to followers
    #[inline]
    pub async fn propose(&self, cmd: C) -> Result<C::ER, ProposeError> {
        let ft = self.addrs.len().wrapping_div(2);
        let rpcs = self.addrs.iter().map(|addr| {
            let cmd = cmd.clone();
            self.ep
                .call_timeout(*addr, Propose::new(cmd), PROPOSE_TIMEOUT)
        });

        let mut rpcs: FuturesUnordered<_> = rpcs.collect();

        let mut ok_cnt: usize = 0;
        let mut max_term = 0;
        let mut execute_result: Option<C::ER> = None;

        while ok_cnt
            < (ft
                .wrapping_add(ft.wrapping_add(1).wrapping_div(2))
                .wrapping_add(1))
            || execute_result.is_none()
        {
            if rpcs
                .next()
                .await
                .map(|resp_result| {
                    resp_result
                        .map_err(|e| {
                            warn!("rpc error when sending `Propose` request, {e}");
                        })
                        .map(|resp| {
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
                                resp.map_or_else(
                                    |er: Option<&<C as Command>::ER>| {
                                        if let Some(er) = er {
                                            execute_result = Some(er.clone());
                                        }
                                        ok_cnt = ok_cnt.wrapping_add(1);
                                    },
                                    |err| {
                                        // ignore error
                                        warn!("`ProposeError` met, {err}");
                                    },
                                );
                            })
                        })
                })
                .is_none()
            {
                // All requests have got responses
                break;
            }
        }

        if let Some(er) = execute_result {
            Ok(er)
        } else {
            match self
                .ep
                .call_timeout(
                    self.leader,
                    WaitSynced::<C>::new(cmd.id().clone()),
                    PROPOSE_TIMEOUT,
                )
                .await
            {
                Ok(resp) => match resp {
                    WaitSyncedResponse::Success((_index, er)) => Ok(er.unwrap_or_else(|| {
                        unreachable!("Wait Synced Response should contain the execution value");
                    })),
                    WaitSyncedResponse::Error(e) => Err(ProposeError::SyncedError(e)),
                },
                Err(e) => Err(ProposeError::SyncedError(format!(
                    "Sending `WaitSyncedResponse` rpc error: {e}"
                ))),
            }
        }
    }

    /// Propose a command and wait for the synced index
    /// # Errors
    ///   `ProposeError::SyncedError` error met while syncing logs to followers
    ///   `ProposeError::RpcError` rpc error met, usually it's network error
    ///   `ProposeError::ProtocolError` execution result is not got from the two requests
    #[inline]
    pub async fn propose_indexed(&self, cmd: C) -> Result<(C::ER, C::ASR), ProposeError> {
        let execute_result = match self
            .ep
            .call_timeout(self.leader, Propose::new(cmd.clone()), PROPOSE_TIMEOUT)
            .await
        {
            Ok(er) => er.map_or_else(
                |option_er| Ok(option_er.map(|r| Some(r.clone()))),
                |err| {
                    if let &ProposeError::KeyConflict = err {
                        // key conflict is ok as we alaways wait the sync result
                        Ok(None)
                    } else {
                        Err(err.clone())
                    }
                },
            )?,
            Err(e) => return Err(ProposeError::RpcError(format!("{e}"))),
        }
        .flatten();

        match self
            .ep
            .call_timeout(
                self.leader,
                WaitSynced::<C>::new(cmd.id().clone()),
                PROPOSE_TIMEOUT,
            )
            .await
        {
            Ok(resp) => match resp {
                WaitSyncedResponse::Success((index, option_er)) => match option_er {
                    Some(er) => Ok((er, index)),
                    None => match execute_result {
                        Some(er) => Ok((er, index)),
                        None => Err(ProposeError::ProtocolError(
                            "Can't get the execution result".to_owned(),
                        )),
                    },
                },
                WaitSyncedResponse::Error(e) => Err(ProposeError::SyncedError(e)),
            },
            Err(e) => Err(ProposeError::SyncedError(format!(
                "Sending `WaitSyncedResponse` rpc error: {e}"
            ))),
        }
    }
}
