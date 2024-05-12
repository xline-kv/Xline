use std::{cmp::Ordering, marker::PhantomData, ops::AddAssign, sync::Arc, time::Duration};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::{Future, StreamExt};
use tonic::Response;
use tracing::{debug, warn};

use super::{state::State, ClientApi, LeaderStateUpdate, ProposeResponse, RepeatableClientApi};
use crate::{
    members::ServerId,
    quorum,
    rpc::{
        connect::ConnectApi, ConfChange, CurpError, FetchClusterRequest, FetchClusterResponse,
        FetchReadStateRequest, Member, MoveLeaderRequest, ProposeConfChangeRequest, ProposeId,
        ProposeRequest, PublishRequest, ReadState, ShutdownRequest, WaitSyncedRequest,
    },
    super_quorum,
};

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
    state: Arc<State>,
    /// Unary config
    config: UnaryConfig,
    /// marker
    phantom: PhantomData<C>,
}

impl<C: Command> Unary<C> {
    /// Create an unary client
    pub(super) fn new(state: Arc<State>, config: UnaryConfig) -> Self {
        Self {
            state,
            config,
            phantom: PhantomData,
        }
    }

    /// Get a handle `f` and apply to the leader
    /// NOTICE:
    /// The leader might be outdate if the local state is stale.
    /// `map_leader` should never be invoked in [`ClientApi::fetch_cluster`]
    /// `map_leader` might call `fetch_leader_id`, `fetch_cluster`, finally
    /// result in stack overflow.
    async fn map_leader<R, F: Future<Output = Result<R, CurpError>>>(
        &self,
        f: impl FnOnce(Arc<dyn ConnectApi>) -> F,
    ) -> Result<R, CurpError> {
        let cached_leader = self.state.leader_id().await;
        let leader_id = match cached_leader {
            Some(id) => id,
            None => <Unary<C> as ClientApi>::fetch_leader_id(self, false).await?,
        };

        self.state.map_server(leader_id, f).await
    }

    /// Send proposal to all servers
    pub(super) async fn fast_round(
        &self,
        propose_id: ProposeId,
        cmd: &C,
        token: Option<&String>,
    ) -> Result<Result<C::ER, C::Error>, CurpError> {
        let req = ProposeRequest::new(propose_id, cmd, self.state.cluster_version().await);
        let timeout = self.config.propose_timeout;

        let mut responses = self
            .state
            .for_each_server(|conn| {
                let req_c = req.clone();
                let token_c = token.cloned();
                async move { (conn.id(), conn.propose(req_c, token_c, timeout).await) }
            })
            .await;
        let super_quorum = super_quorum(responses.len());

        let mut err: Option<CurpError> = None;
        let mut execute_result: Option<C::ER> = None;
        let mut ok_cnt = 0;

        while let Some((id, resp)) = responses.next().await {
            let resp = match resp {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("propose cmd({propose_id}) to server({id}) error: {e:?}");
                    if e.should_abort_fast_round() {
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
        let req = WaitSyncedRequest::new(propose_id, self.state.cluster_version().await);
        let resp = self
            .map_leader(|conn| async move { conn.wait_synced(req, timeout).await })
            .await?
            .into_inner();
        let synced_res = resp.map_result::<C, _, _>(|res| res).map_err(|ser_err| {
            warn!("serialize error: {ser_err}");
            // Same as fast round, we blame the server for the serializing error.
            CurpError::from(ser_err)
        })?;
        debug!("slow round for cmd({}) succeed", propose_id);
        Ok(synced_res)
    }

    /// New a seq num and record it
    #[allow(clippy::unused_self)] // TODO: implement request tracker
    fn new_seq_num(&self) -> u64 {
        rand::random()
    }
}

#[async_trait]
impl<C: Command> ClientApi for Unary<C> {
    /// The error is generated from server
    type Error = CurpError;

    /// The command type
    type Cmd = C;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &C,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<C>, CurpError> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose(self, propose_id, cmd, token, use_fast_path).await
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, CurpError> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose_conf_change(self, propose_id, changes).await
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), CurpError> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose_shutdown(self, propose_id).await
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
    ) -> Result<(), Self::Error> {
        let propose_id = self.gen_propose_id()?;
        RepeatableClientApi::propose_publish(self, propose_id, node_id, node_name, node_client_urls)
            .await
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: ServerId) -> Result<(), Self::Error> {
        let req = MoveLeaderRequest::new(node_id, self.state.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.move_leader(req, timeout).await })
            .await?;
        Ok(())
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &C) -> Result<ReadState, CurpError> {
        // Same as fast_round, we blame the serializing error to the server even
        // thought it is the local error
        let req = FetchReadStateRequest::new(cmd, self.state.cluster_version().await).map_err(
            |ser_err| {
                warn!("serializing error: {ser_err}");
                CurpError::from(ser_err)
            },
        )?;
        let timeout = self.config.wait_synced_timeout;
        let state = self
            .map_leader(|conn| async move { conn.fetch_read_state(req, timeout).await })
            .await?
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
            if let Some(connect) = self.state.local_connect().await {
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
        let mut responses = self
            .state
            .for_each_server(|conn| async move {
                (
                    conn.id(),
                    conn.fetch_cluster(FetchClusterRequest { linearizable }, timeout)
                        .await
                        .map(Response::into_inner),
                )
            })
            .await;
        let quorum = quorum(responses.len());

        let mut max_term = 0;
        let mut res = None;
        let mut ok_cnt = 0;
        let mut err: Option<CurpError> = None;

        while let Some((id, resp)) = responses.next().await {
            let inner = match resp {
                Ok(r) => r,
                Err(e) => {
                    warn!("fetch cluster from {} failed, {:?}", id, e);
                    // similar to fast round
                    if e.should_abort_fast_round() {
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
                #[allow(clippy::arithmetic_side_effects)]
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
                    if let Err(e) = self.state.check_and_update(&res).await {
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
impl<C: Command> RepeatableClientApi for Unary<C> {
    /// Generate a unique propose id during the retry process.
    fn gen_propose_id(&self) -> Result<ProposeId, Self::Error> {
        let client_id = self.state.client_id();
        let seq_num = self.new_seq_num();
        Ok(ProposeId(client_id, seq_num))
    }

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        propose_id: ProposeId,
        cmd: &Self::Cmd,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error> {
        tokio::pin! {
            let fast_round = self.fast_round(propose_id, cmd, token);
            let slow_round = self.slow_round(propose_id);
        }

        let res: ProposeResponse<C> = if use_fast_path {
            match futures::future::select(fast_round, slow_round).await {
                futures::future::Either::Left((fast_result, slow_round)) => match fast_result {
                    Ok(er) => er.map(|e| {
                        #[cfg(feature = "client-metrics")]
                        super::metrics::get().client_fast_path_count.add(1, &[]);

                        (e, None)
                    }),
                    Err(fast_err) => {
                        if fast_err.should_abort_slow_round() {
                            return Err(fast_err);
                        }
                        // fallback to slow round if fast round failed
                        let sr = match slow_round.await {
                            Ok(sr) => sr,
                            Err(slow_err) => {
                                return Err(std::cmp::max_by_key(fast_err, slow_err, |err| {
                                    err.priority()
                                }))
                            }
                        };
                        sr.map(|(asr, er)| {
                            #[cfg(feature = "client-metrics")]
                            {
                                super::metrics::get().client_slow_path_count.add(1, &[]);
                                super::metrics::get()
                                    .client_fast_path_fallback_slow_path_count
                                    .add(1, &[]);
                            }

                            (er, Some(asr))
                        })
                    }
                },
                futures::future::Either::Right((slow_result, fast_round)) => match slow_result {
                    Ok(er) => er.map(|(asr, e)| {
                        #[cfg(feature = "client-metrics")]
                        super::metrics::get().client_slow_path_count.add(1, &[]);

                        (e, Some(asr))
                    }),
                    Err(slow_err) => {
                        if slow_err.should_abort_fast_round() {
                            return Err(slow_err);
                        }
                        // try to poll fast round
                        let fr = match fast_round.await {
                            Ok(fr) => fr,
                            Err(fast_err) => {
                                return Err(std::cmp::max_by_key(fast_err, slow_err, |err| {
                                    err.priority()
                                }))
                            }
                        };
                        fr.map(|er| {
                            #[cfg(feature = "client-metrics")]
                            super::metrics::get().client_fast_path_count.add(1, &[]);

                            (er, None)
                        })
                    }
                },
            }
        } else {
            match futures::future::join(fast_round, slow_round).await {
                (_, Ok(sr)) => sr.map(|(asr, er)| {
                    #[cfg(feature = "client-metrics")]
                    super::metrics::get().client_slow_path_count.add(1, &[]);

                    (er, Some(asr))
                }),
                (Ok(_), Err(err)) => return Err(err),
                (Err(fast_err), Err(slow_err)) => {
                    return Err(std::cmp::max_by_key(fast_err, slow_err, |err| {
                        err.priority()
                    }))
                }
            }
        };

        Ok(res)
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        propose_id: ProposeId,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, Self::Error> {
        let req =
            ProposeConfChangeRequest::new(propose_id, changes, self.state.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let members = self
            .map_leader(|conn| async move { conn.propose_conf_change(req, timeout).await })
            .await?
            .into_inner()
            .members;
        Ok(members)
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self, propose_id: ProposeId) -> Result<(), Self::Error> {
        let req = ShutdownRequest::new(propose_id, self.state.cluster_version().await);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.shutdown(req, timeout).await })
            .await?;
        Ok(())
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        propose_id: ProposeId,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
    ) -> Result<(), Self::Error> {
        let req = PublishRequest::new(propose_id, node_id, node_name, node_client_urls);
        let timeout = self.config.wait_synced_timeout;
        let _ig = self
            .map_leader(|conn| async move { conn.publish(req, timeout).await })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<C: Command> LeaderStateUpdate for Unary<C> {
    /// Update leader
    async fn update_leader(&self, leader_id: Option<ServerId>, term: u64) -> bool {
        self.state.check_and_update_leader(leader_id, term).await
    }
}
