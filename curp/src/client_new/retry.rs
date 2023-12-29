#![allow(unused)] // TODO: remove

use std::{marker::PhantomData, ops::SubAssign, sync::Arc, time::Duration};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::Future;
use tracing::warn;

use crate::{
    members::ServerId,
    rpc::{
        connect::ConnectApi, ConfChange, CurpError, FetchClusterResponse, Member, ReadState,
        Redirect,
    },
};

use super::{ClientApi, LeaderStateUpdate, ProposeResponse};

/// Backoff config
#[derive(Debug, Clone)]
pub(super) enum BackoffConfig {
    /// A fixed delay backoff
    Fixed,
    /// A exponential delay backoff
    Exponential {
        /// Control the max delay of exponential
        max_delay: Duration,
    },
}

/// Retry config to control the retry policy
#[derive(Debug, Clone)]
pub(super) struct RetryConfig {
    /// Backoff config
    backoff: BackoffConfig,
    /// Initial delay
    delay: Duration,
    /// Retry count
    count: usize,
}

/// Backoff tool
#[derive(Debug)]
struct Backoff {
    /// The retry config
    config: RetryConfig,
    /// Current delay
    cur_delay: Duration,
    /// Total RPC count
    count: usize,
}

impl RetryConfig {
    /// Create a fixed retry config
    fn new_fixed(delay: Duration, count: usize) -> Self {
        assert!(count > 0, "retry count should be larger than 0");
        Self {
            backoff: BackoffConfig::Fixed,
            delay,
            count,
        }
    }

    /// Create a exponential retry config
    fn new_exponential(delay: Duration, max_delay: Duration, count: usize) -> Self {
        assert!(count > 0, "retry count should be larger than 0");
        Self {
            backoff: BackoffConfig::Exponential { max_delay },
            delay,
            count,
        }
    }

    /// Create a backoff process
    fn init_backoff(&self) -> Backoff {
        Backoff {
            config: self.clone(),
            cur_delay: self.delay,
            count: self.count,
        }
    }
}

impl Backoff {
    /// Get the next delay duration, None means the end.
    fn next_delay(&mut self) -> Option<Duration> {
        if self.count == 0 {
            return None;
        }
        self.count.sub_assign(1);
        let mut cur = self.cur_delay;
        if let BackoffConfig::Exponential { max_delay } = self.config.backoff {
            self.cur_delay = self
                .cur_delay
                .checked_mul(2)
                .unwrap_or(self.cur_delay)
                .min(max_delay);
        }
        Some(cur)
    }
}

/// The retry client automatically retry the requests of the inner client api
/// which raises the [`tonic::Status`] error
#[derive(Debug)]
pub(super) struct Retry<Api> {
    /// Inner client
    inner: Api,
    /// Retry config
    config: RetryConfig,
}

impl<Api> Retry<Api>
where
    Api: ClientApi<Error = CurpError> + LeaderStateUpdate + Send + Sync + 'static,
{
    /// Create a retry client
    fn new(inner: Api, config: RetryConfig) -> Self {
        Self { inner, config }
    }

    /// Takes a function f and run retry.
    async fn retry<'a, R, F>(&'a self, f: impl Fn(&'a Api) -> F) -> Result<R, tonic::Status>
    where
        F: Future<Output = Result<R, CurpError>>,
    {
        let mut backoff = self.config.init_backoff();
        while let Some(delay) = backoff.next_delay() {
            let err = match f(&self.inner).await {
                Ok(res) => return Ok(res),
                Err(err) => err,
            };

            match err {
                // some errors that should not retry
                CurpError::Duplicated(_)
                | CurpError::ShuttingDown(_)
                | CurpError::InvalidConfig(_)
                | CurpError::NodeNotExists(_)
                | CurpError::NodeAlreadyExists(_)
                | CurpError::LearnerNotCatchUp(_) => {
                    return Err(tonic::Status::from(err));
                }

                // some errors that could have a retry
                CurpError::ExpiredClientId(_)
                | CurpError::KeyConflict(_)
                | CurpError::RpcTransport(_)
                | CurpError::Internal(_) => {}

                // update the cluster state if got WrongClusterVersion
                CurpError::WrongClusterVersion(_) => {
                    // the inner client should automatically update cluster state when fetch_cluster
                    if let Err(e) = self.inner.fetch_cluster(false).await {
                        warn!("fetch cluster failed, error {e:?}");
                    }
                }

                // update the leader state if got Redirect
                CurpError::Redirect(Redirect { leader_id, term }) => {
                    self.inner.update_leader(leader_id, term);
                }
            }

            warn!("retry on {} seconds later", delay.as_secs_f32());
            tokio::time::sleep(delay).await;
        }

        Err(tonic::Status::deadline_exceeded("request timeout"))
    }
}

#[async_trait]
impl<Api> ClientApi for Retry<Api>
where
    Api: ClientApi<Error = CurpError> + LeaderStateUpdate + Send + Sync + 'static,
{
    /// The client error
    type Error = tonic::Status;

    /// Inherit the command type
    type Cmd = Api::Cmd;

    /// Get the local connection when the client is on the server node.
    async fn local_connect(&self) -> Option<Arc<dyn ConnectApi>> {
        self.inner.local_connect().await
    }

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<Self::Cmd>, tonic::Status> {
        self.retry::<_, _>(|client| client.propose(cmd, use_fast_path))
            .await
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, tonic::Status> {
        self.retry::<_, _>(|client| {
            let changes_c = changes.clone();
            async move { client.propose_conf_change(changes_c).await }
        })
        .await
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), tonic::Status> {
        self.retry::<_, _>(ClientApi::propose_shutdown).await
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
    ) -> Result<(), Self::Error> {
        self.retry::<_, _>(|client| {
            let name_c = node_name.clone();
            async move { client.propose_publish(node_id, name_c).await }
        })
        .await
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &Self::Cmd) -> Result<ReadState, tonic::Status> {
        self.retry::<_, _>(|client| client.fetch_read_state(cmd))
            .await
    }

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(
        &self,
        linearizable: bool,
    ) -> Result<FetchClusterResponse, tonic::Status> {
        self.retry::<_, _>(|client| client.fetch_cluster(linearizable))
            .await
    }
}
