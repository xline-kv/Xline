use std::{ops::SubAssign, time::Duration};

use async_trait::async_trait;
use futures::Future;
use tokio::task::JoinHandle;
use tracing::warn;

use super::{ClientApi, LeaderStateUpdate, ProposeResponse, RepeatableClientApi};
use crate::{
    members::ServerId,
    rpc::{ConfChange, CurpError, FetchClusterResponse, Member, ReadState, Redirect},
};

/// Backoff config
#[derive(Debug, Clone)]
enum BackoffConfig {
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
    pub(super) fn new_fixed(delay: Duration, count: usize) -> Self {
        assert!(count > 0, "retry count should be larger than 0");
        Self {
            backoff: BackoffConfig::Fixed,
            delay,
            count,
        }
    }

    /// Create a exponential retry config
    pub(super) fn new_exponential(delay: Duration, max_delay: Duration, count: usize) -> Self {
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
        let cur = self.cur_delay;
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
    /// Background task handle
    bg_handle: Option<JoinHandle<()>>,
}

impl<Api> Drop for Retry<Api> {
    fn drop(&mut self) {
        if let Some(handle) = self.bg_handle.as_ref() {
            handle.abort();
        }
    }
}

impl<Api> Retry<Api>
where
    Api: RepeatableClientApi<Error = CurpError> + LeaderStateUpdate + Send + Sync + 'static,
{
    /// Create a retry client
    pub(super) fn new(inner: Api, config: RetryConfig, bg_handle: Option<JoinHandle<()>>) -> Self {
        Self {
            inner,
            config,
            bg_handle,
        }
    }

    /// Takes a function f and run retry.
    async fn retry<'a, R, F>(&'a self, f: impl Fn(&'a Api) -> F) -> Result<R, tonic::Status>
    where
        F: Future<Output = Result<R, CurpError>>,
    {
        let mut backoff = self.config.init_backoff();
        let mut last_err = None;
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
                | CurpError::Internal(_)
                | CurpError::LeaderTransfer(_) => {}

                // update leader state if we got a rpc transport error
                CurpError::RpcTransport(_) => {
                    if let Err(e) = self.inner.fetch_leader_id(true).await {
                        warn!("fetch leader failed, error {e:?}");
                    }
                }

                // update the cluster state if got WrongClusterVersion
                CurpError::WrongClusterVersion(_) => {
                    // the inner client should automatically update cluster state when fetch_cluster
                    if let Err(e) = self.inner.fetch_cluster(true).await {
                        warn!("fetch cluster failed, error {e:?}");
                    }
                }

                // update the leader state if got Redirect
                CurpError::Redirect(Redirect { leader_id, term }) => {
                    let _ig = self.inner.update_leader(leader_id, term).await;
                }
            }

            #[cfg(feature = "client-metrics")]
            super::metrics::get().client_retry_count.add(1, &[]);

            warn!(
                "got error: {err:?}, retry on {} seconds later",
                delay.as_secs_f32()
            );
            last_err = Some(err);
            tokio::time::sleep(delay).await;
        }

        Err(tonic::Status::deadline_exceeded(format!(
            "request timeout, last error: {:?}",
            last_err.unwrap_or_else(|| unreachable!("last error must be set"))
        )))
    }
}

#[async_trait]
impl<Api> ClientApi for Retry<Api>
where
    Api: RepeatableClientApi<Error = CurpError> + LeaderStateUpdate + Send + Sync + 'static,
{
    /// The client error
    type Error = tonic::Status;

    /// Inherit the command type
    type Cmd = Api::Cmd;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<Self::Cmd>, tonic::Status> {
        let propose_id = self.inner.gen_propose_id()?;
        self.retry::<_, _>(|client| {
            RepeatableClientApi::propose(client, propose_id, cmd, token, use_fast_path)
        })
        .await
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, tonic::Status> {
        let propose_id = self.inner.gen_propose_id()?;
        self.retry::<_, _>(|client| {
            let changes_c = changes.clone();
            RepeatableClientApi::propose_conf_change(client, propose_id, changes_c)
        })
        .await
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), tonic::Status> {
        let propose_id = self.inner.gen_propose_id()?;
        self.retry::<_, _>(|client| RepeatableClientApi::propose_shutdown(client, propose_id))
            .await
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
    ) -> Result<(), Self::Error> {
        let propose_id = self.inner.gen_propose_id()?;
        self.retry::<_, _>(|client| {
            let name_c = node_name.clone();
            let node_client_urls_c = node_client_urls.clone();
            RepeatableClientApi::propose_publish(
                client,
                propose_id,
                node_id,
                name_c,
                node_client_urls_c,
            )
        })
        .await
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: u64) -> Result<(), Self::Error> {
        self.retry::<_, _>(|client| client.move_leader(node_id))
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

/// Tests for backoff
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RetryConfig;

    #[test]
    fn test_fixed_backoff_works() {
        let config = RetryConfig::new_fixed(Duration::from_secs(1), 3);
        let mut backoff = config.init_backoff();
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next_delay(), None);
    }

    #[test]
    fn test_exponential_backoff_works() {
        let config =
            RetryConfig::new_exponential(Duration::from_secs(1), Duration::from_secs(5), 4);
        let mut backoff = config.init_backoff();
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(1)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(2)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(4)));
        assert_eq!(backoff.next_delay(), Some(Duration::from_secs(5))); // 8 > 5
        assert_eq!(backoff.next_delay(), None);
    }
}
