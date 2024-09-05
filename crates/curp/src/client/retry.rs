use std::{
    ops::SubAssign,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use futures::Future;
use parking_lot::RwLock;
use tracing::{debug, warn};

use super::{
    cluster_state::{ClusterState, ClusterStateInit, ClusterStateSuper},
    config::Config,
    fetch::Fetch,
    keep_alive::{KeepAlive, KeepAliveHandle},
    ClientApi, ProposeIdGuard, ProposeResponse, RepeatableClientApi,
};
use crate::{
    members::ServerId,
    rpc::{connects, ConfChange, CurpError, FetchClusterResponse, Member, ProposeId, ReadState},
    tracker::Tracker,
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

/// The context of a retry
#[derive(Debug)]
pub(crate) struct Context {
    /// The propose id
    propose_id: ProposeId,
    /// First incomplete seqence
    first_incomplete: u64,
    /// The current cluster state
    cluster_state: ClusterState,
}

impl Context {
    /// Creates a new `Context`
    pub(crate) fn new(
        propose_id: ProposeId,
        first_incomplete: u64,
        cluster_state: ClusterState,
    ) -> Self {
        Self {
            propose_id,
            first_incomplete,
            cluster_state,
        }
    }

    /// Returns the current propose id
    pub(crate) fn propose_id(&self) -> ProposeId {
        self.propose_id
    }

    /// Returns the first incomplete sequence number
    pub(crate) fn first_incomplete(&self) -> u64 {
        self.first_incomplete
    }

    /// Returns the current client id
    pub(crate) fn cluster_state(&self) -> ClusterState {
        self.cluster_state.clone()
    }
}

/// Command tracker
#[derive(Debug, Default)]
struct CmdTracker {
    /// Last sent sequence number
    last_sent_seq: AtomicU64,
    /// Request tracker
    tracker: RwLock<Tracker>,
}

impl CmdTracker {
    /// New a seq num and record it
    fn new_seq_num(&self) -> u64 {
        self.last_sent_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Generate a unique propose id during the retry process.
    fn gen_propose_id(&self, client_id: u64) -> ProposeIdGuard<'_> {
        let seq_num = self.new_seq_num();
        ProposeIdGuard::new(&self.tracker, ProposeId(client_id, seq_num))
    }

    /// Generate a unique propose id during the retry process.
    fn first_incomplete(&self) -> u64 {
        self.tracker.read().first_incomplete()
    }
}

/// A shared cluster state
#[derive(Debug)]
pub(crate) struct ClusterStateShared {
    /// Inner state
    inner: RwLock<ClusterStateSuper>,
    /// Fetch cluster object
    fetch: Fetch,
}

impl ClusterStateShared {
    /// Creates a new `ClusterStateShared`
    fn new(inner: ClusterStateSuper, fetch: Fetch) -> Self {
        Self {
            inner: RwLock::new(inner),
            fetch,
        }
    }

    /// Fetch and updates current state
    ///
    /// Returns the fetched cluster state
    pub(crate) async fn fetch_and_update(&self) -> Result<ClusterState, CurpError> {
        let current = self.inner.read().clone();
        let (new_state, _) = self.fetch.fetch_cluster(current).await?;
        *self.inner.write() = ClusterStateSuper::Ready(new_state.clone());
        debug!("cluster state updates to: {new_state:?}");

        Ok(new_state)
    }

    /// Retrieves the cluster state if it's ready, or fetches and updates it if not.
    pub(crate) async fn ready_or_fetch(&self) -> Result<ClusterState, CurpError> {
        let current = self.inner.read().clone();
        match current {
            ClusterStateSuper::Init(init) => self.fetch_and_update().await,
            ClusterStateSuper::Ready(ready) => Ok(ready),
        }
    }
}

/// The retry client automatically retry the requests of the inner client api
/// which raises the [`tonic::Status`] error
#[derive(Debug)]
pub(super) struct Retry<Api> {
    /// Inner client
    inner: Api,
    /// Retry config
    retry_config: RetryConfig,
    /// Cluster state
    cluster_state: Arc<ClusterStateShared>,
    /// Keep alive client
    keep_alive: KeepAliveHandle,
    /// Fetch cluster object
    fetch: Fetch,
    /// Command tracker
    tracker: CmdTracker,
}

impl<Api> Retry<Api>
where
    Api: RepeatableClientApi<Error = CurpError> + Send + Sync + 'static,
{
    /// Create a retry client
    pub(super) fn new(
        inner: Api,
        retry_config: RetryConfig,
        keep_alive: KeepAlive,
        fetch: Fetch,
        cluster_state_init: ClusterStateInit,
    ) -> Self {
        let cluster_state = Arc::new(ClusterStateShared::new(
            ClusterStateSuper::Init(cluster_state_init),
            fetch.clone(),
        ));
        let keep_alive_handle = keep_alive.spawn_keep_alive(Arc::clone(&cluster_state));
        Self {
            inner,
            retry_config,
            cluster_state,
            keep_alive: keep_alive_handle,
            fetch,
            tracker: CmdTracker::default(),
        }
    }

    /// Takes a function f and run retry.
    async fn retry<'a, R, F>(
        &'a self,
        f: impl Fn(&'a Api, Context) -> F,
    ) -> Result<R, tonic::Status>
    where
        F: Future<Output = Result<R, CurpError>>,
    {
        let mut backoff = self.retry_config.init_backoff();
        let mut last_err = None;
        let client_id = self.keep_alive.wait_id_update(0).await;
        let propose_id_guard = self.tracker.gen_propose_id(client_id);
        let first_incomplete = self.tracker.first_incomplete();
        while let Some(delay) = backoff.next_delay() {
            let cluster_state = self.cluster_state.ready_or_fetch().await?;
            let context = Context::new(*propose_id_guard, first_incomplete, cluster_state.clone());
            let result = tokio::select! {
                result = f(&self.inner, context) => result,
                _ = self.keep_alive.wait_id_update(client_id) => {
                    return Err(CurpError::expired_client_id().into());
                },
            };
            let err = match result {
                Ok(res) => return Ok(res),
                Err(err) => err,
            };
            self.handle_err(&err, cluster_state).await?;

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

    /// Handles errors before another retry
    async fn handle_err(
        &self,
        err: &CurpError,
        cluster_state: ClusterState,
    ) -> Result<(), tonic::Status> {
        match *err {
            // some errors that should not retry
            CurpError::Duplicated(())
            | CurpError::ShuttingDown(())
            | CurpError::InvalidConfig(())
            | CurpError::NodeNotExists(())
            | CurpError::NodeAlreadyExists(())
            | CurpError::LearnerNotCatchUp(()) => {
                return Err(tonic::Status::from(err.clone()));
            }

            // some errors that could have a retry
            CurpError::ExpiredClientId(())
            | CurpError::KeyConflict(())
            | CurpError::Internal(_)
            | CurpError::LeaderTransfer(_) => {}

            // Some error that needs to update cluster state
            CurpError::RpcTransport(())
            | CurpError::WrongClusterVersion(())
            | CurpError::Redirect(_) // FIXME: The redirect error needs to include full cluster state
            | CurpError::Zombie(()) => {
                // TODO: Prevent concurrent updating cluster state
                let _ignore = self.cluster_state.fetch_and_update().await?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<Api> ClientApi for Retry<Api>
where
    Api: RepeatableClientApi<Error = CurpError> + Send + Sync + 'static,
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
        self.retry::<_, _>(|client, ctx| async move {
            RepeatableClientApi::propose(client, cmd, token, use_fast_path, ctx).await
        })
        .await
    }

    /// Send propose configuration changes to the cluster
    async fn propose_conf_change(
        &self,
        changes: Vec<ConfChange>,
    ) -> Result<Vec<Member>, tonic::Status> {
        self.retry::<_, _>(|client, ctx| {
            let changes_c = changes.clone();
            async move { RepeatableClientApi::propose_conf_change(client, changes_c, ctx).await }
        })
        .await
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), tonic::Status> {
        self.retry::<_, _>(|client, ctx| async move {
            RepeatableClientApi::propose_shutdown(client, ctx).await
        })
        .await
    }

    /// Send propose to publish a node id and name
    async fn propose_publish(
        &self,
        node_id: ServerId,
        node_name: String,
        node_client_urls: Vec<String>,
    ) -> Result<(), Self::Error> {
        self.retry::<_, _>(|client, ctx| {
            let name_c = node_name.clone();
            let node_client_urls_c = node_client_urls.clone();
            async move {
                RepeatableClientApi::propose_publish(
                    client,
                    node_id,
                    name_c,
                    node_client_urls_c,
                    ctx,
                )
                .await
            }
        })
        .await
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: u64) -> Result<(), Self::Error> {
        self.retry::<_, _>(|client, ctx| client.move_leader(node_id, ctx))
            .await
    }

    /// Send fetch read state from leader
    async fn fetch_read_state(&self, cmd: &Self::Cmd) -> Result<ReadState, tonic::Status> {
        self.retry::<_, _>(|client, ctx| client.fetch_read_state(cmd, ctx))
            .await
    }

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    ///
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(
        &self,
        linearizable: bool,
    ) -> Result<FetchClusterResponse, tonic::Status> {
        self.retry::<_, _>(|client, ctx| async move {
            let (_, resp) = self.fetch.fetch_cluster(ctx.cluster_state()).await?;
            Ok(resp)
        })
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
