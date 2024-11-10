#![allow(clippy::same_name_method)] // TODO: use another name

use std::{
    collections::BTreeSet,
    ops::SubAssign,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use curp_external_api::cmd::Command;
use futures::{Future, Stream};
use parking_lot::RwLock;
use tracing::{debug, warn};

use super::{
    cluster_state::{ClusterState, ClusterStateFull, ClusterStateInit},
    config::Config,
    connect::{NonRepeatableClientApi, ProposeResponse, RepeatableClientApi},
    fetch::Fetch,
    ClientApi,
};
use crate::{
    members::ServerId,
    rpc::{
        Change, CurpError, MembershipResponse, Node, NodeMetadata, ProposeId, Redirect,
        WaitLearnerResponse,
    },
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
    /// The current cluster state
    cluster_state: ClusterStateFull,
}

impl Context {
    /// Creates a new `Context`
    pub(crate) fn new(propose_id: ProposeId, cluster_state: ClusterStateFull) -> Self {
        Self {
            propose_id,
            cluster_state,
        }
    }

    /// Returns the current propose id
    pub(crate) fn propose_id(&self) -> ProposeId {
        self.propose_id
    }

    /// Returns the current client id
    pub(crate) fn cluster_state(&self) -> ClusterStateFull {
        self.cluster_state.clone()
    }
}

/// A shared cluster state
#[derive(Debug)]
pub(crate) struct ClusterStateShared {
    /// Inner state
    inner: RwLock<ClusterState>,
    /// Fetch cluster object
    fetch: Fetch,
}

impl ClusterStateShared {
    /// Creates a new `ClusterStateShared`
    fn new(inner: ClusterState, fetch: Fetch) -> Self {
        Self {
            inner: RwLock::new(inner),
            fetch,
        }
    }

    /// Creates a new `ClusterStateShared`
    #[cfg(test)]
    pub(crate) fn new_test(inner: ClusterState, fetch: Fetch) -> Self {
        Self {
            inner: RwLock::new(inner),
            fetch,
        }
    }

    /// Retrieves the cluster state if it's ready, or fetches and updates it if not.
    pub(crate) async fn ready_or_fetch(&self) -> Result<ClusterStateFull, CurpError> {
        let current = self.inner.read().clone();
        match current {
            ClusterState::Init(_) | ClusterState::Errored(_) => self.fetch_and_update().await,
            ClusterState::Full(ready) => Ok(ready),
        }
    }

    /// Marks the current state as errored by updating the inner state to `ClusterState::Errored`.
    pub(crate) fn errored(&self) {
        let mut inner_w = self.inner.write();
        *inner_w = ClusterState::Errored(Box::new(inner_w.clone()));
    }

    /// Updates the current state with the provided `ClusterStateReady`.
    pub(crate) fn update_with(&self, cluster_state: ClusterStateFull) {
        *self.inner.write() = ClusterState::Full(cluster_state);
    }

    /// Retrieves the cluster state
    #[cfg(test)]
    pub(crate) fn unwrap_full_state(&self) -> ClusterStateFull {
        let current = self.inner.read().clone();
        match current {
            ClusterState::Init(_) | ClusterState::Errored(_) => unreachable!("initial state"),
            ClusterState::Full(ready) => ready,
        }
    }

    /// Fetch and updates current state
    ///
    /// Returns the fetched cluster state
    async fn fetch_and_update(&self) -> Result<ClusterStateFull, CurpError> {
        let current = self.inner.read().clone();
        let (new_state, _) = self.fetch.fetch_cluster(current).await?;
        *self.inner.write() = ClusterState::Full(new_state.clone());
        debug!("cluster state updates to: {new_state:?}");

        Ok(new_state)
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
    /// Fetch cluster object
    fetch: Fetch,
    /// The client id
    client_id: u64,
}

impl<Api> Retry<Api> {
    /// Gets the context required for unary requests
    async fn get_context(&self) -> Result<Context, CurpError> {
        let propose_id = ProposeId(self.client_id, rand::random());
        let cluster_state = self.cluster_state.ready_or_fetch().await?;
        // TODO: gen propose id
        Ok(Context::new(propose_id, cluster_state))
    }

    /// Execute a future and update cluster state if an error is returned.
    async fn with_error_handling<T, Fut>(&self, fut: Fut) -> Result<T, CurpError>
    where
        Fut: Future<Output = Result<T, CurpError>>,
    {
        let result = fut.await;
        if let Err(ref err) = result {
            match *err {
                // Some error that needs to update cluster state
                CurpError::RpcTransport(())
                | CurpError::WrongClusterVersion(())
                | CurpError::Redirect(_) // FIXME: The redirect error needs to include full cluster state
                | CurpError::Zombie(()) => {
                    self.cluster_state.errored();
                }
                CurpError::KeyConflict(())
                | CurpError::Duplicated(())
                | CurpError::ExpiredClientId(())
                | CurpError::InvalidConfig(())
                | CurpError::NodeNotExists(())
                | CurpError::NodeAlreadyExists(())
                | CurpError::LearnerNotCatchUp(())
                | CurpError::ShuttingDown(())
                | CurpError::Internal(_)
                | CurpError::LeaderTransfer(_)
                | CurpError::InvalidMemberChange(()) => {}
            }
        }
        result
    }
}

impl<Api> Retry<Api>
where
    Api: RepeatableClientApi<Error = CurpError> + Send + Sync + 'static,
{
    /// Create a retry client
    pub(super) fn new(
        inner: Api,
        retry_config: RetryConfig,
        fetch: Fetch,
        cluster_state: ClusterState,
    ) -> Self {
        let client_id: u64 = rand::random();
        let cluster_state = Arc::new(ClusterStateShared::new(cluster_state, fetch.clone()));
        Self {
            inner,
            retry_config,
            cluster_state,
            fetch,
            client_id,
        }
    }

    #[cfg(madsim)]
    /// Create a retry client, also returns client id for tests
    pub(super) fn new_with_client_id(
        inner: Api,
        retry_config: RetryConfig,
        keep_alive: KeepAlive,
        fetch: Fetch,
        cluster_state: ClusterState,
    ) -> (Self, Arc<AtomicU64>) {
        let cluster_state = Arc::new(ClusterStateShared::new(cluster_state, fetch.clone()));
        let keep_alive_handle = keep_alive.spawn_keep_alive(Arc::clone(&cluster_state));
        let client_id = keep_alive_handle.clone_client_id();
        let retry = Self {
            inner,
            retry_config,
            cluster_state,
            keep_alive: keep_alive_handle,
            fetch,
            tracker: CmdTracker::default(),
        };
        (retry, client_id)
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
        while let Some(delay) = backoff.next_delay() {
            let context = match self.with_error_handling(self.get_context()).await {
                Ok(x) => x,
                Err(err) => {
                    // TODO: refactor on_error like with_error_handling
                    self.on_error(err, delay, &mut last_err).await?;
                    continue;
                }
            };
            match f(&self.inner, context).await {
                Ok(res) => return Ok(res),
                Err(err) => self.on_error(err, delay, &mut last_err).await?,
            };
        }

        Err(tonic::Status::deadline_exceeded(format!(
            "request timeout, last error: {:?}",
            last_err.unwrap_or_else(|| unreachable!("last error must be set"))
        )))
    }

    /// Actions performs on error
    async fn on_error(
        &self,
        err: CurpError,
        delay: Duration,
        last_err: &mut Option<CurpError>,
    ) -> Result<(), tonic::Status> {
        Self::early_return(&err)?;

        #[cfg(feature = "client-metrics")]
        super::metrics::get().client_retry_count.add(1, &[]);

        warn!(
            "got error: {err:?}, retry on {} seconds later",
            delay.as_secs_f32()
        );
        *last_err = Some(err);
        tokio::time::sleep(delay).await;

        Ok(())
    }

    /// Handles errors before another retry
    fn early_return(err: &CurpError) -> Result<(), tonic::Status> {
        match *err {
            // some errors that should not retry
            CurpError::Duplicated(())
            | CurpError::ShuttingDown(())
            | CurpError::InvalidConfig(())
            | CurpError::NodeNotExists(())
            | CurpError::NodeAlreadyExists(())
            | CurpError::LearnerNotCatchUp(())
            | CurpError::InvalidMemberChange(()) => {
                return Err(tonic::Status::from(err.clone()));
            }

            // some errors that could have a retry
            CurpError::ExpiredClientId(())
            | CurpError::KeyConflict(())
            | CurpError::Internal(_)
            | CurpError::LeaderTransfer(_)
            | CurpError::RpcTransport(())
            | CurpError::WrongClusterVersion(())
            | CurpError::Redirect(_)
            | CurpError::Zombie(()) => {}
        }

        Ok(())
    }

    /// Returns the shared cluster state
    #[cfg(test)]
    pub(crate) fn cluster_state(&self) -> &ClusterStateShared {
        &self.cluster_state
    }
}

impl<Api> Retry<Api>
where
    Api: NonRepeatableClientApi<Error = CurpError> + Send + Sync + 'static,
{
    /// Takes a function f and run once.
    async fn once<'a, R, F>(&'a self, f: impl Fn(&'a Api, Context) -> F) -> Result<R, tonic::Status>
    where
        F: Future<Output = Result<R, CurpError>>,
    {
        let ctx = self.with_error_handling(self.get_context()).await?;
        self.with_error_handling(f(&self.inner, ctx))
            .await
            .map_err(Into::into)
    }
}

impl<Api> Retry<Api>
where
    Api: RepeatableClientApi<Error = CurpError> + Send + Sync + 'static,
{
    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), tonic::Status> {
        self.retry::<_, _>(|client, ctx| async move {
            RepeatableClientApi::propose_shutdown(client, ctx).await
        })
        .await
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: u64) -> Result<(), tonic::Status> {
        self.retry::<_, _>(|client, ctx| client.move_leader(node_id, ctx))
            .await
    }

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    ///
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<MembershipResponse, tonic::Status> {
        self.retry::<_, _>(|client, ctx| async move {
            let (_, resp) = self
                .fetch
                .fetch_cluster(ClusterState::Full(ctx.cluster_state()))
                .await?;
            Ok(resp)
        })
        .await
    }

    /// Performs membership change
    async fn change_membership(&self, changes: Vec<Change>) -> Result<(), tonic::Status> {
        let resp = self
            .retry::<_, _>(|client, ctx| client.change_membership(changes.clone(), ctx))
            .await?;
        if let Some(resp) = resp {
            let cluster_state =
                Fetch::build_cluster_state_from_response(self.fetch.connect_to(), resp);
            self.cluster_state.update_with(cluster_state);
        }

        Ok(())
    }

    /// Send wait learner of the give ids, returns a stream of updating response stream
    async fn wait_learner(
        &self,
        node_ids: BTreeSet<u64>,
    ) -> Result<
        Box<dyn Stream<Item = Result<WaitLearnerResponse, tonic::Status>> + Send>,
        tonic::Status,
    > {
        self.retry::<_, _>(|client, ctx| client.wait_learner(node_ids.clone(), ctx))
            .await
    }
}

impl<Api, C> Retry<Api>
where
    C: Command,
    Api: NonRepeatableClientApi<Error = CurpError, Cmd = C> + Send + Sync + 'static,
{
    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &C,
        token: Option<&String>,
        use_fast_path: bool,
    ) -> Result<ProposeResponse<C>, tonic::Status> {
        self.once::<_, _>(|client, ctx| async move {
            NonRepeatableClientApi::propose(client, cmd, token, use_fast_path, ctx).await
        })
        .await
    }
}

#[async_trait]
impl<Api, C> ClientApi for Retry<Api>
where
    C: Command,
    Api: NonRepeatableClientApi<Error = CurpError, Cmd = C>
        + RepeatableClientApi<Error = CurpError>
        + Send
        + Sync
        + 'static,
{
    /// The client error
    type Error = tonic::Status;

    /// The command type
    type Cmd = C;

    /// Send propose to the whole cluster, `use_fast_path` set to `false` to fallback into ordered
    /// requests (event the requests are commutative).
    async fn propose(
        &self,
        cmd: &Self::Cmd,
        token: Option<&String>, // TODO: Allow external custom interceptors, do not pass token in parameters
        use_fast_path: bool,
    ) -> Result<ProposeResponse<Self::Cmd>, Self::Error> {
        self.propose(cmd, token, use_fast_path).await
    }

    /// Send propose to shutdown cluster
    async fn propose_shutdown(&self) -> Result<(), Self::Error> {
        self.propose_shutdown().await
    }

    /// Send move leader request
    async fn move_leader(&self, node_id: ServerId) -> Result<(), Self::Error> {
        self.move_leader(node_id).await
    }

    /// Send fetch cluster requests to all servers (That's because initially, we didn't
    /// know who the leader is.)
    ///
    /// Note: The fetched cluster may still be outdated if `linearizable` is false
    async fn fetch_cluster(&self, linearizable: bool) -> Result<MembershipResponse, Self::Error> {
        self.fetch_cluster(linearizable).await
    }

    /// Performs membership change
    async fn change_membership(&self, changes: Vec<Change>) -> Result<(), Self::Error> {
        self.change_membership(changes).await
    }

    /// Send wait learner of the give ids, returns a stream of updating response stream
    async fn wait_learner(
        &self,
        node_ids: BTreeSet<u64>,
    ) -> Result<Box<dyn Stream<Item = Result<WaitLearnerResponse, Self::Error>> + Send>, Self::Error>
    {
        self.wait_learner(node_ids).await
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
