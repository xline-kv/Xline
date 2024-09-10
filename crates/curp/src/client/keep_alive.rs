use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use event_listener::Event;
use futures::{
    future::{self, OptionFuture},
    Future, FutureExt,
};
use parking_lot::RwLock;
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{debug, info, warn};

use super::{
    cluster_state::{ClusterState, ClusterStateReady},
    fetch::Fetch,
    retry::ClusterStateShared,
};
use crate::rpc::{connect::ConnectApi, CurpError, Redirect};

/// Keep alive
#[derive(Clone, Debug)]
pub(crate) struct KeepAlive {
    /// Heartbeat interval
    heartbeat_interval: Duration,
}

/// Handle of the keep alive task
#[derive(Debug)]
pub(crate) struct KeepAliveHandle {
    /// Client id
    client_id: Arc<AtomicU64>,
    /// Update event of client id
    update_event: Arc<Event>,
    /// Task join handle
    handle: JoinHandle<()>,
}

impl KeepAliveHandle {
    /// Wait for the client id
    pub(crate) async fn wait_id_update(&self, current_id: u64) -> u64 {
        loop {
            let listen_update = self.update_event.listen();
            let id = self.client_id.load(Ordering::Relaxed);
            if current_id != id {
                return id;
            }
            listen_update.await;
        }
    }

    #[cfg(madsim)]
    /// Clone the client id
    pub(crate) fn clone_client_id(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.client_id)
    }
}

impl KeepAlive {
    /// Creates a new `KeepAlive`
    pub(crate) fn new(heartbeat_interval: Duration) -> Self {
        Self { heartbeat_interval }
    }

    /// Streaming keep alive
    pub(crate) fn spawn_keep_alive(
        self,
        cluster_state: Arc<ClusterStateShared>,
    ) -> KeepAliveHandle {
        /// Sleep duration when keep alive failed
        const FAIL_SLEEP_DURATION: Duration = Duration::from_secs(1);
        let client_id = Arc::new(AtomicU64::new(0));
        let client_id_c = Arc::clone(&client_id);
        let update_event = Arc::new(Event::new());
        let update_event_c = Arc::clone(&update_event);
        let handle = tokio::spawn(async move {
            loop {
                let fetch_result = cluster_state.ready_or_fetch().await;
                // TODO: make the error handling code reusable
                let current_state = match fetch_result {
                    Ok(ready) => ready,
                    Err(CurpError::ShuttingDown(())) => {
                        info!("cluster is shutting down, exiting keep alive task");
                        return;
                    }
                    Err(e) => {
                        warn!("fetch cluster failed: {e:?}");
                        // Sleep for some time, the cluster state should be updated in a while
                        tokio::time::sleep(FAIL_SLEEP_DURATION).await;
                        continue;
                    }
                };
                let current_id = client_id.load(Ordering::Relaxed);
                let result = self.keep_alive_with(current_id, current_state).await;
                match result {
                    Ok(new_id) => {
                        client_id.store(new_id, Ordering::Relaxed);
                        let _ignore = update_event.notify(usize::MAX);
                    }
                    Err(CurpError::ShuttingDown(())) => {
                        info!("cluster is shutting down, exiting keep alive task");
                        return;
                    }
                    Err(e) => {
                        warn!("keep alive failed: {e:?}");
                        if let Err(err) = cluster_state.fetch_and_update().await {
                            warn!("fetch cluster failed: {err:?}");
                            tokio::time::sleep(FAIL_SLEEP_DURATION).await;
                        }
                    }
                }

                /// This helps prevent blocking the runtime if this task cannot be
                /// cancelled on runtime exit.
                tokio::task::yield_now().await;
            }
        });

        KeepAliveHandle {
            client_id: client_id_c,
            update_event: update_event_c,
            handle,
        }
    }

    /// Keep alive with the given state and config
    pub(crate) async fn keep_alive_with(
        &self,
        client_id: u64,
        cluster_state: ClusterStateReady,
    ) -> Result<u64, CurpError> {
        cluster_state
            .map_leader(|conn| async move {
                conn.lease_keep_alive(client_id, self.heartbeat_interval)
                    .await
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use futures::{future::BoxFuture, Stream};
    use tonic::Status;
    use tracing_test::traced_test;

    use crate::rpc::{
        connect::{ConnectApi, MockConnectApi},
        AddLearnerRequest, AddLearnerResponse, AddMemberRequest, AddMemberResponse, CurpError,
        FetchClusterRequest, FetchClusterResponse, FetchMembershipRequest, FetchMembershipResponse,
        FetchReadStateRequest, FetchReadStateResponse, Member, MoveLeaderRequest,
        MoveLeaderResponse, OpResponse, ProposeId, ProposeRequest, ProposeResponse, PublishRequest,
        PublishResponse, ReadIndexResponse, RecordRequest, RecordResponse, RemoveLearnerRequest,
        RemoveLearnerResponse, RemoveMemberRequest, RemoveMemberResponse, ResponseOp,
        ShutdownRequest, ShutdownResponse, SyncedResponse,
    };

    struct MockedStreamConnectApi {
        id: u64,
        lease_keep_alive_handle:
            Box<dyn Fn(u64) -> BoxFuture<'static, Result<u64, CurpError>> + Send + Sync + 'static>,
    }

    #[async_trait::async_trait]
    impl ConnectApi for MockedStreamConnectApi {
        /// Get server id
        fn id(&self) -> u64 {
            self.id
        }

        /// Update server addresses, the new addresses will override the old ones
        async fn update_addrs(&self, _addrs: Vec<String>) -> Result<(), tonic::transport::Error> {
            Ok(())
        }

        /// Send `ProposeRequest`
        async fn propose_stream(
            &self,
            _request: ProposeRequest,
            _token: Option<String>,
            _timeout: Duration,
        ) -> Result<
            tonic::Response<Box<dyn Stream<Item = Result<OpResponse, Status>> + Send>>,
            CurpError,
        > {
            unreachable!("please use MockedConnectApi")
        }

        /// Send `RecordRequest`
        async fn record(
            &self,
            _request: RecordRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<RecordResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Send `ReadIndexRequest`
        async fn read_index(
            &self,
            _timeout: Duration,
        ) -> Result<tonic::Response<ReadIndexResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Send `PublishRequest`
        async fn publish(
            &self,
            _request: PublishRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<PublishResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Send `ShutdownRequest`
        async fn shutdown(
            &self,
            _request: ShutdownRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<ShutdownResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Send `FetchClusterRequest`
        async fn fetch_cluster(
            &self,
            _request: FetchClusterRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<FetchClusterResponse>, CurpError> {
            let members = (0..5)
                .into_iter()
                .map(|id| Member::new(id, format!("{id}"), vec![], vec![], false))
                .collect();
            let resp = FetchClusterResponse::new(Some(0), 1, 1, members, 1);
            Ok(tonic::Response::new(resp))
        }

        /// Send `FetchReadStateRequest`
        async fn fetch_read_state(
            &self,
            _request: FetchReadStateRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<FetchReadStateResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Send `MoveLeaderRequest`
        async fn move_leader(
            &self,
            _request: MoveLeaderRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<MoveLeaderResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Keep send lease keep alive to server and mutate the client id
        async fn lease_keep_alive(
            &self,
            client_id: u64,
            _interval: Duration,
        ) -> Result<u64, CurpError> {
            (self.lease_keep_alive_handle)(client_id).await
        }

        async fn fetch_membership(
            &self,
            _request: FetchMembershipRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<FetchMembershipResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        async fn add_learner(
            &self,
            _request: AddLearnerRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<AddLearnerResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        async fn remove_learner(
            &self,
            _request: RemoveLearnerRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<RemoveLearnerResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Add a learner to the cluster.
        async fn add_member(
            &self,
            request: AddMemberRequest,
            timeout: Duration,
        ) -> Result<tonic::Response<AddMemberResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }

        /// Remove a learner from the cluster.
        async fn remove_member(
            &self,
            request: RemoveMemberRequest,
            timeout: Duration,
        ) -> Result<tonic::Response<RemoveMemberResponse>, CurpError> {
            unreachable!("please use MockedConnectApi")
        }
    }

    /// Create mocked stream connects
    ///
    /// The leader is S0
    #[allow(trivial_casts)] // cannot be inferred
    fn init_mocked_stream_connects(
        size: usize,
        leader_idx: usize,
        leader_term: u64,
        keep_alive_handle: impl Fn(u64) -> BoxFuture<'static, Result<u64, CurpError>>
            + Send
            + Sync
            + 'static,
    ) -> HashMap<u64, Arc<dyn ConnectApi>> {
        let mut keep_alive_handle = Some(keep_alive_handle);
        let redirect_handle = move |_id| {
            Box::pin(async move { Err(CurpError::redirect(Some(leader_idx as u64), leader_term)) })
                as BoxFuture<'static, Result<u64, CurpError>>
        };
        (0..size)
            .map(|id| MockedStreamConnectApi {
                id: id as u64,
                lease_keep_alive_handle: if id == leader_idx {
                    Box::new(keep_alive_handle.take().unwrap())
                } else {
                    Box::new(redirect_handle)
                },
            })
            .enumerate()
            .map(|(id, api)| (id as u64, Arc::new(api) as Arc<dyn ConnectApi>))
            .collect()
    }

    /// Create stream client for test
    fn init_stream_client(
        connects: HashMap<u64, Arc<dyn ConnectApi>>,
        leader: u64,
        term: u64,
        cluster_version: u64,
    ) -> KeepAliveHandle {
        let state = ClusterState::Ready(ClusterStateReady::new(
            leader,
            term,
            cluster_version,
            connects.clone(),
        ));
        let fetch = Fetch::new(Duration::from_secs(0), move |_| connects.clone());
        let state_shared = ClusterStateShared::new_test(state, fetch);

        let keep_alive = KeepAlive::new(Duration::from_secs(1));
        keep_alive.spawn_keep_alive(Arc::new(state_shared))
    }

    #[traced_test]
    #[tokio::test]
    async fn test_stream_client_keep_alive_works() {
        let connects =
            init_mocked_stream_connects(5, 0, 1, move |client_id| Box::pin(async move { Ok(10) }));
        let mut keep_alive = init_stream_client(connects, 0, 1, 1);
        tokio::time::timeout(Duration::from_millis(100), &mut keep_alive.handle)
            .await
            .unwrap_err();
        assert_eq!(keep_alive.wait_id_update(0).await, 10);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_stream_client_keep_alive_on_redirect() {
        let connects =
            init_mocked_stream_connects(5, 0, 2, move |client_id| Box::pin(async move { Ok(10) }));
        let mut keep_alive = init_stream_client(connects, 1, 1, 1);
        tokio::time::timeout(Duration::from_millis(100), &mut keep_alive.handle)
            .await
            .unwrap_err();
        assert_eq!(keep_alive.wait_id_update(0).await, 10);
    }
}
