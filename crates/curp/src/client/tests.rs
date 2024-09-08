use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use curp_test_utils::test_cmd::{LogIndexResult, TestCommand, TestCommandResult};
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing_test::traced_test;
#[cfg(madsim)]
use utils::ClientTlsConfig;

use super::{cluster_state::ClusterState, config::Config, unary::Unary};
use crate::{
    client::{
        cluster_state::ClusterStateReady,
        fetch::Fetch,
        keep_alive::KeepAlive,
        retry::{Context, Retry, RetryConfig},
        ClientApi, RepeatableClientApi,
    },
    members::ServerId,
    rpc::{
        connect::{ConnectApi, MockConnectApi},
        CurpError, FetchClusterResponse, Member, OpResponse, ProposeId, ProposeResponse,
        ReadIndexResponse, RecordResponse, ResponseOp, SyncedResponse,
    },
};

/// Create a mocked connects with server id from 0~size
#[allow(trivial_casts)] // Trait object with high ranked type inferences failed, cast manually
pub(super) fn init_mocked_connects(
    size: usize,
    f: impl Fn(usize, &mut MockConnectApi),
) -> HashMap<ServerId, Arc<dyn ConnectApi>> {
    std::iter::repeat_with(|| MockConnectApi::new())
        .take(size)
        .enumerate()
        .map(|(id, mut conn)| {
            conn.expect_id().returning(move || id as ServerId);
            conn.expect_update_addrs().returning(|_addr| Ok(()));
            conn.expect_lease_keep_alive().returning(|_, _| Ok(1));
            f(id, &mut conn);
            (id as ServerId, Arc::new(conn) as Arc<dyn ConnectApi>)
        })
        .collect()
}

/// Create unary client for test
fn init_unary_client(
    local_server: Option<ServerId>,
    tls_config: Option<ClientTlsConfig>,
) -> Unary<TestCommand> {
    Unary::new(Config::new(
        local_server,
        tls_config,
        Duration::from_secs(0),
        Duration::from_secs(0),
        false,
    ))
}

// Tests for unary client

fn build_propose_response(conflict: bool) -> OpResponse {
    let resp = ResponseOp::Propose(ProposeResponse::new_result::<TestCommand>(
        &Ok(TestCommandResult::default()),
        conflict,
    ));
    OpResponse { op: Some(resp) }
}

fn build_synced_response() -> OpResponse {
    let resp = ResponseOp::Synced(SyncedResponse::new_result::<TestCommand>(&Ok(1.into())));
    OpResponse { op: Some(resp) }
}

// TODO: rewrite this tests
#[cfg(ignore)]
fn build_empty_response() -> OpResponse {
    OpResponse { op: None }
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_fast_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose_stream()
            .return_once(move |_req, _token, _timeout| {
                assert_eq!(id, 0, "followers should not receive propose");
                let resp = async_stream::stream! {
                    yield Ok(build_propose_response(false));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    yield Ok(build_synced_response());
                };
                Ok(tonic::Response::new(Box::new(resp)))
            });
        conn.expect_record().return_once(move |_req, _timeout| {
            let resp = match id {
                0 => unreachable!("leader should not receive record request"),
                1 | 2 | 3 => RecordResponse { conflict: false },
                4 => RecordResponse { conflict: true },
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });
    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
    let ctx = Context::new(ProposeId::default(), 0, cluster_state);
    let res = unary
        .propose(&TestCommand::new_put(vec![1], 1), None, true, ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, (TestCommandResult::default(), None));
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_slow_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose_stream()
            .return_once(move |_req, _token, _timeout| {
                assert_eq!(id, 0, "followers should not receive propose");
                let resp = async_stream::stream! {
                    yield Ok(build_propose_response(false));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    yield Ok(build_synced_response());
                };
                Ok(tonic::Response::new(Box::new(resp)))
            });
        conn.expect_record().return_once(move |_req, _timeout| {
            let resp = match id {
                0 => unreachable!("leader should not receive record request"),
                1 | 2 | 3 => RecordResponse { conflict: false },
                4 => RecordResponse { conflict: true },
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });

    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
    let ctx = Context::new(ProposeId::default(), 0, cluster_state);
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::new_put(vec![1], 1), None, false, ctx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        start_at.elapsed() > Duration::from_millis(100),
        "slow round takes at least 100ms"
    );
    assert_eq!(
        res,
        (TestCommandResult::default(), Some(LogIndexResult::from(1)))
    );
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_fast_path_fallback_slow_path() {
    // record how many times `handle_propose` was invoked.
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose_stream()
            .return_once(move |_req, _token, _timeout| {
                assert_eq!(id, 0, "followers should not receive propose");
                let resp = async_stream::stream! {
                    yield Ok(build_propose_response(false));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    yield Ok(build_synced_response());
                };
                Ok(tonic::Response::new(Box::new(resp)))
            });
        // insufficient quorum
        conn.expect_record().return_once(move |_req, _timeout| {
            let resp = match id {
                0 => unreachable!("leader should not receive record request"),
                1 | 2 => RecordResponse { conflict: false },
                3 | 4 => RecordResponse { conflict: true },
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });

    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
    let ctx = Context::new(ProposeId::default(), 0, cluster_state);
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::new_put(vec![1], 1), None, true, ctx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        start_at.elapsed() > Duration::from_millis(100),
        "slow round takes at least 100ms"
    );
    // indicate that we actually run out of fast round
    assert_eq!(
        res,
        (TestCommandResult::default(), Some(LogIndexResult::from(1)))
    );
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_return_early_err() {
    for early_err in [
        CurpError::shutting_down(),
        CurpError::invalid_config(),
        CurpError::node_already_exists(),
        CurpError::node_not_exist(),
        CurpError::learner_not_catch_up(),
        CurpError::expired_client_id(),
        CurpError::redirect(Some(1), 0),
    ] {
        assert!(early_err.should_abort_fast_round());
        // record how many times rpc was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(5, |_id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose_stream()
                .return_once(move |_req, _token, _timeout| {
                    *counter_c.lock().unwrap() += 1;
                    Err(err)
                });

            let err = early_err.clone();
            conn.expect_record()
                .return_once(move |_req, _timeout| Err(err));
        });

        let unary = init_unary_client(None, None);
        let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
        let ctx = Context::new(ProposeId::default(), 0, cluster_state);
        let err = unary
            .propose(&TestCommand::new_put(vec![1], 1), None, true, ctx)
            .await
            .unwrap_err();
        assert_eq!(err, early_err);
        assert_eq!(*counter.lock().unwrap(), 1);
    }
}

// Tests for retry layer

#[traced_test]
#[tokio::test]
async fn test_retry_propose_return_no_retry_error() {
    for early_err in [
        CurpError::shutting_down(),
        CurpError::invalid_config(),
        CurpError::node_already_exists(),
        CurpError::node_not_exist(),
        CurpError::learner_not_catch_up(),
    ] {
        // record how many times rpc was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(5, |_id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose_stream()
                .return_once(move |_req, _token, _timeout| {
                    *counter_c.lock().unwrap() += 1;
                    Err(err)
                });

            let err = early_err.clone();
            conn.expect_record()
                .return_once(move |_req, _timeout| Err(err));
        });

        let unary = init_unary_client(None, None);
        let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
        let retry = Retry::new(
            unary,
            RetryConfig::new_fixed(Duration::from_millis(100), 5),
            KeepAlive::new(Duration::from_secs(1)),
            Fetch::new_disable(),
            ClusterState::Ready(cluster_state),
        );
        let err = retry
            .propose(&TestCommand::new_put(vec![1], 1), None, false)
            .await
            .unwrap_err();
        assert_eq!(err.message(), tonic::Status::from(early_err).message());
        assert_eq!(*counter.lock().unwrap(), 1);
    }
}

#[traced_test]
#[tokio::test]
async fn test_retry_propose_return_retry_error() {
    for early_err in [
        CurpError::RpcTransport(()),
        CurpError::internal("No reason"),
    ] {
        let connects = init_mocked_connects(5, |id, conn| {
            conn.expect_fetch_cluster()
                .returning(move |_req, _timeout| {
                    Ok(tonic::Response::new(FetchClusterResponse {
                        leader_id: Some(0.into()),
                        term: 2,
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["A3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["A4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    }))
                });
            if id == 0 {
                let err = early_err.clone();
                conn.expect_propose_stream()
                    .times(5) // propose should be retried in 5 times on leader
                    .returning(move |_req, _token, _timeout| Err(err.clone()));
            }

            let err = early_err.clone();
            conn.expect_record()
                .returning(move |_req, _timeout| Err(err.clone()));
        });
        let unary = init_unary_client(None, None);
        let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
        let retry = Retry::new(
            unary,
            RetryConfig::new_fixed(Duration::from_millis(10), 5),
            KeepAlive::new(Duration::from_secs(1)),
            Fetch::new_disable(),
            ClusterState::Ready(cluster_state),
        );
        let _err = retry
            .propose(&TestCommand::new_put(vec![1], 1), None, false)
            .await
            .unwrap_err();
    }
}

#[traced_test]
#[tokio::test]
async fn test_read_index_success() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose_stream()
            .return_once(move |_req, _token, _timeout| {
                assert_eq!(id, 0, "followers should not receive propose");
                let resp = async_stream::stream! {
                    yield Ok(build_propose_response(false));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    yield Ok(build_synced_response());
                };
                Ok(tonic::Response::new(Box::new(resp)))
            });
        conn.expect_read_index().return_once(move |_timeout| {
            let resp = match id {
                0 => unreachable!("read index should not send to leader"),
                1 | 2 => ReadIndexResponse { term: 1 },
                3 | 4 => ReadIndexResponse { term: 2 },
                _ => unreachable!("there are only 5 nodes"),
            };

            Ok(tonic::Response::new(resp))
        });
    });

    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
    let ctx = Context::new(ProposeId::default(), 0, cluster_state);
    let res = unary
        .propose(&TestCommand::default(), None, true, ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, (TestCommandResult::default(), None));
}

#[traced_test]
#[tokio::test]
async fn test_read_index_fail() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose_stream()
            .return_once(move |_req, _token, _timeout| {
                assert_eq!(id, 0, "followers should not receive propose");
                let resp = async_stream::stream! {
                    yield Ok(build_propose_response(false));
                    yield Ok(build_synced_response());
                };
                Ok(tonic::Response::new(Box::new(resp)))
            });
        conn.expect_read_index().return_once(move |_timeout| {
            let resp = match id {
                0 => unreachable!("read index should not send to leader"),
                1 => ReadIndexResponse { term: 1 },
                2 | 3 | 4 => ReadIndexResponse { term: 2 },
                _ => unreachable!("there are only 5 nodes"),
            };

            Ok(tonic::Response::new(resp))
        });
    });
    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateReady::new(0, 1, 0, connects);
    let ctx = Context::new(ProposeId::default(), 0, cluster_state);
    let res = unary
        .propose(&TestCommand::default(), None, true, ctx)
        .await;
    assert!(res.is_err());
}

// TODO: rewrite these tests
#[cfg(ignore)]
mod test_stream {
    use super::*;

    // Tests for stream client

    struct MockedStreamConnectApi {
        id: ServerId,
        lease_keep_alive_handle:
            Box<dyn Fn(u64) -> BoxFuture<'static, Result<u64, CurpError>> + Send + Sync + 'static>,
    }

    #[async_trait::async_trait]
    impl ConnectApi for MockedStreamConnectApi {
        /// Get server id
        fn id(&self) -> ServerId {
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

        /// Send `ProposeConfChange`
        async fn propose_conf_change(
            &self,
            _request: ProposeConfChangeRequest,
            _timeout: Duration,
        ) -> Result<tonic::Response<ProposeConfChangeResponse>, CurpError> {
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
            unreachable!("please use MockedConnectApi")
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
    ) -> HashMap<ServerId, Arc<dyn ConnectApi>> {
        let mut keep_alive_handle = Some(keep_alive_handle);
        let redirect_handle = move |_id| {
            Box::pin(async move {
                Err(CurpError::redirect(
                    Some(leader_idx as ServerId),
                    leader_term,
                ))
            }) as BoxFuture<'static, Result<u64, CurpError>>
        };
        (0..size)
            .map(|id| MockedStreamConnectApi {
                id: id as ServerId,
                lease_keep_alive_handle: if id == leader_idx {
                    Box::new(keep_alive_handle.take().unwrap())
                } else {
                    Box::new(redirect_handle)
                },
            })
            .enumerate()
            .map(|(id, api)| (id as ServerId, Arc::new(api) as Arc<dyn ConnectApi>))
            .collect()
    }

    /// Create stream client for test
    fn init_stream_client(
        connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
        local_server: Option<ServerId>,
        leader: Option<ServerId>,
        term: u64,
        cluster_version: u64,
    ) -> Streaming {
        let state = State::new_arc(connects, local_server, leader, term, cluster_version, None);
        Streaming::new(state, StreamingConfig::new(Duration::from_secs(1)))
    }

    #[traced_test]
    #[tokio::test]
    async fn test_stream_client_keep_alive_works() {
        let connects = init_mocked_stream_connects(5, 0, 1, move |client_id| {
            Box::pin(async move {
                client_id
                    .compare_exchange(
                        1,
                        10,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(30)).await;
                unreachable!("test timeout")
            })
        });
        let stream = init_stream_client(connects, None, Some(0), 1, 1);
        tokio::time::timeout(Duration::from_millis(100), stream.keep_heartbeat())
            .await
            .unwrap_err();
        assert_eq!(stream.state.client_id(), 10);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_stream_client_keep_alive_on_redirect() {
        let connects = init_mocked_stream_connects(5, 0, 2, move |client_id| {
            Box::pin(async move {
                client_id
                    .compare_exchange(
                        1,
                        10,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(30)).await;
                unreachable!("test timeout")
            })
        });
        let stream = init_stream_client(connects, None, Some(1), 1, 1);
        tokio::time::timeout(Duration::from_millis(100), stream.keep_heartbeat())
            .await
            .unwrap_err();
        assert_eq!(stream.state.client_id(), 10);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_stream_client_keep_alive_hang_up_on_bypassed() {
        let connects = init_mocked_stream_connects(5, 0, 1, |_client_id| {
            Box::pin(
                async move { panic!("should not invoke lease_keep_alive in bypassed connection") },
            )
        });
        let stream = init_stream_client(connects, Some(0), Some(0), 1, 1);
        tokio::time::timeout(Duration::from_millis(100), stream.keep_heartbeat())
            .await
            .unwrap_err();
        assert_ne!(stream.state.client_id(), 0);
    }

    #[traced_test]
    #[tokio::test]
    #[allow(clippy::ignored_unit_patterns)] // tokio select internal triggered
    async fn test_stream_client_keep_alive_resume_on_leadership_changed() {
        let connects = init_mocked_stream_connects(5, 1, 2, move |client_id| {
            Box::pin(async move {
                // generated a client id for bypassed client
                assert_ne!(client_id.load(std::sync::atomic::Ordering::Relaxed), 0);
                client_id.store(10, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(30)).await;
                unreachable!("test timeout")
            })
        });
        let stream = init_stream_client(connects, Some(0), Some(0), 1, 1);
        let update_leader = async {
            // wait for stream to hang up
            tokio::time::sleep(Duration::from_millis(100)).await;
            // check the local id
            assert_ne!(stream.state.client_id(), 0);
            stream.state.check_and_update_leader(Some(1), 2).await;
            // wait for stream to resume
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        tokio::select! {
            _ = stream.keep_heartbeat() => {},
            _ = update_leader => {}
        }
        assert_eq!(stream.state.client_id(), 10);
    }
}
