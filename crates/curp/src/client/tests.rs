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
        connect::RepeatableClientApi,
        fetch::Fetch,
        keep_alive::KeepAlive,
        retry::{Context, Retry, RetryConfig},
        ClientApi,
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
