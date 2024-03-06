use std::{
    collections::HashMap,
    ops::AddAssign,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, Mutex,
    },
    time::Duration,
};

use curp_external_api::LogIndex;
use curp_test_utils::test_cmd::{LogIndexResult, TestCommand, TestCommandResult};
use futures::future::BoxFuture;
use tokio::time::Instant;
#[cfg(not(madsim))]
use tonic::transport::ClientTlsConfig;
use tracing_test::traced_test;
#[cfg(madsim)]
use utils::ClientTlsConfig;

use super::{
    retry::{Retry, RetryConfig},
    state::State,
    stream::{Streaming, StreamingConfig},
    unary::{Unary, UnaryConfig},
};
use crate::{
    client::ClientApi,
    members::ServerId,
    rpc::{
        connect::{ConnectApi, MockConnectApi},
        *,
    },
};

/// Create a mocked connects with server id from 0~size
#[allow(trivial_casts)] // Trait object with high ranked type inferences failed, cast manually
fn init_mocked_connects(
    size: usize,
    f: impl Fn(usize, &mut MockConnectApi),
) -> HashMap<ServerId, Arc<dyn ConnectApi>> {
    std::iter::repeat_with(|| MockConnectApi::new())
        .take(size)
        .enumerate()
        .map(|(id, mut conn)| {
            conn.expect_id().returning(move || id as ServerId);
            conn.expect_update_addrs().returning(|_addr| Ok(()));
            f(id, &mut conn);
            (id as ServerId, Arc::new(conn) as Arc<dyn ConnectApi>)
        })
        .collect()
}

/// Create unary client for test
fn init_unary_client(
    connects: HashMap<ServerId, Arc<dyn ConnectApi>>,
    local_server: Option<ServerId>,
    leader: Option<ServerId>,
    term: u64,
    cluster_version: u64,
    tls_config: Option<ClientTlsConfig>,
) -> Unary<TestCommand> {
    let state = State::new_arc(
        connects,
        local_server,
        leader,
        term,
        cluster_version,
        tls_config,
    );
    Unary::new(
        state,
        UnaryConfig::new(Duration::from_secs(0), Duration::from_secs(0)),
    )
}

// Tests for unary client

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_serializable() {
    let connects = init_mocked_connects(3, |_id, conn| {
        conn.expect_fetch_cluster().return_once(|_req, _timeout| {
            Ok(tonic::Response::new(FetchClusterResponse {
                leader_id: Some(0),
                term: 1,
                cluster_id: 123,
                members: vec![
                    Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                    Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                    Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                ],
                cluster_version: 1,
            }))
        });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary.fetch_cluster(false).await.unwrap();
    assert_eq!(
        res.into_peer_urls(),
        HashMap::from([
            (0, vec!["A0".to_owned()]),
            (1, vec!["A1".to_owned()]),
            (2, vec!["A2".to_owned()])
        ])
    );
}

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_serializable_local_first() {
    let connects = init_mocked_connects(3, |id, conn| {
        conn.expect_fetch_cluster()
            .return_once(move |_req, _timeout| {
                let members = if id == 1 {
                    // local server(1) does not see the cluster members
                    vec![]
                } else {
                    panic!("other server's `fetch_cluster` should not be invoked");
                };
                Ok(tonic::Response::new(FetchClusterResponse {
                    leader_id: Some(0),
                    term: 1,
                    cluster_id: 123,
                    members,
                    cluster_version: 1,
                }))
            });
    });
    let unary = init_unary_client(connects, Some(1), None, 0, 0, None);
    let res = unary.fetch_cluster(false).await.unwrap();
    assert!(res.members.is_empty());
}

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_linearizable() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_fetch_cluster()
            .return_once(move |_req, _timeout| {
                let resp = match id {
                    0 => FetchClusterResponse {
                        leader_id: Some(0),
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
                    },
                    1 | 4 => FetchClusterResponse {
                        leader_id: Some(0),
                        term: 2,
                        cluster_id: 123,
                        members: vec![], // linearizable read from follower returns empty members
                        cluster_version: 1,
                    },
                    2 => FetchClusterResponse {
                        leader_id: None,
                        term: 23, // abnormal term
                        cluster_id: 123,
                        members: vec![],
                        cluster_version: 1,
                    },
                    3 => FetchClusterResponse {
                        leader_id: Some(3), // imagine this node is a old leader
                        term: 1,            // with the old term
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["B0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["B1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["B2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["B3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["B4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    },
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary.fetch_cluster(true).await.unwrap();
    assert_eq!(
        res.into_peer_urls(),
        HashMap::from([
            (0, vec!["A0".to_owned()]),
            (1, vec!["A1".to_owned()]),
            (2, vec!["A2".to_owned()]),
            (3, vec!["A3".to_owned()]),
            (4, vec!["A4".to_owned()])
        ])
    );
}

#[traced_test]
#[tokio::test]
async fn test_unary_fetch_clusters_linearizable_failed() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_fetch_cluster()
            .return_once(move |_req, _timeout| {
                let resp = match id {
                    0 => FetchClusterResponse {
                        leader_id: Some(0),
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
                    },
                    1 => FetchClusterResponse {
                        leader_id: Some(0),
                        term: 2,
                        cluster_id: 123,
                        members: vec![], // linearizable read from follower returns empty members
                        cluster_version: 1,
                    },
                    2 => FetchClusterResponse {
                        leader_id: None, // imagine this node is a disconnected candidate
                        term: 23,        // with a high term
                        cluster_id: 123,
                        members: vec![],
                        cluster_version: 1,
                    },
                    3 => FetchClusterResponse {
                        leader_id: Some(3), // imagine this node is a old leader
                        term: 1,            // with the old term
                        cluster_id: 123,
                        members: vec![
                            Member::new(0, "S0", vec!["B0".to_owned()], [], false),
                            Member::new(1, "S1", vec!["B1".to_owned()], [], false),
                            Member::new(2, "S2", vec!["B2".to_owned()], [], false),
                            Member::new(3, "S3", vec!["B3".to_owned()], [], false),
                            Member::new(4, "S4", vec!["B4".to_owned()], [], false),
                        ],
                        cluster_version: 1,
                    },
                    4 => FetchClusterResponse {
                        leader_id: Some(3), // imagine this node is a old follower of old leader(3)
                        term: 1,            // with the old term
                        cluster_id: 123,
                        members: vec![],
                        cluster_version: 1,
                    },
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary.fetch_cluster(true).await.unwrap_err();
    // only server(0, 1)'s responses are valid, less than majority quorum(3), got a mocked RpcTransport to retry
    assert_eq!(res, CurpError::RpcTransport(()));
}

#[traced_test]
#[tokio::test]
async fn test_unary_fast_round_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(&Ok(
                        TestCommandResult::default(),
                    )),
                    1 | 2 | 3 => ProposeResponse::new_empty(),
                    4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary
        .fast_round(ProposeId(0, 0), &TestCommand::default(), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, TestCommandResult::default());
}

#[traced_test]
#[tokio::test]
async fn test_unary_fast_round_return_early_err() {
    for early_err in [
        CurpError::duplicated(),
        CurpError::shutting_down(),
        CurpError::invalid_config(),
        CurpError::node_already_exists(),
        CurpError::node_not_exist(),
        CurpError::learner_not_catch_up(),
        CurpError::expired_client_id(),
        CurpError::redirect(Some(1), 0),
    ] {
        assert!(early_err.should_abort_fast_round());
        // record how many times `handle_propose` was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(3, |_id, conn| {
            let counter_c = Arc::clone(&counter);
            let err = early_err.clone();
            conn.expect_propose()
                .return_once(move |_req, _token, _timeout| {
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
        });
        let unary = init_unary_client(connects, None, None, 0, 0, None);
        let err = unary
            .fast_round(ProposeId(0, 0), &TestCommand::default(), None)
            .await
            .unwrap_err();
        assert_eq!(err, early_err);
        assert_eq!(*counter.lock().unwrap(), 1);
    }
}

#[traced_test]
#[tokio::test]
async fn test_unary_fast_round_less_quorum() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(&Ok(
                        TestCommandResult::default(),
                    )),
                    1 | 2 => ProposeResponse::new_empty(),
                    3 | 4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let err = unary
        .fast_round(ProposeId(0, 0), &TestCommand::default(), None)
        .await
        .unwrap_err();
    assert_eq!(err, CurpError::KeyConflict(()));
}

/// FIXME: two leader
/// TODO: fix in subsequence PR
#[traced_test]
#[tokio::test]
#[should_panic]
async fn test_unary_fast_round_with_two_leader() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp =
                    match id {
                        // The execution result has been returned, indicating that server(0) has also recorded the command.
                        0 => ProposeResponse::new_result::<TestCommand>(&Ok(
                            TestCommandResult::new(vec![1], vec![1]),
                        )),
                        // imagine that server(1) is the new leader
                        1 => ProposeResponse::new_result::<TestCommand>(&Ok(
                            TestCommandResult::new(vec![2], vec![2]),
                        )),
                        2 | 3 => ProposeResponse::new_empty(),
                        4 => return Err(CurpError::key_conflict()),
                        _ => unreachable!("there are only 5 nodes"),
                    };
                Ok(tonic::Response::new(resp))
            });
    });
    // old local leader(0), term 1
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let res = unary
        .fast_round(ProposeId(0, 0), &TestCommand::default(), None)
        .await
        .unwrap()
        .unwrap();
    // quorum: server(0, 1, 2, 3)
    assert_eq!(res, TestCommandResult::new(vec![2], vec![2]));
}

// We may encounter this scenario during leader election
#[traced_test]
#[tokio::test]
async fn test_unary_fast_round_without_leader() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp = match id {
                    0 | 1 | 2 | 3 | 4 => ProposeResponse::new_empty(),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    // old local leader(0), term 1
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let res = unary
        .fast_round(ProposeId(0, 0), &TestCommand::default(), None)
        .await
        .unwrap_err();
    // quorum: server(0, 1, 2, 3)
    assert_eq!(res, CurpError::WrongClusterVersion(()));
}

#[traced_test]
#[tokio::test]
async fn test_unary_slow_round_fetch_leader_first() {
    let flag = Arc::new(AtomicBool::new(false));
    let connects = init_mocked_connects(3, |id, conn| {
        let flag_c = Arc::clone(&flag);
        conn.expect_fetch_cluster()
            .return_once(move |_req, _timeout| {
                flag_c.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(tonic::Response::new(FetchClusterResponse {
                    leader_id: Some(0),
                    term: 1,
                    cluster_id: 123,
                    members: vec![
                        Member::new(0, "S0", vec!["A0".to_owned()], [], false),
                        Member::new(1, "S1", vec!["A1".to_owned()], [], false),
                        Member::new(2, "S2", vec!["A2".to_owned()], [], false),
                    ],
                    cluster_version: 1,
                }))
            });
        let flag_c = Arc::clone(&flag);
        conn.expect_wait_synced()
            .return_once(move |_req, _timeout| {
                assert!(id == 0, "wait synced should send to leader");
                assert!(
                    flag_c.load(std::sync::atomic::Ordering::Relaxed),
                    "fetch_leader should invoke first"
                );
                Ok(tonic::Response::new(WaitSyncedResponse::new_from_result::<
                    TestCommand,
                >(
                    Ok(TestCommandResult::default()),
                    Some(Ok(1.into())),
                )))
            });
    });
    let unary = init_unary_client(connects, None, None, 0, 0, None);
    let res = unary.slow_round(ProposeId(0, 0)).await.unwrap().unwrap();
    assert_eq!(LogIndex::from(res.0), 1);
    assert_eq!(res.1, TestCommandResult::default());
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_fast_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(&Ok(
                        TestCommandResult::default(),
                    )),
                    1 | 2 | 3 => ProposeResponse::new_empty(),
                    4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
        conn.expect_wait_synced()
            .return_once(move |_req, _timeout| {
                assert!(id == 0, "wait synced should send to leader");
                std::thread::sleep(Duration::from_millis(100));
                Ok(tonic::Response::new(WaitSyncedResponse::new_from_result::<
                    TestCommand,
                >(
                    Ok(TestCommandResult::default()),
                    Some(Ok(1.into())),
                )))
            });
    });
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let res = unary
        .propose(&TestCommand::default(), None, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, (TestCommandResult::default(), None));
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_slow_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(&Ok(
                        TestCommandResult::default(),
                    )),
                    1 | 2 | 3 => ProposeResponse::new_empty(),
                    4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
        conn.expect_wait_synced()
            .return_once(move |_req, _timeout| {
                assert!(id == 0, "wait synced should send to leader");
                std::thread::sleep(Duration::from_millis(100));
                Ok(tonic::Response::new(WaitSyncedResponse::new_from_result::<
                    TestCommand,
                >(
                    Ok(TestCommandResult::default()),
                    Some(Ok(1.into())),
                )))
            });
    });
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::default(), None, false)
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
    let counter = Arc::new(Mutex::new(0));
    let connects = init_mocked_connects(5, |id, conn| {
        let counter_c = Arc::clone(&counter);
        conn.expect_propose()
            .return_once(move |_req, _token, _timeout| {
                counter_c.lock().unwrap().add_assign(1);
                // insufficient quorum
                let resp = match id {
                    0 => ProposeResponse::new_result::<TestCommand>(&Ok(
                        TestCommandResult::default(),
                    )),
                    1 | 2 => ProposeResponse::new_empty(),
                    3 | 4 => return Err(CurpError::key_conflict()),
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
        conn.expect_wait_synced()
            .return_once(move |_req, _timeout| {
                assert!(id == 0, "wait synced should send to leader");
                std::thread::sleep(Duration::from_millis(100));
                Ok(tonic::Response::new(WaitSyncedResponse::new_from_result::<
                    TestCommand,
                >(
                    Ok(TestCommandResult::default()),
                    Some(Ok(1.into())),
                )))
            });
    });
    let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::default(), None, true)
        .await
        .unwrap()
        .unwrap();
    assert!(
        start_at.elapsed() > Duration::from_millis(100),
        "slow round takes at least 100ms"
    );
    // indicate that we actually run out of fast round
    assert_eq!(*counter.lock().unwrap(), 5);
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
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose()
                .return_once(move |_req, _token, _timeout| {
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_wait_synced()
                .return_once(move |_req, _timeout| {
                    assert!(id == 0, "wait synced should send to leader");
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
        });
        let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
        let err = unary
            .propose(&TestCommand::default(), None, true)
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
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose()
                .return_once(move |_req, _token, _timeout| {
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_wait_synced()
                .return_once(move |_req, _timeout| {
                    assert!(id == 0, "wait synced should send to leader");
                    counter_c.lock().unwrap().add_assign(1);
                    Err(err)
                });
        });
        let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
        let retry = Retry::new(
            unary,
            RetryConfig::new_fixed(Duration::from_millis(100), 5),
            None,
        );
        let err = retry
            .propose(&TestCommand::default(), None, false)
            .await
            .unwrap_err();
        assert_eq!(err.message(), tonic::Status::from(early_err).message());
        // fast path + slow path = 2
        assert_eq!(*counter.lock().unwrap(), 2);
    }
}

#[traced_test]
#[tokio::test]
async fn test_retry_propose_return_retry_error() {
    for early_err in [
        CurpError::expired_client_id(),
        CurpError::key_conflict(),
        CurpError::RpcTransport(()),
        CurpError::internal("No reason"),
    ] {
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            conn.expect_fetch_cluster()
                .returning(move |_req, _timeout| {
                    Ok(tonic::Response::new(FetchClusterResponse {
                        leader_id: Some(0),
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
            conn.expect_propose()
                .returning(move |_req, _token, _timeout| Err(err.clone()));
            if id == 0 {
                let err = early_err.clone();
                conn.expect_wait_synced()
                    .times(5) // wait synced should be retried in 5 times on leader
                    .returning(move |_req, _timeout| Err(err.clone()));
            }
        });
        let unary = init_unary_client(connects, None, Some(0), 1, 0, None);
        let retry = Retry::new(
            unary,
            RetryConfig::new_fixed(Duration::from_millis(10), 5),
            None,
        );
        let err = retry
            .propose(&TestCommand::default(), None, false)
            .await
            .unwrap_err();
        assert!(err.message().contains("request timeout"));
    }
}

// Tests for stream client

struct MockedStreamConnectApi {
    id: ServerId,
    lease_keep_alive_handle:
        Box<dyn Fn(Arc<AtomicU64>) -> BoxFuture<'static, CurpError> + Send + Sync + 'static>,
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
    async fn propose(
        &self,
        _request: ProposeRequest,
        _token: Option<String>,
        _timeout: Duration,
    ) -> Result<tonic::Response<ProposeResponse>, CurpError> {
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

    /// Send `WaitSyncedRequest`
    async fn wait_synced(
        &self,
        _request: WaitSyncedRequest,
        _timeout: Duration,
    ) -> Result<tonic::Response<WaitSyncedResponse>, CurpError> {
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
    async fn lease_keep_alive(&self, client_id: Arc<AtomicU64>, _interval: Duration) -> CurpError {
        (self.lease_keep_alive_handle)(Arc::clone(&client_id)).await
    }
}

/// Create mocked stream connects
/// The leader is S0
#[allow(trivial_casts)] // cannot be inferred
fn init_mocked_stream_connects(
    size: usize,
    leader_idx: usize,
    leader_term: u64,
    keep_alive_handle: impl Fn(Arc<AtomicU64>) -> BoxFuture<'static, CurpError> + Send + Sync + 'static,
) -> HashMap<ServerId, Arc<dyn ConnectApi>> {
    let mut keep_alive_handle = Some(keep_alive_handle);
    let redirect_handle = move |_id| {
        Box::pin(async move { CurpError::redirect(Some(leader_idx as ServerId), leader_term) })
            as BoxFuture<'static, CurpError>
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
                    0,
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
                    0,
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
        Box::pin(async move { panic!("should not invoke lease_keep_alive in bypassed connection") })
    });
    let stream = init_stream_client(connects, Some(0), Some(0), 1, 1);
    tokio::time::timeout(Duration::from_millis(100), stream.keep_heartbeat())
        .await
        .unwrap_err();
    assert_ne!(stream.state.client_id(), 0);
}

#[traced_test]
#[tokio::test]
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
