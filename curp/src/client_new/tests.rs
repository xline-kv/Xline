use std::{
    collections::HashMap,
    ops::AddAssign,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Duration,
};

use curp_external_api::LogIndex;
use curp_test_utils::test_cmd::{LogIndexResult, TestCommand, TestCommandResult};
use tokio::time::Instant;
use tracing_test::traced_test;

use super::{
    retry::{Retry, RetryConfig},
    unary::Unary,
};
use crate::{
    client_new::ClientApi,
    members::ServerId,
    rpc::{
        connect::{ConnectApi, MockConnectApi},
        CurpError, CurpErrorPriority, FetchClusterResponse, Member, ProposeId, ProposeResponse,
        WaitSyncedResponse,
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
                    Member::new(0, "S0", vec!["A0".to_owned()], false),
                    Member::new(1, "S1", vec!["A1".to_owned()], false),
                    Member::new(2, "S2", vec!["A2".to_owned()], false),
                ],
                cluster_version: 1,
            }))
        });
    });
    let unary = Unary::<TestCommand>::new(connects, None, None, None);
    assert!(unary.local_connect().await.is_none());
    let res = unary.fetch_cluster(false).await.unwrap();
    assert_eq!(
        res.into_members_addrs(),
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
    let unary = Unary::<TestCommand>::new(connects, Some(1), None, None);
    assert!(unary.local_connect().await.is_some());
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
                            Member::new(0, "S0", vec!["A0".to_owned()], false),
                            Member::new(1, "S1", vec!["A1".to_owned()], false),
                            Member::new(2, "S2", vec!["A2".to_owned()], false),
                            Member::new(3, "S3", vec!["A3".to_owned()], false),
                            Member::new(4, "S4", vec!["A4".to_owned()], false),
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
                            Member::new(0, "S0", vec!["B0".to_owned()], false),
                            Member::new(1, "S1", vec!["B1".to_owned()], false),
                            Member::new(2, "S2", vec!["B2".to_owned()], false),
                            Member::new(3, "S3", vec!["B3".to_owned()], false),
                            Member::new(4, "S4", vec!["B4".to_owned()], false),
                        ],
                        cluster_version: 1,
                    },
                    _ => unreachable!("there are only 5 nodes"),
                };
                Ok(tonic::Response::new(resp))
            });
    });
    let unary = Unary::<TestCommand>::new(connects, None, None, None);
    assert!(unary.local_connect().await.is_none());
    let res = unary.fetch_cluster(true).await.unwrap();
    assert_eq!(
        res.into_members_addrs(),
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
                            Member::new(0, "S0", vec!["A0".to_owned()], false),
                            Member::new(1, "S1", vec!["A1".to_owned()], false),
                            Member::new(2, "S2", vec!["A2".to_owned()], false),
                            Member::new(3, "S3", vec!["A3".to_owned()], false),
                            Member::new(4, "S4", vec!["A4".to_owned()], false),
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
                            Member::new(0, "S0", vec!["B0".to_owned()], false),
                            Member::new(1, "S1", vec!["B1".to_owned()], false),
                            Member::new(2, "S2", vec!["B2".to_owned()], false),
                            Member::new(3, "S3", vec!["B3".to_owned()], false),
                            Member::new(4, "S4", vec!["B4".to_owned()], false),
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
    let unary = Unary::<TestCommand>::new(connects, None, None, None);
    assert!(unary.local_connect().await.is_none());
    let res = unary.fetch_cluster(true).await.unwrap_err();
    // only server(0, 1)'s responses are valid, less than majority quorum(3), got a mocked RpcTransport to retry
    assert_eq!(res, CurpError::RpcTransport(()));
}

#[traced_test]
#[tokio::test]
async fn test_unary_fast_round_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose().return_once(move |_req, _timeout| {
            let resp = match id {
                0 => ProposeResponse::new_result::<TestCommand>(&Ok(TestCommandResult::default())),
                1 | 2 | 3 => ProposeResponse::new_empty(),
                4 => return Err(CurpError::key_conflict()),
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });
    let unary = Unary::<TestCommand>::new(connects, None, None, None);
    let res = unary
        .fast_round(ProposeId(0, 0), &TestCommand::default())
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
        assert_eq!(early_err.priority(), CurpErrorPriority::ReturnImmediately);
        // record how many times `handle_propose` was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(3, |_id, conn| {
            let counter_c = Arc::clone(&counter);
            let err = early_err.clone();
            conn.expect_propose().return_once(move |_req, _timeout| {
                counter_c.lock().unwrap().add_assign(1);
                Err(err)
            });
        });
        let unary = Unary::<TestCommand>::new(connects, None, None, None);
        let err = unary
            .fast_round(ProposeId(0, 0), &TestCommand::default())
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
        conn.expect_propose().return_once(move |_req, _timeout| {
            let resp = match id {
                0 => ProposeResponse::new_result::<TestCommand>(&Ok(TestCommandResult::default())),
                1 | 2 => ProposeResponse::new_empty(),
                3 | 4 => return Err(CurpError::key_conflict()),
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });
    let unary = Unary::<TestCommand>::new(connects, None, None, None);
    let err = unary
        .fast_round(ProposeId(0, 0), &TestCommand::default())
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
        conn.expect_propose().return_once(move |_req, _timeout| {
            let resp = match id {
                // The execution result has been returned, indicating that server(0) has also recorded the command.
                0 => ProposeResponse::new_result::<TestCommand>(&Ok(TestCommandResult::new(
                    vec![1],
                    vec![1],
                ))),
                // imagine that server(1) is the new leader
                1 => ProposeResponse::new_result::<TestCommand>(&Ok(TestCommandResult::new(
                    vec![2],
                    vec![2],
                ))),
                2 | 3 => ProposeResponse::new_empty(),
                4 => return Err(CurpError::key_conflict()),
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });
    // old local leader(0), term 1
    let unary = Unary::<TestCommand>::new(connects, None, Some(0), Some(1));
    let res = unary
        .fast_round(ProposeId(0, 0), &TestCommand::default())
        .await
        .unwrap()
        .unwrap();
    // quorum: server(0, 1, 2, 3)
    assert_eq!(res, TestCommandResult::new(vec![2], vec![2]));
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
                        Member::new(0, "S0", vec!["A0".to_owned()], false),
                        Member::new(1, "S1", vec!["A1".to_owned()], false),
                        Member::new(2, "S2", vec!["A2".to_owned()], false),
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
    let unary = Unary::<TestCommand>::new(connects, None, None, None);
    let res = unary.slow_round(ProposeId(0, 0)).await.unwrap().unwrap();
    assert_eq!(LogIndex::from(res.0), 1);
    assert_eq!(res.1, TestCommandResult::default());
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_fast_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose().return_once(move |_req, _timeout| {
            let resp = match id {
                0 => ProposeResponse::new_result::<TestCommand>(&Ok(TestCommandResult::default())),
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
    let unary = Unary::<TestCommand>::new(connects, None, Some(0), Some(1));
    let res = unary
        .propose(&TestCommand::default(), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res, (TestCommandResult::default(), None));
}

#[traced_test]
#[tokio::test]
async fn test_unary_propose_slow_path_works() {
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose().return_once(move |_req, _timeout| {
            let resp = match id {
                0 => ProposeResponse::new_result::<TestCommand>(&Ok(TestCommandResult::default())),
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
    let unary = Unary::<TestCommand>::new(connects, None, Some(0), Some(1));
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::default(), false)
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
        conn.expect_propose().return_once(move |_req, _timeout| {
            counter_c.lock().unwrap().add_assign(1);
            // insufficient quorum
            let resp = match id {
                0 => ProposeResponse::new_result::<TestCommand>(&Ok(TestCommandResult::default())),
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
    let unary = Unary::<TestCommand>::new(connects, None, Some(0), Some(1));
    let start_at = Instant::now();
    let res = unary
        .propose(&TestCommand::default(), true)
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
        CurpError::duplicated(),
        CurpError::shutting_down(),
        CurpError::invalid_config(),
        CurpError::node_already_exists(),
        CurpError::node_not_exist(),
        CurpError::learner_not_catch_up(),
        CurpError::expired_client_id(),
        CurpError::redirect(Some(1), 0),
    ] {
        assert_eq!(early_err.priority(), CurpErrorPriority::ReturnImmediately);
        // record how many times rpc was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose().return_once(move |_req, _timeout| {
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
        let unary = Unary::<TestCommand>::new(connects, None, Some(0), Some(1));
        let err = unary
            .propose(&TestCommand::default(), true)
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
        CurpError::duplicated(),
        CurpError::shutting_down(),
        CurpError::invalid_config(),
        CurpError::node_already_exists(),
        CurpError::node_not_exist(),
        CurpError::learner_not_catch_up(),
    ] {
        // all no retry errors are returned early
        assert_eq!(early_err.priority(), CurpErrorPriority::ReturnImmediately);
        // record how many times rpc was invoked.
        let counter = Arc::new(Mutex::new(0));
        let connects = init_mocked_connects(5, |id, conn| {
            let err = early_err.clone();
            let counter_c = Arc::clone(&counter);
            conn.expect_propose().return_once(move |_req, _timeout| {
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
        let unary = Unary::<TestCommand>::new(connects, None, Some(0), Some(1));
        let retry = Retry::new(unary, RetryConfig::new_fixed(Duration::from_millis(100), 5));
        let err = retry
            .propose(&TestCommand::default(), false)
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
            conn.expect_propose()
                .returning(move |_req, _timeout| Err(err.clone()));
            if id == 0 {
                let err = early_err.clone();
                conn.expect_wait_synced()
                    .times(5) // wait synced should be retried in 5 times on leader
                    .returning(move |_req, _timeout| Err(err.clone()));
            }
        });
        let unary = Unary::<TestCommand>::new(connects, None, Some(0), Some(1));
        let retry = Retry::new(unary, RetryConfig::new_fixed(Duration::from_millis(10), 5));
        let err = retry
            .propose(&TestCommand::default(), false)
            .await
            .unwrap_err();
        assert!(err.message().contains("request timeout"));
    }
}
