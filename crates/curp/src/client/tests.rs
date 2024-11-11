use std::{
    collections::{BTreeSet, HashMap},
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
        cluster_state::ClusterStateFull,
        connect::NonRepeatableClientApi,
        fetch::Fetch,
        retry::{Context, Retry, RetryConfig},
        ClientApi,
    },
    member::Membership,
    members::ServerId,
    rpc::{
        self,
        connect::{ConnectApi, MockConnectApi},
        Change, CurpError, MembershipResponse, Node, NodeMetadata, OpResponse, ProposeId,
        ProposeResponse, ReadIndexResponse, RecordResponse, ResponseOp, SyncedResponse,
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
        0,
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

pub(super) fn build_default_membership() -> Membership {
    let members = (0..5).collect::<BTreeSet<_>>();
    let nodes = members
        .iter()
        .map(|id| (*id, NodeMetadata::default()))
        .collect();
    Membership::new(vec![members], nodes)
}

fn build_membership_resp(
    leader_id: Option<u64>,
    term: u64,
    members: impl IntoIterator<Item = u64>,
    learners: impl IntoIterator<Item = u64>,
) -> Result<tonic::Response<MembershipResponse>, CurpError> {
    let leader_id = leader_id.ok_or(CurpError::leader_transfer("no current leader"))?;

    let members: Vec<_> = members.into_iter().collect();
    let nodes: Vec<Node> = members
        .clone()
        .into_iter()
        .chain(learners)
        .map(|node_id| Node {
            node_id,
            meta: Some(NodeMetadata::default()),
        })
        .collect();
    let qs = rpc::QuorumSet { set: members };

    let resp = MembershipResponse {
        members: vec![qs],
        nodes,
        term,
        leader_id,
    };
    Ok(tonic::Response::new(resp))
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
                1 | 2 | 3 => RecordResponse {
                    conflict: false,
                    sp_version: 0,
                },
                4 => RecordResponse {
                    conflict: true,
                    sp_version: 0,
                },
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });
    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
    let ctx = Context::new(ProposeId::default(), cluster_state);
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
                1 | 2 | 3 => RecordResponse {
                    conflict: false,
                    sp_version: 0,
                },
                4 => RecordResponse {
                    conflict: true,
                    sp_version: 0,
                },
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });

    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
    let ctx = Context::new(ProposeId::default(), cluster_state);
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
                1 | 2 => RecordResponse {
                    conflict: false,
                    sp_version: 0,
                },
                3 | 4 => RecordResponse {
                    conflict: true,
                    sp_version: 0,
                },
                _ => unreachable!("there are only 5 nodes"),
            };
            Ok(tonic::Response::new(resp))
        });
    });

    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
    let ctx = Context::new(ProposeId::default(), cluster_state);
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
        let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
        let ctx = Context::new(ProposeId::default(), cluster_state);
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
        let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
        let retry = Retry::new(
            unary,
            RetryConfig::new_fixed(Duration::from_millis(100), 5),
            Fetch::new_disable(),
            ClusterState::Full(cluster_state),
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
            conn.expect_fetch_membership()
                .returning(move |_req, _timeout| {
                    build_membership_resp(Some(0), 2, vec![0, 1, 2, 3, 4], [])
                });
            if id == 0 {
                let err = early_err.clone();
                conn.expect_shutdown()
                    .times(5) // propose should be retried in 5 times on leader
                    .returning(move |_req, _timeout| Err(err.clone()));
            }

            let err = early_err.clone();
            conn.expect_record()
                .returning(move |_req, _timeout| Err(err.clone()));
        });
        let unary = init_unary_client(None, None);
        let cluster_state =
            ClusterStateFull::new(0, 1, connects.clone(), build_default_membership());
        let retry = Retry::new(
            unary,
            RetryConfig::new_fixed(Duration::from_millis(10), 5),
            Fetch::new(Duration::from_secs(1), move |_| connects.clone()),
            ClusterState::Full(cluster_state),
        );
        // Propose shutdown is a retryable request
        let _err = retry.propose_shutdown().await.unwrap_err();
    }
}

#[traced_test]
#[tokio::test]
async fn test_retry_will_update_state_on_error() {
    let mut return_cnt = [0; 5];
    let connects = init_mocked_connects(5, |id, conn| {
        conn.expect_propose_stream()
            .returning(move |_req, _token, _timeout| {
                return_cnt[id] += 1;
                match return_cnt[id] {
                    // on first propose, return an error; the client should update its state
                    1 => Err(CurpError::wrong_cluster_version()),
                    // on second propose, return success result
                    2 => {
                        let resp = async_stream::stream! {
                            yield Ok(build_propose_response(false));
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            yield Ok(build_synced_response());
                        };
                        Ok(tonic::Response::new(Box::new(resp)))
                    }
                    _ => unreachable!(),
                }
            });

        conn.expect_record()
            .return_once(move |_req, _timeout| Err(CurpError::internal("none")));

        conn.expect_fetch_membership()
            .returning(move |_req, _timeout| {
                build_membership_resp(Some(0), 1, vec![0, 1, 2, 3], [4])
            });
    });
    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateFull::new(0, 1, connects.clone(), build_default_membership());
    let retry = Retry::new(
        unary,
        RetryConfig::new_fixed(Duration::from_millis(10), 5),
        Fetch::new(Duration::from_secs(1), move |_| connects.clone()),
        ClusterState::Full(cluster_state),
    );
    let _err = retry
        .propose(&TestCommand::new_put(vec![1], 1), None, false)
        .await
        .unwrap_err();
    // on a retry the client should update the cluster state
    let _result = retry
        .propose(&TestCommand::new_put(vec![1], 1), None, false)
        .await
        .unwrap();

    // The state should update to the new membership
    let state = retry.cluster_state().unwrap_full_state();
    let members = (0..4).collect::<BTreeSet<_>>();
    let nodes = (0..5).map(|id| (id, NodeMetadata::default())).collect();
    let expect_membership = Membership::new(vec![members], nodes);
    assert_eq!(*state.membership(), expect_membership);
}

#[traced_test]
#[tokio::test]
async fn test_retry_will_update_state_on_change_membership() {
    let connects = init_mocked_connects(5, |_id, conn| {
        conn.expect_fetch_membership()
            .returning(move |_req, _timeout| {
                build_membership_resp(Some(0), 2, vec![0, 1, 2, 3, 4], [])
            });
        conn.expect_change_membership()
            .returning(move |_req, _timeout| {
                build_membership_resp(Some(0), 2, vec![0, 1, 2, 3], [4])
            });
    });
    let unary = init_unary_client(None, None);
    let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
    let retry = Retry::new(
        unary,
        RetryConfig::new_fixed(Duration::from_millis(10), 5),
        Fetch::new_disable(),
        ClusterState::Full(cluster_state),
    );

    retry
        .change_membership(vec![Change::Demote(4)])
        .await
        .unwrap();
    // The state should update to the changed membership
    let state = retry.cluster_state().unwrap_full_state();
    let members = (0..4).collect::<BTreeSet<_>>();
    let nodes = (0..5).map(|id| (id, NodeMetadata::default())).collect();
    let expect_membership = Membership::new(vec![members], nodes);
    assert_eq!(*state.membership(), expect_membership);
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
    let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
    let ctx = Context::new(ProposeId::default(), cluster_state);
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
    let cluster_state = ClusterStateFull::new(0, 1, connects, build_default_membership());
    let ctx = Context::new(ProposeId::default(), cluster_state);
    let res = unary
        .propose(&TestCommand::default(), None, true, ctx)
        .await;
    assert!(res.is_err());
}
