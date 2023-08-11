//! Integration test for the curp server

use std::{sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use curp::client::Builder;
use curp_test_utils::{init_logger, sleep_millis, sleep_secs, test_cmd::TestCommand};
use madsim::rand::{thread_rng, Rng};
use test_macros::abort_on_panic;
use utils::config::ClientTimeout;

use crate::common::curp_group::{
    proto::propose_response::ExeResult, CurpGroup, ProposeRequest, ProposeResponse,
};

mod common;

#[tokio::test]
#[abort_on_panic]
async fn basic_propose() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client(ClientTimeout::default()).await;

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0), true)
            .await
            .unwrap()
            .0,
        (vec![], vec![])
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]), true)
            .await
            .unwrap()
            .0,
        (vec![0], vec![1])
    );

    group.stop();
}

#[tokio::test]
#[abort_on_panic]
async fn client_build_from_addrs_shoulde_fetch_cluster_from_server() {
    init_logger();
    let group = CurpGroup::new(3).await;

    let all_addrs = group.all.values().cloned().collect::<Vec<_>>();
    let _client = Builder::<TestCommand>::default()
        .timeout(ClientTimeout::default())
        .build_from_addrs(all_addrs)
        .await
        .unwrap();

    group.stop();
}

#[tokio::test]
#[abort_on_panic]
async fn synced_propose() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;
    let cmd = TestCommand::new_get(vec![0]);

    let (er, index) = client.propose(cmd.clone(), false).await.unwrap();
    assert_eq!(er, (vec![], vec![]));
    assert_eq!(index.unwrap(), 1); // log[0] is a fake one

    for exe_rx in group.exe_rxs() {
        let (cmd1, er) = exe_rx.recv().await.unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(er, (vec![], vec![]));
    }

    for as_rx in group.as_rxs() {
        let (cmd1, index) = as_rx.recv().await.unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(index, 1);
    }

    group.stop();
}

// Each command should be executed once and only once on each node
#[tokio::test]
#[abort_on_panic]
async fn exe_exact_n_times() {
    init_logger();

    let mut group = CurpGroup::new(3).await;
    let client = group.new_client(ClientTimeout::default()).await;
    let cmd = TestCommand::new_get(vec![0]);

    let er = client.propose(cmd.clone(), true).await.unwrap().0;
    assert_eq!(er, (vec![], vec![]));

    for exe_rx in group.exe_rxs() {
        let (cmd1, er) = exe_rx.recv().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exe_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(cmd1, cmd);
        assert_eq!(er.0, vec![]);
    }

    for as_rx in group.as_rxs() {
        let (cmd1, index) = as_rx.recv().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), as_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(cmd1, cmd);
        assert_eq!(index, 1);
    }

    group.stop();
}

// To verify PR #86 is fixed
#[tokio::test]
#[abort_on_panic]
async fn fast_round_is_slower_than_slow_round() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let cmd = Arc::new(TestCommand::new_get(vec![0]));

    let leader = group.get_leader().await.0;

    // send propose only to the leader
    let mut leader_connect = group.get_connect(&leader).await;
    leader_connect
        .propose(tonic::Request::new(ProposeRequest {
            command: bincode::serialize(&cmd).unwrap(),
        }))
        .await
        .unwrap();

    // wait for the command to be synced to others
    // because followers never get the cmd from the client, it will mark the cmd done in spec pool instead of removing the cmd from it
    tokio::time::sleep(Duration::from_secs(1)).await;

    // send propose to follower
    let follower_addr = group.all.keys().find(|&id| &leader != id).unwrap();
    let mut follower_connect = group.get_connect(follower_addr).await;

    // the follower should response empty immediately
    let resp: ProposeResponse = follower_connect
        .propose(tonic::Request::new(ProposeRequest {
            command: bincode::serialize(&cmd).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.exe_result.is_none());

    group.stop();
}

#[tokio::test]
#[abort_on_panic]
async fn concurrent_cmd_order() {
    init_logger();

    let cmd0 = TestCommand::new_put(vec![0], 0).set_exe_dur(Duration::from_secs(1));
    let cmd1 = TestCommand::new_put(vec![0, 1], 1);
    let cmd2 = TestCommand::new_put(vec![1], 2);

    let group = CurpGroup::new(3).await;
    let leader = group.get_leader().await.0;
    let mut leader_connect = group.get_connect(&leader).await;

    let mut c = leader_connect.clone();
    tokio::spawn(async move {
        c.propose(ProposeRequest {
            command: bincode::serialize(&cmd0).unwrap(),
        })
        .await
        .expect("propose failed");
    });

    sleep_millis(20).await;
    let response = leader_connect
        .propose(ProposeRequest {
            command: bincode::serialize(&cmd1).unwrap(),
        })
        .await
        .expect("propose failed")
        .into_inner();
    assert!(matches!(response.exe_result.unwrap(), ExeResult::Error(_)));
    let response = leader_connect
        .propose(ProposeRequest {
            command: bincode::serialize(&cmd2).unwrap(),
        })
        .await
        .expect("propose failed")
        .into_inner();
    assert!(matches!(response.exe_result.unwrap(), ExeResult::Error(_)));

    sleep_secs(1).await;

    let client = group.new_client(ClientTimeout::default()).await;

    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![1]), true)
            .await
            .unwrap()
            .0
             .0,
        vec![2]
    );

    group.stop();
}

/// This test case ensures that the issue 228 is fixed.
#[tokio::test]
#[abort_on_panic]
async fn concurrent_cmd_order_should_have_correct_revision() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client(ClientTimeout::default()).await;

    let sample_range = 1..=100;

    for i in sample_range.clone() {
        let rand_dur = Duration::from_millis(thread_rng().gen_range(0..500).numeric_cast());
        let _er = client
            .propose(TestCommand::new_put(vec![i], i).set_as_dur(rand_dur), true)
            .await
            .unwrap();
    }

    for i in sample_range {
        assert_eq!(
            client
                .propose(TestCommand::new_get(vec![i]), true)
                .await
                .unwrap()
                .0
                 .1,
            vec![i.numeric_cast::<i64>()]
        )
    }

    group.stop();
}
