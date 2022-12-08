#![allow(
    clippy::all,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::integer_arithmetic,
    clippy::str_to_string,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::shadow_unrelated,
    dead_code,
    unused_results
)]

mod curp_group;
mod test_cmd;

use std::{sync::Arc, time::Duration};

use tracing::debug;
use tracing_test::traced_test;

use crate::{
    rpc::ProposeRequest,
    server::tests::{curp_group::CurpGroup, test_cmd::TestCommand},
};

// Initial election
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn initial_election() {
    // watch the log while doing sync, TODO: find a better way
    let group = CurpGroup::new(3).await;

    // check whether there is exact one leader in the group
    let leader1 = group.get_leader().expect("There should be one leader");
    let term1 = group.get_term_checked();

    // check after some time, the term and the leader is still not changed
    tokio::time::sleep(Duration::from_secs(1)).await;
    let leader2 = group.get_leader().expect("There should be one leader");
    let term2 = group.get_term_checked();

    assert_eq!(term1, term2);
    assert_eq!(leader1, leader2);
}

// Reelect after network failure
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn reelect() {
    let group = CurpGroup::new(5).await;

    // check whether there is exact one leader in the group
    let leader1 = group.get_leader().expect("There should be one leader");
    let term1 = group.get_term_checked();

    ////////// disable leader 1
    group.disable_node(&leader1);
    debug!("disable leader {leader1}");

    // after some time, a new leader should be elected
    tokio::time::sleep(Duration::from_secs(1)).await;
    let leader2 = group.get_leader().expect("There should be one leader");
    let term2 = group.get_leader_term();

    assert_ne!(term1, term2);
    assert_ne!(leader1, leader2);

    ////////// disable leader 2
    group.disable_node(&leader2);
    debug!("disable leader {leader2}");

    // after some time, a new leader should be elected
    tokio::time::sleep(Duration::from_secs(1)).await;
    let leader3 = group.get_leader().expect("There should be one leader");
    let term3 = group.get_leader_term();

    assert_ne!(term1, term3);
    assert_ne!(term2, term3);
    assert_ne!(leader1, leader3);
    assert_ne!(leader2, leader3);

    ////////// disable leader 3
    group.disable_node(&leader3);
    debug!("disable leader {leader3}");

    // after some time, no leader should be elected
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(group.get_leader().is_none());

    ////////// recover network partition
    group.enable_node(&leader1);
    group.enable_node(&leader2);
    group.enable_node(&leader3);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(group.get_leader().is_some());
    assert!(group.get_term_checked() > term3);
}

// Basic propose
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn basic_propose() {
    // watch the log while doing sync, TODO: find a better way
    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    assert_eq!(
        client
            .propose(TestCommand::new_put(0, vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(1, vec![0]))
            .await
            .unwrap(),
        vec![0]
    );
}

// Propose after reelection. Client should learn about the new server.
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn propose_after_reelect() {
    let group = CurpGroup::new(5).await;
    let client = group.new_client().await;
    assert_eq!(
        client
            .propose(TestCommand::new_put(0, vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    // wait for the cmd to be synced
    tokio::time::sleep(Duration::from_secs(1)).await;

    let leader1 = group.get_leader().expect("There should be one leader");
    group.disable_node(&leader1);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        client
            .propose(TestCommand::new_get(1, vec![0]))
            .await
            .unwrap(),
        vec![0]
    );
}

#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn synced_propose() {
    let mut group = CurpGroup::new(5).await;
    let client = group.new_client().await;
    let cmd = TestCommand::new_get(0, vec![0]);

    let (er, index) = client.propose_indexed(cmd.clone()).await.unwrap();
    assert_eq!(er, vec![]);
    assert_eq!(index, 1); // log[0] is a fake one

    for exe_rx in group.exe_rxs() {
        let (cmd1, er) = exe_rx.recv().await.unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(er, vec![]);
    }

    for as_rx in group.as_rxs() {
        let (cmd1, index) = as_rx.recv().await.unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(index, 1);
    }
}

// Each command should be executed once and only once on each node
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn exe_exact_n_times() {
    let mut group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    let cmd = TestCommand::new_get(0, vec![0]);

    let er = client.propose(cmd.clone()).await.unwrap();
    assert_eq!(er, vec![]);

    for exe_rx in group.exe_rxs() {
        let (cmd1, er) = exe_rx.recv().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exe_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(cmd1, cmd);
        assert_eq!(er, vec![]);
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
}

// To verify PR #86 is fixed
#[traced_test]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn fast_round_is_slower_than_slow_round() {
    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    let cmd = Arc::new(TestCommand::new_get(0, vec![0]));

    let leader = group.get_leader().expect("There should be one leader");

    let connects = client.get_connects();
    let leader_connect = connects
        .iter()
        .find(|connect| connect.id() == &leader)
        .unwrap();
    leader_connect
        .propose(
            ProposeRequest::new_from_rc(Arc::clone(&cmd)).unwrap(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    // wait for the command to be synced to others
    // because followers never get the cmd from the client, it will mark the cmd done in spec pool instead of removing the cmd from it
    tokio::time::sleep(Duration::from_secs(1)).await;

    let follower_connect = connects
        .iter()
        .find(|connect| connect.id() != &leader)
        .unwrap();

    // the follower should response empty immediately
    let resp = follower_connect
        .propose(
            ProposeRequest::new_from_rc(cmd).unwrap(),
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .into_inner();
    assert!(resp.exe_result.is_none());
}
