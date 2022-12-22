//! Integration test for the curp server

use std::time::Duration;

use tracing_test::traced_test;

use crate::common::{curp_group::CurpGroup, test_cmd::TestCommand};

mod common;

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
