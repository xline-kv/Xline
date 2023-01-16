//! Integration test for the curp server

use std::time::Duration;

use crate::common::{curp_group::CurpGroup, init_logger, test_cmd::TestCommand};

mod common;

#[tokio::test]
async fn basic_propose() {
    init_logger();

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

    group.stop();
}

#[tokio::test]
async fn synced_propose() {
    init_logger();

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

    group.stop();
}

// Each command should be executed once and only once on each node
#[tokio::test]
async fn exe_exact_n_times() {
    init_logger();

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

    group.stop();
}

#[tokio::test]
async fn leader_crash_and_recovery() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client().await;

    let leader = group.fetch_leader().await.unwrap();
    group.crash(&leader);

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

    // restart the original leader
    group.restart(&leader).await;
    let old_leader = group.nodes.get_mut(&leader).unwrap();

    let er = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let er = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![0]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    group.stop();
}

#[tokio::test]
async fn follower_crash_and_recovery() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client().await;

    let leader = group.fetch_leader().await.unwrap();
    let follower = group
        .nodes
        .keys()
        .find(|&id| id != &leader)
        .unwrap()
        .clone();
    group.crash(&follower);

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

    // let cmds to be synced
    tokio::time::sleep(Duration::from_secs(2)).await;

    // restart follower
    group.restart(&follower).await;
    let follower = group.nodes.get_mut(&follower).unwrap();

    let er = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let er = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![0]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    group.stop();
}

#[tokio::test]
async fn leader_and_follower_both_crash_and_recovery() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client().await;

    let leader = group.fetch_leader().await.unwrap();
    let follower = group
        .nodes
        .keys()
        .find(|&id| id != &leader)
        .unwrap()
        .clone();
    group.crash(&follower);

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

    // let cmds to be synced
    tokio::time::sleep(Duration::from_secs(2)).await;
    group.crash(&leader);

    // restart the original leader
    group.restart(&leader).await;
    let old_leader = group.nodes.get_mut(&leader).unwrap();

    let er = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let er = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![0]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    // restart follower
    group.restart(&follower).await;
    let follower = group.nodes.get_mut(&follower).unwrap();

    let er = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let er = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.1, vec![0]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    group.stop();
}
