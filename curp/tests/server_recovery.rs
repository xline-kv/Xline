//! Integration test for the curp server

use std::{sync::Arc, time::Duration};

use engine::StorageEngine;
use itertools::Itertools;
use tracing::debug;
use utils::config::ClientTimeout;

use crate::common::{
    curp_group::{CurpGroup, ProposeRequest},
    init_logger, sleep_millis, sleep_secs,
    test_cmd::TestCommand,
    TEST_TABLE,
};

mod common;

#[tokio::test]
async fn leader_crash_and_recovery() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;

    let leader = group.try_get_leader().await.unwrap().0;
    group.crash(&leader);

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );

    // restart the original leader
    sleep_secs(3).await;
    group.restart(&leader, false).await;
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
    let client = group.new_client(ClientTimeout::default()).await;

    let leader = group.try_get_leader().await.unwrap().0;
    let follower = group
        .nodes
        .keys()
        .find(|&id| id != &leader)
        .unwrap()
        .clone();
    group.crash(&follower);

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );

    // let cmds to be synced
    tokio::time::sleep(Duration::from_secs(2)).await;

    // restart follower
    group.restart(&follower, false).await;
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
    let client = group.new_client(ClientTimeout::default()).await;

    let leader = group.try_get_leader().await.unwrap().0;
    let follower = group
        .nodes
        .keys()
        .find(|&id| id != &leader)
        .unwrap()
        .clone();
    group.crash(&follower);

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );

    // let cmds to be synced
    tokio::time::sleep(Duration::from_secs(2)).await;
    group.crash(&leader);

    // restart the original leader
    group.restart(&leader, false).await;
    // add a new log to commit previous logs
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );
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
    group.restart(&follower, false).await;
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

// Leader should recover speculatively executed commands
#[tokio::test]
async fn new_leader_will_recover_spec_cmds_cond1() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;

    let leader1 = group.get_leader().await.0;

    // 1: send cmd1 to all others except the leader
    let cmd1 = Arc::new(TestCommand::new_put(vec![0], 0));
    let req1 = ProposeRequest {
        command: bincode::serialize(&cmd1).unwrap(),
    };
    for id in group.all.keys().filter(|&id| id != &leader1).take(4) {
        let mut connect = group.get_connect(id).await;
        connect.propose(req1.clone()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 2: disable leader1
    group.disable_node(&leader1);

    // 3: the client should automatically find the new leader and get the response
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );

    // old leader should recover from the new leader
    group.enable_node(&leader1);

    // every cmd should be executed and after synced on every node
    for rx in group.exe_rxs() {
        rx.recv().await;
        rx.recv().await;
    }
    for rx in group.as_rxs() {
        rx.recv().await;
        rx.recv().await;
    }

    group.stop();
}

#[tokio::test]
async fn new_leader_will_recover_spec_cmds_cond2() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;

    let leader1 = group.get_leader().await.0;

    // 1: disable leader1
    group.disable_node(&leader1);

    // now when the client proposes, all others will receive the proposal.
    // but since a new round of election has not started yet, none of them will execute them
    // when a new leader is elected, the cmd will be recovered(because it has been replicated on all others)
    // now the client will resend the proposal to the new leader, asking it to sync again(the leader could have already completed sync or is syncing)
    // the new leader should return empty, asking the client to fall back to wait synced

    // 2: the client should automatically find the new leader and get the response
    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );

    group.stop();
}

// Old Leader should discard spec states
#[tokio::test]
async fn old_leader_will_discard_spec_exe_cmds() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;

    // 0: let's first propose an initial cmd0
    let cmd0 = TestCommand::new_put(vec![0], 0);
    let (er, index) = client.propose_indexed(cmd0).await.unwrap();
    assert_eq!(er, vec![]);
    assert_eq!(index, 1);
    sleep_secs(1).await;

    // 1: disable all others to prevent the cmd1 to be synced
    let leader1 = group.get_leader().await.0;
    for node in group.nodes.values().filter(|node| node.id != leader1) {
        group.disable_node(&node.id);
    }

    // 2: send the cmd1 to the leader, it should be speculatively executed
    let cmd1 = Arc::new(TestCommand::new_put(vec![0], 1));
    let req1 = ProposeRequest {
        command: bincode::serialize(&cmd1).unwrap(),
    };
    let mut leader1_connect = group.get_connect(&leader1).await;
    leader1_connect.propose(req1).await.unwrap();
    sleep_millis(100).await;
    let leader1_store = Arc::clone(&group.get_node(&leader1).store);
    let res = leader1_store.get_all(TEST_TABLE).unwrap();
    assert_eq!(
        res,
        vec![(0u32.to_be_bytes().to_vec(), 1u32.to_be_bytes().to_vec())]
    );

    // 3: recover all others and disable leader, a new leader will be elected
    group.disable_node(&leader1);
    sleep_millis(100).await;
    for node in group.nodes.values().filter(|node| node.id != leader1) {
        group.enable_node(&node.id);
    }
    sleep_secs(3).await;
    let leader2 = group.get_leader().await.0;
    assert_ne!(leader2, leader1);

    // 4: recover the old leader, its state should be reverted to the original state
    group.enable_node(&leader1);
    sleep_secs(1).await;
    let res = leader1_store.get_all(TEST_TABLE).unwrap();
    assert_eq!(
        res,
        vec![(0u32.to_be_bytes().to_vec(), 0u32.to_be_bytes().to_vec())]
    );

    // 5: the client should also get the original state
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );

    group.stop();
}

#[tokio::test]
async fn all_crash_and_recovery() {
    init_logger();

    let mut group = CurpGroup::new(3).await;
    let client = group.new_client(ClientTimeout::default()).await;

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );

    let all = group.all.keys().cloned().collect_vec();
    for node in &all {
        group.crash(node);
    }
    sleep_secs(2).await;
    for node in &all {
        group.restart(node, node.as_str() == "S0").await;
    }

    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![0]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 1))
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        client.propose(TestCommand::new_get(vec![0])).await.unwrap(),
        vec![1]
    );

    group.stop();
}

#[tokio::test]
async fn recovery_after_compaction() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client(Default::default()).await;
    let (leader, _term) = group.get_leader().await;
    let node_id = group
        .nodes
        .keys()
        .find(|&n| n != &leader)
        .unwrap()
        .to_owned();
    group.crash(&node_id);

    // since the log entries cap is set to 10, 50 commands will trigger log compactions
    for i in 0..50 {
        assert!(client
            .propose(TestCommand::new_put(vec![i], i))
            .await
            .is_ok());
    }

    sleep_secs(1).await;

    debug!("start recovery");

    // the restarted node should use snapshot to recover
    group.restart(&node_id, false).await;

    sleep_secs(3).await;

    {
        let node = group.nodes.get_mut(&node_id).unwrap();
        for i in 0..50_u32 {
            let kv = i.to_be_bytes().to_vec();
            let val = node.store.get(TEST_TABLE, &kv).unwrap().unwrap();
            assert_eq!(val, kv);
        }
    }

    group.stop();
}
