//! Integration test for the curp server

use std::sync::Arc;

use curp_test_utils::{init_logger, sleep_secs, test_cmd::TestCommand, TEST_TABLE};
use engine::StorageEngine;
use itertools::Itertools;
use tracing::debug;
use utils::config::ClientTimeout;

use simulation::curp_group::{CurpGroup, ProposeRequest};

#[madsim::test]
async fn leader_crash_and_recovery() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;

    let leader = group.try_get_leader().await.unwrap().0;
    group.crash(&leader).await;

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap()
            .0,
        vec![]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );

    // restart the original leader
    group.restart(&leader, false).await;
    let old_leader = group.nodes.get_mut(&leader).unwrap();

    let (_cmd, er) = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let (_cmd, er) = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![0]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    group.stop().await;
}

#[madsim::test]
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
    group.crash(&follower).await;

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap()
            .0,
        vec![]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );

    // restart follower
    group.restart(&follower, false).await;
    let follower = group.nodes.get_mut(&follower).unwrap();

    let (_cmd, er) = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let (_cmd, er) = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![0]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    group.stop().await;
}

#[madsim::test]
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
    group.crash(&follower).await;

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap()
            .0,
        vec![]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );

    group.crash(&leader).await;

    // restart the original leader
    group.restart(&leader, false).await;
    // add a new log to commit previous logs
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );
    let old_leader = group.nodes.get_mut(&leader).unwrap();

    let (_cmd, er) = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let (_cmd, er) = old_leader.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![0]);
    let asr = old_leader.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    // restart follower
    group.restart(&follower, false).await;
    let follower = group.nodes.get_mut(&follower).unwrap();

    let (_cmd, er) = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 1);

    let (_cmd, er) = follower.exe_rx.recv().await.unwrap();
    assert_eq!(er.0, vec![0]);
    let asr = follower.as_rx.recv().await.unwrap();
    assert_eq!(asr.1, 2);

    group.stop().await;
}

#[madsim::test]
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

    // 2: disable leader1
    group.disable_node(&leader1);

    // 3: the client should automatically find the new leader and get the response
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
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

    group.stop().await;
}

#[madsim::test]
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
            .unwrap()
            .0,
        vec![]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );

    group.stop().await;
}

// Old Leader should discard spec states
#[madsim::test]
async fn old_leader_will_discard_spec_exe_cmds() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;

    // 0: let's first propose an initial cmd0
    let cmd0 = TestCommand::new_put(vec![0], 0);
    let (er, index) = client.propose_indexed(cmd0).await.unwrap();
    assert_eq!(er.0, vec![]);
    assert_eq!(index, 1);

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
    let leader1_store = Arc::clone(&group.get_node(&leader1).store);
    let res = leader1_store
        .lock()
        .as_ref()
        .unwrap()
        .get_all(TEST_TABLE)
        .unwrap();
    assert_eq!(
        res,
        vec![(0u32.to_be_bytes().to_vec(), 1u32.to_be_bytes().to_vec())]
    );

    // 3: recover all others and disable leader, a new leader will be elected
    group.disable_node(&leader1);
    for node in group.nodes.values().filter(|node| node.id != leader1) {
        group.enable_node(&node.id);
    }
    // wait for election
    sleep_secs(15).await;
    let leader2 = group.get_leader().await.0;
    assert_ne!(leader2, leader1);

    // 4: recover the old leader, its state should be reverted to the original state
    group.enable_node(&leader1);
    // wait for reversion
    sleep_secs(15).await;
    let res = leader1_store
        .lock()
        .as_ref()
        .unwrap()
        .get_all(TEST_TABLE)
        .unwrap();
    assert_eq!(
        res,
        vec![(0u32.to_be_bytes().to_vec(), 0u32.to_be_bytes().to_vec())]
    );

    // 5: the client should also get the original state
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );

    group.stop().await;
}

#[madsim::test]
async fn minority_crash_and_recovery() {
    init_logger();

    const NODES: usize = 9;
    const MINORITY: usize = (NODES - 1) / 2;

    let mut group = CurpGroup::new(NODES).await;

    let client = group.new_client(ClientTimeout::default()).await;

    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap()
            .0,
        vec![]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );

    let minority = group.all.keys().take(MINORITY).cloned().collect_vec();
    for node in &minority {
        group.crash(node).await;
    }
    for node in &minority {
        group.restart(node, node.as_str() == "S0").await;
    }

    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 1))
            .await
            .unwrap()
            .0,
        vec![]
    );
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![1]
    );

    group.stop().await;
}

#[madsim::test]
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
    group.crash(&node_id).await;

    // since the log entries cap is set to 10, 50 commands will trigger log compactions
    for i in 0..50 {
        assert!(client
            .propose(TestCommand::new_put(vec![i], i))
            .await
            .is_ok());
    }

    // wait for log compactions
    sleep_secs(15).await;

    debug!("start recovery");

    // the restarted node should use snapshot to recover
    group.restart(&node_id, false).await;

    // wait for node to restart
    sleep_secs(15).await;

    {
        let node = group.nodes.get_mut(&node_id).unwrap();
        for i in 0..50_u32 {
            let kv = i.to_be_bytes().to_vec();
            let val = node
                .store
                .lock()
                .as_ref()
                .unwrap()
                .get(TEST_TABLE, &kv)
                .unwrap()
                .unwrap();
            assert_eq!(val, kv);
        }
    }

    group.stop().await;
}
