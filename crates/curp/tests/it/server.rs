//! Integration test for the curp server

use std::{sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use curp::{
    client::{ClientApi, ClientBuilder},
    members::ClusterInfo,
    rpc::{ConfChange, CurpError},
};
use curp_test_utils::{
    init_logger, sleep_millis, sleep_secs,
    test_cmd::{TestCommand, TestCommandResult, TestCommandType},
};
use madsim::rand::{thread_rng, Rng};
use test_macros::abort_on_panic;
use tokio::net::TcpListener;
use utils::{config::ClientConfig, timestamp};

use crate::common::curp_group::{
    commandpb::ProposeId, CurpGroup, FetchClusterRequest, ProposeRequest, ProposeResponse,
};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn basic_propose() {
    init_logger();

    let group = CurpGroup::new(3).await;

    let client = group.new_client().await;

    assert_eq!(
        client
            .propose(&TestCommand::new_put(vec![0], 0), None, true)
            .await
            .unwrap()
            .unwrap()
            .0,
        TestCommandResult::new(vec![], vec![])
    );
    assert_eq!(
        client
            .propose(&TestCommand::new_get(vec![0]), None, true)
            .await
            .unwrap()
            .unwrap()
            .0,
        TestCommandResult::new(vec![0], vec![1])
    );
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn synced_propose() {
    init_logger();

    let mut group = CurpGroup::new(5).await;
    let client = group.new_client().await;
    let cmd = TestCommand::new_get(vec![0]);

    let (er, index) = client.propose(&cmd, None, false).await.unwrap().unwrap();
    assert_eq!(er, TestCommandResult::new(vec![], vec![]));
    assert_eq!(index.unwrap(), 1.into()); // log[0] is a fake one

    for exe_rx in group.exe_rxs() {
        let (cmd1, er) = exe_rx.recv().await.unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(er, TestCommandResult::new(vec![], vec![]));
    }
    for as_rx in group.as_rxs() {
        let (cmd1, index) = as_rx.recv().await.unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(index, 1);
    }
}

// Each command should be executed once and only once on each node
#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn exe_exact_n_times() {
    init_logger();

    let mut group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    let cmd = TestCommand::new_get(vec![0]);

    let er = client.propose(&cmd, None, true).await.unwrap().unwrap().0;
    assert_eq!(er, TestCommandResult::new(vec![], vec![]));

    for exe_rx in group.exe_rxs() {
        let (cmd1, er) = exe_rx.recv().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exe_rx.recv())
                .await
                .is_err()
        );
        assert_eq!(cmd1, cmd);
        assert_eq!(er.values, Vec::<u32>::new());
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
#[tokio::test(flavor = "multi_thread")]
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
            propose_id: Some(ProposeId {
                client_id: 0,
                seq_num: 0,
            }),
            command: bincode::serialize(&cmd).unwrap(),
            cluster_version: 0,
        }))
        .await
        .unwrap();

    // wait for the command to be synced to others
    // because followers never get the cmd from the client, it will mark the cmd done in spec pool instead of removing the cmd from it
    tokio::time::sleep(Duration::from_secs(1)).await;

    // send propose to follower
    let follower_id = group.nodes.keys().find(|&id| &leader != id).unwrap();
    let mut follower_connect = group.get_connect(follower_id).await;

    // the follower should response empty immediately
    let resp: ProposeResponse = follower_connect
        .propose(tonic::Request::new(ProposeRequest {
            propose_id: Some(ProposeId {
                client_id: 0,
                seq_num: 0,
            }),
            command: bincode::serialize(&cmd).unwrap(),
            cluster_version: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.result.is_none());
}

#[tokio::test(flavor = "multi_thread")]
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
            propose_id: Some(ProposeId {
                client_id: 0,
                seq_num: 0,
            }),
            command: bincode::serialize(&cmd0).unwrap(),
            cluster_version: 0,
        })
        .await
        .expect("propose failed");
    });

    sleep_millis(20).await;
    let response = leader_connect
        .propose(ProposeRequest {
            propose_id: Some(ProposeId {
                client_id: 0,
                seq_num: 1,
            }),
            command: bincode::serialize(&cmd1).unwrap(),
            cluster_version: 0,
        })
        .await;
    assert!(response.is_err());
    let response = leader_connect
        .propose(ProposeRequest {
            propose_id: Some(ProposeId {
                client_id: 0,
                seq_num: 2,
            }),
            command: bincode::serialize(&cmd2).unwrap(),
            cluster_version: 0,
        })
        .await;
    assert!(response.is_err());

    sleep_secs(1).await;

    let client = group.new_client().await;

    assert_eq!(
        client
            .propose(&TestCommand::new_get(vec![1]), None, true)
            .await
            .unwrap()
            .unwrap()
            .0
            .values,
        vec![2]
    );
}

/// This test case ensures that the issue 228 is fixed.
#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn concurrent_cmd_order_should_have_correct_revision() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let sample_range = 1..=100;

    for i in sample_range.clone() {
        let rand_dur = Duration::from_millis(thread_rng().gen_range(0..500).numeric_cast());
        let _er = client
            .propose(
                &TestCommand::new_put(vec![i], i).set_as_dur(rand_dur),
                None,
                true,
            )
            .await
            .unwrap()
            .unwrap();
    }

    for i in sample_range {
        assert_eq!(
            client
                .propose(&TestCommand::new_get(vec![i]), None, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .revisions,
            vec![i.numeric_cast::<i64>()]
        )
    }
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn shutdown_rpc_should_shutdown_the_cluster() {
    init_logger();
    let tmp_path = tempfile::TempDir::new().unwrap().into_path();
    let group = CurpGroup::new_rocks(3, tmp_path.clone()).await;

    let req_client = group.new_client().await;
    let collection_task = tokio::spawn(async move {
        let mut collection = vec![];
        for i in 0..10 {
            let cmd = TestCommand::new_put(vec![i], i);
            let res = req_client.propose(&cmd, None, true).await;
            if res.is_ok() && res.unwrap().is_ok() {
                collection.push(i);
            }
        }
        collection
    });

    let client = group.new_client().await;
    client.propose_shutdown().await.unwrap();

    let res = client
        .propose(&TestCommand::new_put(vec![888], 1), None, false)
        .await;
    assert!(matches!(
        CurpError::from(res.unwrap_err()),
        CurpError::ShuttingDown(_)
    ));

    let collection = collection_task.await.unwrap();
    sleep_secs(7).await; // wait for the cluster to shutdown
    assert!(group.is_finished());

    let group = CurpGroup::new_rocks(3, tmp_path).await;
    let client = group.new_client().await;
    for i in collection {
        let res = client
            .propose(&TestCommand::new_get(vec![i]), None, true)
            .await
            .unwrap();
        assert_eq!(res.unwrap().0.values, vec![i]);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_add_node_should_success() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let node_id =
        ClusterInfo::calculate_member_id(vec!["address".to_owned()], "", Some(timestamp()));
    let changes = vec![ConfChange::add(node_id, vec!["address".to_string()])];
    let res = client.propose_conf_change(changes).await;
    let members = res.unwrap();
    assert_eq!(members.len(), 4);
    assert!(members.iter().any(|m| m.id == node_id));
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_remove_follower_should_success() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client().await;

    let leader_id = group.get_leader().await.0;
    let follower_id = *group.nodes.keys().find(|&id| &leader_id != id).unwrap();
    let changes = vec![ConfChange::remove(follower_id)];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 4);
    assert!(members.iter().all(|m| m.id != follower_id));
    sleep_secs(7).await; // wait the removed node start election and detect it is removed
    assert!(group
        .nodes
        .get(&follower_id)
        .unwrap()
        .task_manager
        .is_finished());
    // check if the old client can propose to the new cluster
    client
        .propose(&TestCommand::new_get(vec![1]), None, true)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_remove_leader_should_success() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client().await;
    let leader_id = group.get_leader().await.0;
    let changes = vec![ConfChange::remove(leader_id)];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 4);
    assert!(members.iter().all(|m| m.id != leader_id));
    sleep_secs(7).await; // wait for the new leader to be elected
    assert!(group
        .nodes
        .get(&leader_id)
        .unwrap()
        .task_manager
        .is_finished());
    let new_leader_id = group.get_leader().await.0;
    assert_ne!(new_leader_id, leader_id);
    // check if the old client can propose to the new cluster
    client
        .propose(&TestCommand::new_get(vec![1]), None, true)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_update_node_should_success() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client().await;
    let node_id = group.nodes.keys().next().copied().unwrap();
    let changes = vec![ConfChange::update(node_id, vec!["new_addr".to_owned()])];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 5);
    let member = members.iter().find(|m| m.id == node_id);
    assert!(member.is_some_and(|m| m.peer_urls == ["new_addr"]));
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn shutdown_rpc_should_shutdown_the_cluster_when_client_has_wrong_leader() {
    init_logger();
    let tmp_path = tempfile::TempDir::new().unwrap().into_path();
    let group = CurpGroup::new_rocks(3, tmp_path.clone()).await;

    let leader_id = group.get_leader().await.0;
    let follower_id = *group.nodes.keys().find(|&id| &leader_id != id).unwrap();
    // build a client and set a wrong leader id
    let client = ClientBuilder::new(ClientConfig::default(), true)
        .leader_state(follower_id, 0)
        .all_members(group.all_addrs_map())
        .build::<TestCommand>()
        .await
        .unwrap();
    client.propose_shutdown().await.unwrap();

    sleep_secs(7).await; // wait for the cluster to shutdown
    assert!(group.is_finished());
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_conf_change_to_follower() {
    init_logger();
    let group = CurpGroup::new(5).await;

    let leader_id = group.get_leader().await.0;
    let follower_id = *group.nodes.keys().find(|&id| &leader_id != id).unwrap();
    // build a client and set a wrong leader id
    let client = ClientBuilder::new(ClientConfig::default(), true)
        .leader_state(follower_id, 0)
        .all_members(group.all_addrs_map())
        .build::<TestCommand>()
        .await
        .unwrap();

    let node_id = group.nodes.keys().next().copied().unwrap();
    let changes = vec![ConfChange::update(node_id, vec!["new_addr".to_owned()])];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 5);
    let member = members.iter().find(|m| m.id == node_id);
    assert!(member.is_some_and(|m| m.peer_urls == ["new_addr"]));
}

async fn check_new_node(is_learner: bool) {
    init_logger();

    let mut group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    let req = TestCommand::new_put(vec![123], 123);
    let _res = client.propose(&req, None, true).await.unwrap().unwrap();

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let addrs = vec![addr.clone()];
    let node_id = ClusterInfo::calculate_member_id(addrs.clone(), "", Some(123));
    let changes = if is_learner {
        vec![ConfChange::add_learner(node_id, addrs.clone())]
    } else {
        vec![ConfChange::add(node_id, addrs.clone())]
    };

    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 4);
    assert!(members.iter().any(|m| m.id == node_id));

    /*******  start new node *******/

    // 1. fetch cluster from other nodes
    let cluster_info = Arc::new(group.fetch_cluster_info(&[addr], "new_node").await);

    // 2. start new node
    group
        .run_node(listener, "new_node".to_owned(), cluster_info)
        .await;
    sleep_millis(500).await; // wait new node publish it's name to cluster

    // 3. fetch and check cluster from new node
    let mut new_connect = group.get_connect(&node_id).await;
    let res = new_connect
        .fetch_cluster(tonic::Request::new(FetchClusterRequest {
            linearizable: false,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(res.members.len(), 4);
    assert!(res
        .members
        .iter()
        .any(|m| m.id == node_id && m.name == "new_node" && is_learner == m.is_learner));

    // 4. check if the new node executes the command from old cluster
    let new_node = group.nodes.get_mut(&node_id).unwrap();
    let (cmd, res) = new_node.exe_rx.recv().await.unwrap();
    assert_eq!(
        cmd,
        TestCommand {
            keys: vec![123],
            cmd_type: TestCommandType::Put(123),
            ..Default::default()
        }
    );
    assert!(res.values.is_empty());

    // 5. check if the old client can propose to the new cluster
    client
        .propose(&TestCommand::new_get(vec![1]), None, true)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn new_follower_node_should_apply_old_cluster_logs() {
    check_new_node(false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn new_learner_node_should_apply_old_cluster_logs() {
    check_new_node(true).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn shutdown_rpc_should_shutdown_the_cluster_when_client_has_wrong_cluster() {
    init_logger();
    let tmp_path = tempfile::TempDir::new().unwrap().into_path();
    let mut group = CurpGroup::new_rocks(3, tmp_path.clone()).await;
    let client = group.new_client().await;

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addrs = vec![listener.local_addr().unwrap().to_string()];
    let node_id = ClusterInfo::calculate_member_id(addrs.clone(), "", Some(123));
    let changes = vec![ConfChange::add(node_id, addrs.clone())];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 4);
    assert!(members.iter().any(|m| m.id == node_id));

    let cluster_info = Arc::new(group.fetch_cluster_info(&addrs, "new_node").await);
    group
        .run_node(listener, "new_node".to_owned(), cluster_info)
        .await;
    client.propose_shutdown().await.unwrap();

    sleep_secs(7).await; // wait for the cluster to shutdown
    assert!(group.is_finished());
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_conf_change_rpc_should_work_when_client_has_wrong_cluster() {
    init_logger();
    let tmp_path = tempfile::TempDir::new().unwrap().into_path();
    let mut group = CurpGroup::new_rocks(3, tmp_path.clone()).await;
    let client = group.new_client().await;

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addrs = vec![listener.local_addr().unwrap().to_string()];
    let node_id = ClusterInfo::calculate_member_id(addrs.clone(), "", Some(123));
    let changes = vec![ConfChange::add(node_id, addrs.clone())];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 4);
    assert!(members.iter().any(|m| m.id == node_id));
    let cluster_info = Arc::new(group.fetch_cluster_info(&addrs, "new_node").await);
    group
        .run_node(listener, "new_node".to_owned(), cluster_info)
        .await;
    let changes = vec![ConfChange::remove(node_id)];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 3);
    assert!(members.iter().all(|m| m.id != node_id));
    sleep_secs(7).await;
    assert!(group
        .nodes
        .get(&node_id)
        .unwrap()
        .task_manager
        .is_finished());
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn fetch_read_state_rpc_should_work_when_client_has_wrong_cluster() {
    init_logger();
    let tmp_path = tempfile::TempDir::new().unwrap().into_path();
    let mut group = CurpGroup::new_rocks(3, tmp_path.clone()).await;
    let client = group.new_client().await;

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addrs = vec![listener.local_addr().unwrap().to_string()];
    let node_id = ClusterInfo::calculate_member_id(addrs.clone(), "", Some(123));
    let changes = vec![ConfChange::add(node_id, addrs.clone())];
    let members = client.propose_conf_change(changes).await.unwrap();
    assert_eq!(members.len(), 4);
    assert!(members.iter().any(|m| m.id == node_id));
    let cluster_info = Arc::new(group.fetch_cluster_info(&addrs, "new_node").await);
    group
        .run_node(listener, "new_node".to_owned(), cluster_info)
        .await;

    let cmd = TestCommand::new_get(vec![0]);
    let res = client.fetch_read_state(&cmd).await;
    assert!(res.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn move_leader_should_move_leadership_to_target_node() {
    init_logger();
    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let old_leader = group.get_leader().await.0;
    let target = *group.nodes.keys().find(|&id| &old_leader != id).unwrap();

    client.move_leader(target).await.unwrap();
    let new_leader = group.get_leader().await.0;

    assert_eq!(target, new_leader);
    assert_ne!(old_leader, new_leader);
}
