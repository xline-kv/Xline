//! Integration test for the curp server

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use clippy_utilities::NumericCast;
use curp::{
    client::{ClientApi, ClientBuilder},
    member::MembershipInfo,
    rpc::{Change, CurpError, MembershipResponse, Node, NodeMetadata},
};
use curp_test_utils::{
    init_logger, sleep_millis,
    test_cmd::{TestCommand, TestCommandResult, TestCommandType},
};
use futures::stream::FuturesUnordered;
use madsim::rand::{thread_rng, Rng};
use test_macros::abort_on_panic;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tracing_test::traced_test;
use utils::config::ClientConfig;

use crate::common::curp_group::{
    commandpb::FetchMembershipRequest, CurpGroup, DEFAULT_SHUTDOWN_TIMEOUT,
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
    let cmd = TestCommand::new_put(vec![0], 0);

    let (er, index) = client.propose(&cmd, None, false).await.unwrap().unwrap();
    assert_eq!(er, TestCommandResult::new(vec![], vec![]));
    assert_eq!(index.unwrap(), 1.into()); // log[0] is a fake one

    {
        let mut exe_futs = group
            .exe_rxs()
            .map(|rx| rx.recv())
            .collect::<FuturesUnordered<_>>();
        let (cmd1, er) = exe_futs.next().await.unwrap().unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(er, TestCommandResult::new(vec![], vec![]));
    }

    for as_rx in group.as_rxs() {
        let (cmd1, index) = as_rx.recv().await.unwrap();
        assert_eq!(cmd1, cmd);
        assert_eq!(index, 1);
    }
}

// Each command should be executed once and only once on leader
#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn exe_exactly_once_on_leader() {
    init_logger();

    let mut group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    let cmd = TestCommand::new_put(vec![0], 0);

    let er = client.propose(&cmd, None, true).await.unwrap().unwrap().0;
    assert_eq!(er, TestCommandResult::new(vec![], vec![]));

    let leader = group.get_leader().await.0;
    {
        let exec_rx = &mut group.get_node_mut(&leader).exe_rx;
        let (cmd1, er) = exec_rx.recv().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exec_rx.recv())
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

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
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
                client_id: TEST_CLIENT_ID,
                seq_num: 0,
            }),
            command: bincode::serialize(&cmd).unwrap(),
            cluster_version: 0,
            term: 0,
            first_incomplete: 0,
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
                client_id: TEST_CLIENT_ID,
                seq_num: 0,
            }),
            command: bincode::serialize(&cmd).unwrap(),
            cluster_version: 0,
            term: 0,
            first_incomplete: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.result.is_none());
}

// TODO: rewrite this test for propose_stream
#[cfg(ignore)]
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
                client_id: TEST_CLIENT_ID,
                seq_num: 0,
            }),
            command: bincode::serialize(&cmd0).unwrap(),
            cluster_version: 0,
            term: 0,
            first_incomplete: 0,
        })
        .await
        .expect("propose failed");
    });

    sleep_millis(20).await;
    let response = leader_connect
        .propose(ProposeRequest {
            propose_id: Some(ProposeId {
                client_id: TEST_CLIENT_ID,
                seq_num: 1,
            }),
            command: bincode::serialize(&cmd1).unwrap(),
            cluster_version: 0,
            term: 0,
            first_incomplete: 0,
        })
        .await;
    assert!(response.is_err());
    let response = leader_connect
        .propose(ProposeRequest {
            propose_id: Some(ProposeId {
                client_id: TEST_CLIENT_ID,
                seq_num: 2,
            }),
            command: bincode::serialize(&cmd2).unwrap(),
            cluster_version: 0,
            term: 0,
            first_incomplete: 0,
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
        let rand_dur = Duration::from_millis(thread_rng().gen_range(0..50).numeric_cast());
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
    group
        .wait_for_group_shutdown(DEFAULT_SHUTDOWN_TIMEOUT)
        .await;

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

struct NodeAssert {
    id: u64,
    meta: NodeMetadata,
    is_member: bool,
}

impl NodeAssert {
    fn new(id: u64, meta: NodeMetadata, is_member: bool) -> Self {
        Self {
            id,
            meta,
            is_member,
        }
    }
}

async fn assert_cluster<NS>(
    client: &impl ClientApi<Error = tonic::Status, Cmd = TestCommand>,
    num_nodes: usize,
    num_members: usize,
    node_asserts: NS,
) where
    NS: IntoIterator<Item = NodeAssert>,
{
    let resp = loop {
        // workaround for client id expires on new leader
        if let Ok(resp) = client.fetch_cluster(true).await {
            break resp;
        }
    };
    let member_ids: BTreeSet<_> = resp.members.into_iter().flat_map(|t| t.set).collect();
    assert_eq!(resp.nodes.len(), num_nodes);
    assert_eq!(member_ids.len(), num_members);
    for node_assert in node_asserts {
        let node = resp
            .nodes
            .iter()
            .find(|n| n.node_id == node_assert.id)
            .expect("node not found in fetch cluster response");
        assert_eq!(node.meta, Some(node_assert.meta), "node meta not match");
        assert_eq!(
            node_assert.is_member,
            member_ids.iter().any(|i| *i == node_assert.id)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_add_node_should_success() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let node_meta = NodeMetadata::new("new_node", ["addr"], ["addr"]);
    let node_id = 5;
    let node = Node::new(node_id, node_meta.clone());
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 3, [NodeAssert::new(node_id, node_meta, false)]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_remove_node_should_success() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let node_meta = NodeMetadata::new("new_node", ["addr"], ["addr"]);
    let node_id = 5;
    let node = Node::new(node_id, node_meta.clone());
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 3, [NodeAssert::new(node_id, node_meta, false)]).await;

    client
        .change_membership(vec![Change::Remove(node_id)])
        .await
        .unwrap();
    assert_cluster(&client, 3, 3, []).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_add_member_should_success() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let node_meta = NodeMetadata::new("new_node", ["addr"], ["addr"]);
    let node_id = 5;
    let node = Node::new(node_id, node_meta.clone());
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 3, [NodeAssert::new(node_id, node_meta, false)]).await;

    client
        .change_membership(vec![Change::Promote(node_id)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 4, []).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_remove_member_should_success() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let node_meta = NodeMetadata::new("new_node", ["addr"], ["addr"]);
    let node_id = 5;
    let node = Node::new(node_id, node_meta.clone());
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 3, [NodeAssert::new(node_id, node_meta, false)]).await;

    client
        .change_membership(vec![Change::Promote(node_id)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 4, []).await;

    client
        .change_membership(vec![Change::Demote(node_id)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 3, []).await;

    client
        .change_membership(vec![Change::Remove(node_id)])
        .await
        .unwrap();
    assert_cluster(&client, 3, 3, []).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_remove_leader_should_success() {
    init_logger();

    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let id = client.fetch_leader_id(true).await.unwrap();

    client
        .change_membership(vec![Change::Demote(id)])
        .await
        .unwrap();
    assert_cluster(&client, 3, 2, []).await;

    while {
        client
            .change_membership(vec![Change::Remove(id)])
            .await
            .is_err()
    } {}
    assert_cluster(&client, 2, 2, []).await;

    let new_id = client.fetch_leader_id(true).await.unwrap();
    assert_ne!(id, new_id);
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
        .init_cluster(follower_id, 0, group.all_addrs_map())
        .build::<TestCommand>()
        .unwrap();

    client.propose_shutdown().await.unwrap();

    group
        .wait_for_group_shutdown(DEFAULT_SHUTDOWN_TIMEOUT)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn propose_conf_change_to_follower() {
    init_logger();
    let group = CurpGroup::new(3).await;

    let leader_id = group.get_leader().await.0;
    let follower_id = *group.nodes.keys().find(|&id| &leader_id != id).unwrap();
    // build a client and set a wrong leader id
    let client = ClientBuilder::new(ClientConfig::default(), true)
        .init_cluster(follower_id, 0, group.all_addrs_map())
        .build::<TestCommand>()
        .unwrap();

    let node_meta = NodeMetadata::new("new_node", ["addr"], ["addr"]);
    let node_id = 5;
    let node = Node::new(node_id, node_meta.clone());
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();
    assert_cluster(&client, 4, 3, [NodeAssert::new(node_id, node_meta, false)]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn new_node_should_apply_old_cluster_logs() {
    init_logger();

    let mut group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    let req = TestCommand::new_put(vec![123], 123);
    let _res = client.propose(&req, None, true).await.unwrap().unwrap();

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let addrs = vec![addr.clone()];
    let node_meta = NodeMetadata::new("new_node", addrs.clone(), addrs);
    let node_id = 5;
    let node = Node::new(node_id, node_meta.clone());
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();

    /*******  start new node *******/

    // 1. start new node
    group
        .run_node(
            listener,
            "new_node".to_owned(),
            MembershipInfo::new(node_id, BTreeMap::default()),
        )
        .await;

    sleep_millis(500).await; // wait for membership sync

    // 2. fetch and check cluster from new node
    let mut new_connect = group.get_connect(&node_id).await;
    let res = new_connect
        .fetch_membership(FetchMembershipRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(res.nodes.len(), 4);
    assert!(res
        .nodes
        .iter()
        .any(|m| m.node_id == node_id && m.meta.as_ref().unwrap().name == "new_node"));
    assert!(!res
        .members
        .iter()
        .flat_map(|s| &s.set)
        .any(|m| *m == node_id));

    // 3. check if the new node syncs the command from old cluster
    let new_node = group.nodes.get_mut(&node_id).unwrap();
    let (cmd, _) = new_node.as_rx.recv().await.unwrap();
    assert_eq!(
        cmd,
        TestCommand {
            keys: vec![123],
            cmd_type: TestCommandType::Put(123),
            ..Default::default()
        }
    );

    // 4. check if the old client can propose to the new cluster
    client
        .propose(&TestCommand::new_get(vec![1]), None, true)
        .await
        .unwrap()
        .unwrap();
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

    let node_meta = NodeMetadata::new("new_node", addrs.clone(), addrs);
    let node_id = 5;
    let node = Node::new(node_id, node_meta);
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();
    group
        .run_node(
            listener,
            "new_node".to_owned(),
            MembershipInfo::new(node_id, BTreeMap::default()),
        )
        .await;

    client.propose_shutdown().await.unwrap();

    group
        .wait_for_group_shutdown(DEFAULT_SHUTDOWN_TIMEOUT)
        .await;
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
    let node_meta = NodeMetadata::new("new_node", addrs.clone(), addrs);
    let node_id = 5;
    let node = Node::new(node_id, node_meta);
    client
        .change_membership(vec![Change::Add(node)])
        .await
        .unwrap();
    group
        .run_node(
            listener,
            "new_node".to_owned(),
            MembershipInfo::new(node_id, BTreeMap::default()),
        )
        .await;
    client
        .change_membership(vec![Change::Remove(node_id)])
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn move_leader_should_move_leadership_to_target_node() {
    init_logger();
    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;

    let old_leader = group.get_leader().await.0;
    let target = *group.nodes.keys().find(|&id| &old_leader != id).unwrap();

    // TODO: investigate why moving the leader invalidates the client id
    while client.move_leader(target).await.is_err() {}
    let new_leader = group.get_leader().await.0;

    assert_eq!(target, new_leader);
    assert_ne!(old_leader, new_leader);
}

// A full single step change cycle
#[traced_test]
#[tokio::test(flavor = "multi_thread")]
async fn membership_change_ok_case0() {
    let group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    assert_eq!(client.fetch_cluster(true).await.unwrap().leader_id, 0);

    let change = Change::Add(Node::new(3, NodeMetadata::default()));
    client.change_membership(vec![change]).await.unwrap();
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [0, 1, 2], [0, 1, 2, 3]);

    let change = Change::Promote(3);
    client.change_membership(vec![change]).await.unwrap();
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [0, 1, 2, 3], [0, 1, 2, 3]);

    let change = Change::Demote(2);
    client.change_membership(vec![change]).await.unwrap();
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [0, 1, 3], [0, 1, 2, 3]);

    let change = Change::Remove(2);
    client.change_membership(vec![change]).await.unwrap();
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [0, 1, 3], [0, 1, 3]);
}

// Mixed membership change
#[tokio::test(flavor = "multi_thread")]
async fn membership_change_ok_case1() {
    init_logger();
    let mut group = CurpGroup::new(3).await;
    let client = group.new_client().await;
    assert_eq!(client.fetch_cluster(true).await.unwrap().leader_id, 0);

    let listen3 = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr3 = listen3.local_addr().unwrap().to_string();
    let listen4 = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr4 = listen4.local_addr().unwrap().to_string();

    let change0 = Change::Add(Node::new(3, NodeMetadata::new("node3", [&addr3], [&addr3])));
    let change1 = Change::Add(Node::new(4, NodeMetadata::new("node4", [&addr4], [&addr4])));
    client
        .change_membership(vec![change0, change1])
        .await
        .unwrap();
    group
        .run_node(
            listen3,
            "node3".to_owned(),
            MembershipInfo::new(3, BTreeMap::default()),
        )
        .await;
    group
        .run_node(
            listen4,
            "node4".to_owned(),
            MembershipInfo::new(3, BTreeMap::default()),
        )
        .await;
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [0, 1, 2], [0, 1, 2, 3, 4]);

    let change0 = Change::Promote(3);
    let change1 = Change::Demote(2);
    client
        .change_membership(vec![change0, change1])
        .await
        .unwrap();
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [0, 1, 3], [0, 1, 2, 3, 4]);

    let change0 = Change::Promote(4);
    let change1 = Change::Remove(2);
    client
        .change_membership(vec![change0, change1])
        .await
        .unwrap();
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [0, 1, 3, 4], [0, 1, 3, 4]);
}

// Remove the leader
#[tokio::test(flavor = "multi_thread")]
async fn membership_change_ok_case2() {
    init_logger();
    let group = CurpGroup::new(5).await;
    let client = group.new_client().await;
    assert_eq!(client.fetch_cluster(true).await.unwrap().leader_id, 0);

    let change = Change::Demote(0);
    client.change_membership(vec![change]).await.unwrap();
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [1, 2, 3, 4], [0, 1, 2, 3, 4]);

    let change = Change::Remove(0);
    // workaround for clinet id expiry
    while client
        .change_membership(vec![change.clone()])
        .await
        .is_err()
    {}
    let resp = client.fetch_cluster(true).await.unwrap();
    assert_membership_response(resp, [1, 2, 3, 4], [1, 2, 3, 4]);
}

fn assert_membership_response(
    resp: MembershipResponse,
    expect_member_ids: impl IntoIterator<Item = u64>,
    expect_node_ids: impl IntoIterator<Item = u64>,
) {
    println!("leader: {}", resp.leader_id);
    let member_ids: BTreeSet<_> = resp.members.into_iter().flat_map(|s| s.set).collect();
    let expect_member_ids: BTreeSet<_> = expect_member_ids.into_iter().collect();
    assert_eq!(member_ids, expect_member_ids);

    let node_ids: BTreeSet<_> = resp.nodes.into_iter().map(|n| n.node_id).collect();
    let expect_node_ids: BTreeSet<_> = expect_node_ids.into_iter().collect();
    assert_eq!(node_ids, expect_node_ids);
}
