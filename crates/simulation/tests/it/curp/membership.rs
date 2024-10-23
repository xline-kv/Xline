use std::{pin::Pin, time::Duration};

use curp::rpc::{Change, Node, NodeMetadata};
use curp_test_utils::{init_logger, test_cmd::TestCommand};
use futures::{Future, FutureExt};
use itertools::Itertools;
use madsim::rand::{self, seq::IteratorRandom, Rng};
use simulation::curp_group::{CurpGroup, SimClient};
use test_macros::abort_on_panic;

fn spawn_change_membership(
    client: SimClient<TestCommand>,
    change: Change,
) -> Pin<Box<dyn Future<Output = ()>>> {
    let handle = tokio::spawn(async move {
        while let Err(err) = client.change_membership(vec![change.clone()]).await {
            eprintln!("change membership error: {err}");
            if err.code() == tonic::Code::FailedPrecondition {
                break;
            }
        }
    });
    Box::pin(handle.map(|r| r.unwrap()))
}

async fn with_fault_injection<ChangeFut, Fault, Recovery>(
    change: ChangeFut,
    fault: Fault,
    recovery: Recovery,
) where
    ChangeFut: Future<Output = ()>,
    Fault: Future<Output = ()>,
    Recovery: Future<Output = ()>,
{
    // yield so that other task may run
    madsim::task::yield_now().await;
    eprintln!("injecting fault");
    fault.await;
    change.await;
    eprintln!("recovering");
    recovery.await;
}

async fn with_fault_injection_and_early_recovery<ChangeFut, Fault, Recovery>(
    change: ChangeFut,
    fault: Fault,
    recovery: Recovery,
) where
    ChangeFut: Future<Output = ()>,
    Fault: Future<Output = ()>,
    Recovery: Future<Output = ()>,
{
    // yield so that other task may run
    madsim::task::yield_now().await;
    eprintln!("injecting fault");
    fault.await;
    madsim::time::sleep(Duration::from_secs(10)).await;
    eprintln!("recovering");
    recovery.await;
    change.await;
}

async fn get_leader(group: &CurpGroup) -> u64 {
    group
        .new_client()
        .await
        .fetch_cluster()
        .await
        .unwrap()
        .leader_id
}

async fn assert_membership(group: &CurpGroup, id: u64, meta: NodeMetadata, is_voter: bool) {
    let new_membership = group.new_client().await.fetch_cluster().await.unwrap();
    assert!(new_membership
        .nodes
        .into_iter()
        .any(|n| n.node_id == id && n.meta.unwrap() == meta));

    assert_eq!(
        new_membership
            .members
            .into_iter()
            .any(|s| s.set.contains(&id)),
        is_voter
    );
}

async fn assert_non_exist(group: &CurpGroup, id: u64) {
    let new_membership = group.new_client().await.fetch_cluster().await.unwrap();
    assert!(!new_membership.nodes.into_iter().any(|n| n.node_id == id));
}

#[abort_on_panic]
#[madsim::test]
async fn membership_change_with_reelection() {
    init_logger();
    let mut group = CurpGroup::new(5).await;
    let meta = NodeMetadata::new(
        "new",
        vec!["192.168.1.6:2380".to_owned()],
        vec!["192.168.1.6:2379".to_owned()],
    );
    let node = Node::new(5, meta.clone());
    group.run_node(5);

    let leader0 = get_leader(&group).await;
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Add(node)),
        async { group.disable_node(leader0) },
        async { group.enable_node(leader0) },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), false).await;

    let leader1 = get_leader(&group).await;
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Promote(5)),
        async { group.disable_node(leader1) },
        async { group.enable_node(leader1) },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), true).await;

    let leader2 = get_leader(&group).await;
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Demote(5)),
        async { group.disable_node(leader2) },
        async { group.enable_node(leader2) },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), false).await;

    let leader3 = get_leader(&group).await;
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Remove(5)),
        async { group.disable_node(leader3) },
        async { group.enable_node(leader3) },
    )
    .await;
    assert_non_exist(&group, 5).await;
}

#[madsim::test]
async fn membership_change_with_partition_minority() {
    init_logger();
    let mut group = CurpGroup::new(5).await;
    let meta = NodeMetadata::new(
        "new",
        vec!["192.168.1.6:2380".to_owned()],
        vec!["192.168.1.6:2379".to_owned()],
    );
    let node = Node::new(5, meta.clone());
    group.run_node(5);

    let ids = [0, 1, 2, 3, 4];
    let mut rng = rand::thread_rng();
    let mut get_minority = || ids.iter().combinations(2).choose(&mut rng).unwrap();

    let minority = get_minority();
    eprintln!("disabling minority: {minority:?}");
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Add(node.clone())),
        async { minority.iter().for_each(|id| group.disable_node(**id)) },
        async { minority.iter().for_each(|id| group.enable_node(**id)) },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), false).await;

    let minority = get_minority();
    eprintln!("disabling minority: {minority:?}");
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Promote(5)),
        async { minority.iter().for_each(|id| group.disable_node(**id)) },
        async { minority.iter().for_each(|id| group.enable_node(**id)) },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), true).await;
}

#[madsim::test]
async fn membership_change_with_partition_majority() {
    init_logger();
    let mut group = CurpGroup::new(5).await;
    let meta = NodeMetadata::new(
        "new",
        vec!["192.168.1.6:2380".to_owned()],
        vec!["192.168.1.6:2379".to_owned()],
    );
    let node = Node::new(5, meta.clone());
    group.run_node(5);
    let ids = [0, 1, 2, 3, 4];
    let mut rng = rand::thread_rng();
    let mut get_majority = || {
        ids.iter()
            .combinations(rng.gen_range(3..=5))
            .choose(&mut rng)
            .unwrap()
    };

    let majority = get_majority();
    eprintln!("disabling majority: {majority:?}");
    with_fault_injection_and_early_recovery(
        spawn_change_membership(group.new_client().await, Change::Add(node.clone())),
        async { majority.iter().for_each(|id| group.disable_node(**id)) },
        async { majority.iter().for_each(|id| group.enable_node(**id)) },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), false).await;

    let majority = get_majority();
    eprintln!("disabling majority: {majority:?}");
    with_fault_injection_and_early_recovery(
        spawn_change_membership(group.new_client().await, Change::Promote(5)),
        async { majority.iter().for_each(|id| group.disable_node(**id)) },
        async { majority.iter().for_each(|id| group.enable_node(**id)) },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), true).await;
}

#[madsim::test]
async fn membership_change_with_crash_leader() {
    init_logger();
    let mut group = CurpGroup::new(5).await;
    let meta = NodeMetadata::new(
        "new",
        vec!["192.168.1.6:2380".to_owned()],
        vec!["192.168.1.6:2379".to_owned()],
    );
    let node = Node::new(5, meta.clone());
    group.run_node(5);

    let leader = get_leader(&group).await;
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Add(node.clone())),
        group.crash(leader),
        group.restart(leader),
    )
    .await;
    assert_membership(&group, 5, meta.clone(), false).await;

    let leader = get_leader(&group).await;
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Promote(5)),
        group.crash(leader),
        group.restart(leader),
    )
    .await;
    assert_membership(&group, 5, meta.clone(), true).await;
}

#[madsim::test]
async fn membership_change_with_crash_minority() {
    init_logger();
    let mut group = CurpGroup::new(5).await;
    let meta = NodeMetadata::new(
        "new",
        vec!["192.168.1.6:2380".to_owned()],
        vec!["192.168.1.6:2379".to_owned()],
    );
    let node = Node::new(5, meta.clone());
    group.run_node(5);
    let ids = [0, 1, 2, 3, 4];
    let mut rng = rand::thread_rng();
    let mut get_minority = || ids.iter().combinations(2).choose(&mut rng).unwrap();

    let minority = get_minority();
    eprintln!("disabling minority: {minority:?}");
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Add(node.clone())),
        async {
            for id in &minority {
                group.crash(**id).await;
            }
        },
        async {
            for id in &minority {
                group.restart(**id).await;
            }
        },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), false).await;

    let minority = get_minority();
    eprintln!("disabling minority: {minority:?}");
    with_fault_injection(
        spawn_change_membership(group.new_client().await, Change::Promote(5)),
        async {
            for id in &minority {
                group.crash(**id).await;
            }
        },
        async {
            for id in &minority {
                group.restart(**id).await;
            }
        },
    )
    .await;
    assert_membership(&group, 5, meta.clone(), true).await;
}
