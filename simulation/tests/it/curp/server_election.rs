use curp_test_utils::{init_logger, sleep_secs, test_cmd::TestCommand};
use simulation::curp_group::CurpGroup;
use utils::config::ClientTimeout;

/// Wait some time for the election to finish, and get the leader to ensure that the election is
/// completed.
async fn wait_for_election(group: &CurpGroup) -> (String, u64) {
    sleep_secs(15).await;
    group.get_leader().await
}

fn check_leader_state(group: &CurpGroup, leader: &String) {
    let node = group.nodes.get(leader).unwrap();
    assert!(node.role_change_arc.get_is_leader());
}

fn check_non_leader_state(group: &CurpGroup, group_size: usize, leader: &str) {
    let majority: usize = group_size / 2 + 1;
    let non_leader_node_cnt = group
        .nodes
        .iter()
        .filter(|(id, node)| id.as_str() != leader && !node.role_change_arc.get_is_leader())
        .count();
    assert!(
        non_leader_node_cnt >= majority - 1,
        "non_leader_node_cnt = {non_leader_node_cnt}, majority = {majority}"
    );
}

fn check_role_state(group: &CurpGroup, group_size: usize, leader: &String) {
    check_leader_state(group, leader);
    check_non_leader_state(group, group_size, leader);
}

// Election
#[madsim::test]
async fn election() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let leader0 = group.get_leader().await.0;
    check_role_state(&group, 5, &leader0);
    group.disable_node(&leader0);

    // check whether there is exact one leader in the group
    let (leader1, _term) = wait_for_election(&group).await;
    let term1 = group.get_term_checked().await;
    check_role_state(&group, 5, &leader0);

    // check after some time, the term and the leader is still not changed
    sleep_secs(15).await;
    let leader2 = group
        .try_get_leader()
        .await
        .expect("There should be one leader")
        .0;
    let term2 = group.get_term_checked().await;
    check_role_state(&group, 5, &leader0);

    assert_ne!(leader0, leader1);
    assert_eq!(term1, term2);
    assert_eq!(leader1, leader2);

    group.stop().await;
}

// Reelect after network failure
#[madsim::test]
async fn reelect() {
    init_logger();

    let group = CurpGroup::new(5).await;

    // check whether there is exact one leader in the group
    let leader1 = group.get_leader().await.0;
    let term1 = group.get_term_checked().await;
    check_role_state(&group, 5, &leader1);
    // disable leader 1
    group.disable_node(&leader1);
    println!("disable leader {leader1}");

    // after some time, a new leader should be elected
    let (leader2, term2) = wait_for_election(&group).await;
    check_role_state(&group, 5, &leader2);

    assert_ne!(term1, term2);
    assert_ne!(leader1, leader2);

    // disable leader 2
    group.disable_node(&leader2);
    println!("disable leader {leader2}");

    // after some time, a new leader should be elected
    let (leader3, term3) = wait_for_election(&group).await;
    check_role_state(&group, 5, &leader3);

    assert_ne!(term1, term3);
    assert_ne!(term2, term3);
    assert_ne!(leader1, leader3);
    assert_ne!(leader2, leader3);

    // disable leader 3
    group.disable_node(&leader3);
    println!("disable leader {leader3}");

    // after some time, no leader should be elected
    sleep_secs(15).await;
    assert!(group.try_get_leader().await.is_none());

    // recover network partition
    println!("enable all");
    group.enable_node(&leader1);
    group.enable_node(&leader2);
    group.enable_node(&leader3);

    let (final_leader, final_term) = wait_for_election(&group).await;
    check_role_state(&group, 5, &final_leader);
    assert!(final_term > term3);

    group.stop().await;
}

#[madsim::test]
async fn propose_after_reelect() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;
    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0), true)
            .await
            .unwrap()
            .0
             .0,
        vec![]
    );

    let leader1 = group.get_leader().await.0;
    check_role_state(&group, 5, &leader1);
    group.disable_node(&leader1);

    let (_leader, _term) = wait_for_election(&group).await;
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]), true)
            .await
            .unwrap()
            .0
             .0,
        vec![0]
    );

    group.stop().await;
}
