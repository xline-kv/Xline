use std::time::Duration;

use madsim::time::sleep;
use utils::config::ClientTimeout;

use crate::common::{curp_group::CurpGroup, init_logger, test_cmd::TestCommand};
mod common;

async fn wait_for_election() {
    sleep(Duration::from_secs(3)).await;
}

// Election
#[tokio::test]
async fn election() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let leader0 = group.get_leader().await.0;
    group.disable_node(&leader0);
    wait_for_election().await;

    // check whether there is exact one leader in the group
    let leader1 = group.get_leader().await.0;
    let term1 = group.get_term_checked().await;

    // check after some time, the term and the leader is still not changed
    tokio::time::sleep(Duration::from_secs(1)).await;
    let leader2 = group
        .try_get_leader()
        .await
        .expect("There should be one leader")
        .0;
    let term2 = group.get_term_checked().await;

    assert_ne!(leader0, leader1);
    assert_eq!(term1, term2);
    assert_eq!(leader1, leader2);

    group.stop();
}

// Reelect after network failure
#[tokio::test]
async fn reelect() {
    init_logger();

    let group = CurpGroup::new(5).await;

    // check whether there is exact one leader in the group
    let leader1 = group.get_leader().await.0;
    let term1 = group.get_term_checked().await;

    // disable leader 1
    group.disable_node(&leader1);
    println!("disable leader {leader1}");

    // after some time, a new leader should be elected
    wait_for_election().await;
    let (leader2, term2) = group.get_leader().await;

    assert_ne!(term1, term2);
    assert_ne!(leader1, leader2);

    // disable leader 2
    group.disable_node(&leader2);
    println!("disable leader {leader2}");

    // after some time, a new leader should be elected
    wait_for_election().await;
    let (leader3, term3) = group.get_leader().await;

    assert_ne!(term1, term3);
    assert_ne!(term2, term3);
    assert_ne!(leader1, leader3);
    assert_ne!(leader2, leader3);

    // disable leader 3
    group.disable_node(&leader3);
    println!("disable leader {leader3}");

    // after some time, no leader should be elected
    wait_for_election().await;
    assert!(group.try_get_leader().await.is_none());

    // recover network partition
    println!("enable all");
    group.enable_node(&leader1);
    group.enable_node(&leader2);
    group.enable_node(&leader3);

    wait_for_election().await;
    let (_, final_term) = group.get_leader().await;
    assert!(final_term > term3);

    group.stop();
}

#[tokio::test]
async fn propose_after_reelect() {
    init_logger();

    let group = CurpGroup::new(5).await;
    let client = group.new_client(ClientTimeout::default()).await;
    assert_eq!(
        client
            .propose(TestCommand::new_put(vec![0], 0))
            .await
            .unwrap()
            .0,
        vec![]
    );
    // wait for the cmd to be synced
    tokio::time::sleep(Duration::from_secs(1)).await;

    let leader1 = group.get_leader().await.0;
    group.disable_node(&leader1);

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        client
            .propose(TestCommand::new_get(vec![0]))
            .await
            .unwrap()
            .0,
        vec![0]
    );

    group.stop();
}
