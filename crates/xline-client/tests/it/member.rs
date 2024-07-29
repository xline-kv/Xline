use test_macros::abort_on_panic;
use xline_client::error::Result;

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn learner_add_and_remove_are_ok() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.member_client();

    let ids = client
        .add_learner(vec!["10.0.0.4:2379".to_owned(), "10.0.0.5:2379".to_owned()])
        .await
        .expect("failed to add learners");

    let added = ids.len();
    assert_eq!(added, 2, "expected 2 learners to be added, got {added}");

    // Remove the previously added learners
    client
        .remove_learner(ids)
        .await
        .expect("failed to remove learners");

    Ok(())
}
