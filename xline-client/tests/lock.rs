use common::get_cluster_client;
use xline_client::{
    error::Result,
    types::lock::{LockRequest, UnlockRequest},
};

mod common;

#[tokio::test]
async fn lock_unlock_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await?;
    let mut client = client.lock_client();

    let resp = client
        .lock(LockRequest::new().with_name("lock-test"))
        .await?;
    let key = resp.key;
    let key_str = std::str::from_utf8(&key).unwrap();
    assert!(key_str.starts_with("lock-test/"));

    client.unlock(UnlockRequest::new().with_key(key)).await?;
    Ok(())
}
