use std::time::Duration;

use test_macros::abort_on_panic;
use xline_client::{
    error::Result,
    types::lock::{LockRequest, UnlockRequest},
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
async fn lock_unlock_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.lock_client();

    let resp = client.lock(LockRequest::new("lock-test")).await?;
    assert!(resp.key.starts_with(b"lock-test/"));

    client.unlock(UnlockRequest::new(resp.key)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn lock_contention_should_occur_when_acquire_by_two() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.lock_client();
    let client_c = client.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let resp = client.lock(LockRequest::new("lock-test")).await.unwrap();

    let handle = tokio::spawn(async move {
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            client_c.lock(LockRequest::new("lock-test")),
        )
        .await;
        assert!(res.is_err());
        let _ignore = tx.send(());

        let res = tokio::time::timeout(
            Duration::from_millis(200),
            client_c.lock(LockRequest::new("lock-test")),
        )
        .await;
        assert!(res.is_ok_and(|r| r.is_ok_and(|resp| resp.key.starts_with(b"lock-test/"))));
    });

    rx.recv().await.unwrap();
    let _resp = client.unlock(UnlockRequest::new(resp.key)).await.unwrap();

    handle.await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn lock_should_timeout_when_ttl_is_set() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.lock_client();

    let _resp = client
        .lock(LockRequest::new("lock-test").with_ttl(1))
        .await
        .unwrap();

    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        client.lock(LockRequest::new("lock-test")),
    )
    .await
    .expect("timeout when trying to lock")?;

    assert!(resp.key.starts_with(b"lock-test/"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn lock_should_unlock_after_cancelled() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.lock_client();
    let client_c = client.clone();
    // first acquire the lock
    let resp = client.lock(LockRequest::new("lock-test")).await.unwrap();

    // acquire the lock again and then cancel it
    let res = tokio::time::timeout(
        Duration::from_secs(1),
        client_c.lock(LockRequest::new("lock-test")),
    )
    .await;
    assert!(res.is_err());

    // unlock the first one
    client.unlock(UnlockRequest::new(resp.key)).await?;

    // try lock again, it should success
    let resp = tokio::time::timeout(
        Duration::from_secs(1),
        client.lock(LockRequest::new("lock-test")),
    )
    .await
    .expect("timeout when trying to lock")?;

    assert!(resp.key.starts_with(b"lock-test/"));

    Ok(())
}
