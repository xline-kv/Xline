use std::time::Duration;

use test_macros::abort_on_panic;
use xline_client::{
    clients::LeaseClient,
    error::Result,
    types::{
        lease::LeaseGrantRequest,
        lock::{LockRequest, UnlockRequest, DEFAULT_SESSION_TTL},
    },
};

use super::common::get_cluster_client;

async fn gen_lease_id(client: LeaseClient, ttl: i64) -> i64 {
    client
        .grant(LeaseGrantRequest::new(ttl))
        .await
        .expect("grant lease should be success")
        .id
}

#[tokio::test(flavor = "multi_thread")]
async fn lock_unlock_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let lock_client = client.lock_client();
    let lease_id = gen_lease_id(client.lease_client(), DEFAULT_SESSION_TTL).await;

    let resp = lock_client
        .lock(LockRequest::new("lock-test").with_lease(lease_id))
        .await?;
    assert!(resp.key.starts_with(b"lock-test/"));

    lock_client.unlock(UnlockRequest::new(resp.key)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn lock_contention_should_occur_when_acquire_by_two() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let lock_client = client.lock_client();
    let lease_id_1 = gen_lease_id(client.lease_client(), DEFAULT_SESSION_TTL).await;
    let lease_id_2 = gen_lease_id(client.lease_client(), DEFAULT_SESSION_TTL).await;

    let client_c = client.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let resp = lock_client
        .lock(LockRequest::new("lock-test").with_lease(lease_id_1))
        .await
        .unwrap();

    let handle = tokio::spawn(async move {
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            client_c
                .lock_client()
                .lock(LockRequest::new("lock-test").with_lease(lease_id_2)),
        )
        .await;
        assert!(res.is_err());
        let _ignore = tx.send(());
        let lease_id_3 = gen_lease_id(client_c.lease_client(), DEFAULT_SESSION_TTL).await;
        let res = tokio::time::timeout(
            Duration::from_millis(200),
            client_c
                .lock_client()
                .lock(LockRequest::new("lock-test").with_lease(lease_id_3)),
        )
        .await;
        assert!(res.is_ok_and(|r| r.is_ok_and(|resp| resp.key.starts_with(b"lock-test/"))));
    });

    rx.recv().await.unwrap();
    let _resp = lock_client
        .unlock(UnlockRequest::new(resp.key))
        .await
        .unwrap();

    handle.await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn lock_should_timeout_when_ttl_is_set() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let lock_client = client.lock_client();
    let lease_id_1 = gen_lease_id(client.lease_client(), 1).await;

    let _resp = lock_client
        .lock(LockRequest::new("lock-test").with_lease(lease_id_1))
        .await
        .unwrap();

    let lease_id_2 = gen_lease_id(client.lease_client(), DEFAULT_SESSION_TTL).await;
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        lock_client.lock(LockRequest::new("lock-test").with_lease(lease_id_2)),
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
    let lock_client = client.lock_client();
    let client_c = lock_client.clone();
    let lease_id_1 = gen_lease_id(client.lease_client(), DEFAULT_SESSION_TTL).await;
    let lease_id_2 = gen_lease_id(client.lease_client(), DEFAULT_SESSION_TTL).await;
    let lease_id_3 = gen_lease_id(client.lease_client(), DEFAULT_SESSION_TTL).await;
    // first acquire the lock
    let resp = lock_client
        .lock(LockRequest::new("lock-test").with_lease(lease_id_1))
        .await
        .unwrap();

    // acquire the lock again and then cancel it
    let res = tokio::time::timeout(
        Duration::from_secs(1),
        client_c.lock(LockRequest::new("lock-test").with_lease(lease_id_2)),
    )
    .await;
    assert!(res.is_err());

    // unlock the first one
    lock_client.unlock(UnlockRequest::new(resp.key)).await?;

    // try lock again, it should success
    let resp = tokio::time::timeout(
        Duration::from_secs(1),
        lock_client.lock(LockRequest::new("lock-test").with_lease(lease_id_3)),
    )
    .await
    .expect("timeout when trying to lock")?;

    assert!(resp.key.starts_with(b"lock-test/"));

    Ok(())
}
