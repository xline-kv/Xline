use std::{error::Error, time::Duration};

use test_macros::abort_on_panic;
use tokio::time::{self, timeout};
use xline_test_utils::{
    types::{
        lease::LeaseGrantRequest,
        lock::{LockRequest, UnlockRequest, DEFAULT_SESSION_TTL},
    },
    Cluster,
};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_lock() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let lock_client = client.lock_client();
    let lease_id_1 = client
        .lease_client()
        .grant(LeaseGrantRequest::new(DEFAULT_SESSION_TTL))
        .await?
        .id;

    let lock_handle = tokio::spawn({
        let c = lock_client.clone();
        async move {
            let res = c
                .lock(LockRequest::new("test").with_lease(lease_id_1))
                .await
                .unwrap();
            time::sleep(Duration::from_secs(3)).await;
            let _res = c.unlock(UnlockRequest::new(res.key)).await.unwrap();
        }
    });

    time::sleep(Duration::from_secs(1)).await;
    let now = time::Instant::now();
    let lease_id_2 = client
        .lease_client()
        .grant(LeaseGrantRequest::new(DEFAULT_SESSION_TTL))
        .await?
        .id;
    let res = lock_client
        .lock(LockRequest::new("test").with_lease(lease_id_2))
        .await?;
    let elapsed = now.elapsed();
    assert!(res.key.starts_with(b"test"));
    assert!(elapsed >= Duration::from_secs(1));
    let _ignore = lock_handle.await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_lock_timeout() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let lock_client = client.lock_client();

    let lease_id_1 = client
        .lease_client()
        .grant(LeaseGrantRequest::new(1))
        .await?
        .id;
    let _res = lock_client
        .lock(LockRequest::new("test").with_lease(lease_id_1))
        .await?;
    let lease_id_2 = client
        .lease_client()
        .grant(LeaseGrantRequest::new(DEFAULT_SESSION_TTL))
        .await?
        .id;

    let res = timeout(
        Duration::from_secs(3),
        lock_client.lock(LockRequest::new("test").with_lease(lease_id_2)),
    )
    .await??;
    assert!(res.key.starts_with(b"test"));

    Ok(())
}
