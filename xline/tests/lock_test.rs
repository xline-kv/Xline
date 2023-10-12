use std::{error::Error, time::Duration};

use test_macros::abort_on_panic;
use tokio::time::{self, timeout};
use xline_test_utils::{
    types::{
        lease::LeaseGrantRequest,
        lock::{LockRequest, UnlockRequest},
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

    let lock_handle = tokio::spawn({
        let c = lock_client.clone();
        async move {
            let res = c.lock(LockRequest::new("test")).await.unwrap();
            time::sleep(Duration::from_secs(3)).await;
            let _res = c.unlock(UnlockRequest::new(res.key)).await.unwrap();
        }
    });

    time::sleep(Duration::from_secs(1)).await;
    let now = time::Instant::now();
    let res = lock_client.lock(LockRequest::new("test")).await?;
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

    let lease_id = client
        .lease_client()
        .grant(LeaseGrantRequest::new(1))
        .await?
        .id;
    let _res = lock_client
        .lock(LockRequest::new("test").with_lease(lease_id))
        .await?;

    let res = timeout(
        Duration::from_secs(3),
        lock_client.lock(LockRequest::new("test")),
    )
    .await??;
    assert!(res.key.starts_with(b"test"));

    Ok(())
}
