mod common;

use std::{error::Error, time::Duration};

use common::Cluster;
use etcd_client::LockOptions;
use tokio::time::{self, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_lock() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut lock_client = client.lock_client();

    let lock_handle = tokio::spawn({
        let mut c = lock_client.clone();
        async move {
            let res = c.lock("test", None).await.unwrap();
            time::sleep(Duration::from_secs(3)).await;
            let _res = c.unlock(res.key()).await.unwrap();
        }
    });

    time::sleep(Duration::from_secs(1)).await;
    let now = time::Instant::now();
    let res = lock_client.lock("test", None).await?;
    let elapsed = now.elapsed();
    assert!(res.key().starts_with(b"test"));
    assert!(elapsed >= Duration::from_secs(1));
    let _ignore = lock_handle.await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_lock_timeout() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut lock_client = client.lock_client();

    let lease_id = client.lease_client().grant(1, None).await?.id();
    let _res = lock_client
        .lock("test", Some(LockOptions::new().with_lease(lease_id)))
        .await?;

    let res = timeout(Duration::from_secs(3), lock_client.lock("test", None)).await??;
    assert!(res.key().starts_with(b"test"));

    Ok(())
}
