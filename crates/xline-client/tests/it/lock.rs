use std::time::Duration;

use test_macros::abort_on_panic;
use xline_client::{clients::lock::Xutex, error::Result};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
async fn lock_unlock_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let lock_client = client.lock_client();
    let mut xutex = Xutex::new(lock_client, "lock-test", None, None).await?;

    assert!(xutex.lock_unsafe().await.is_ok());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn lock_contention_should_occur_when_acquire_by_two() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let lock_client = client.lock_client();
    let client_c = client.lock_client();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let mut xutex_1 = Xutex::new(lock_client, "lock-test", None, None).await?;
    let lock_1 = xutex_1.lock_unsafe().await.unwrap();

    let mut xutex_2 = Xutex::new(client_c, "lock-test", None, None).await?;

    let handle = tokio::spawn(async move {
        let lock_result = tokio::time::timeout(Duration::from_secs(2), xutex_2.lock_unsafe()).await;
        assert!(lock_result.is_err());
        let _ignore = tx.send(());

        let lock_result =
            tokio::time::timeout(Duration::from_millis(200), xutex_2.lock_unsafe()).await;
        assert!(lock_result.is_ok());
    });

    rx.recv().await.unwrap();
    std::mem::drop(lock_1);

    handle.await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn lock_should_unlock_after_cancelled() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let lock_client = client.lock_client();
    let client_c = lock_client.clone();
    let mut xutex_1 = Xutex::new(lock_client, "lock-test", None, None).await?;
    let mut xutex_2 = Xutex::new(client_c, "lock-test", None, None).await?;
    // first acquire the lock
    let lock_1 = xutex_1.lock_unsafe().await.unwrap();

    // acquire the lock again and then cancel it
    let res = tokio::time::timeout(Duration::from_secs(1), xutex_2.lock_unsafe()).await;
    assert!(res.is_err());

    // unlock the first one
    std::mem::drop(lock_1);

    // try lock again, it should success
    let _session = tokio::time::timeout(Duration::from_secs(1), xutex_2.lock_unsafe())
        .await
        .expect("timeout when trying to lock")?;

    Ok(())
}
