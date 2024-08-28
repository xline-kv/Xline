use std::{error::Error, sync::Arc, time::Duration};

use test_macros::abort_on_panic;
use tokio::time::{sleep, Instant};
use xline_test_utils::{clients::Xutex, Cluster};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_lock() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let lock_client = client.lock_client();
    let event = Arc::new(event_listener::Event::new());

    let lock_handle = tokio::spawn({
        let c = lock_client.clone();
        let event = Arc::clone(&event);
        async move {
            let mut xutex = Xutex::new(c, "test", None, None).await.unwrap();
            let _lock = xutex.lock_unsafe().await.unwrap();
            let _notified = event.notify(1);
            sleep(Duration::from_secs(2)).await;
        }
    });

    event.listen().await;
    let now = Instant::now();

    let mut xutex = Xutex::new(lock_client, "test", None, None).await?;
    let _lock = xutex.lock_unsafe().await?;
    let elapsed = now.elapsed();
    assert!(elapsed >= Duration::from_secs(1));
    let _ignore = lock_handle.await;

    Ok(())
}
