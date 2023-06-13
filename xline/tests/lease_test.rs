use std::{error::Error, time::Duration};

use tracing::info;
use xline::client::kv_types::{LeaseGrantRequest, PutRequest, RangeRequest};

use xline_test_utils::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_lease_expired() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let res = client.lease_grant(LeaseGrantRequest::new(1)).await?;
    let lease_id = res.id;
    assert!(lease_id > 0);

    let _ = client
        .put(PutRequest::new("foo", "bar").with_lease(lease_id))
        .await?;
    let res = client.range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let res = client.range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_lease_keep_alive() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let non_leader_ep = cluster.addrs()["server1"].to_string();
    let client = cluster.client().await;

    let res = client.lease_grant(LeaseGrantRequest::new(1)).await?;
    let lease_id = res.id;
    assert!(lease_id > 0);

    let _ = client
        .put(PutRequest::new("foo", "bar").with_lease(lease_id))
        .await?;
    let res = client.range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    let mut c = etcd_client::Client::connect(vec![non_leader_ep], None).await?;
    let (mut keeper, mut stream) = c.lease_keep_alive(lease_id).await?;
    let handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = keeper.keep_alive().await;
            if let Ok(Some(r)) = stream.message().await {
                info!("keep alive response: {:?}", r);
            };
        }
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    let res = client.range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    handle.abort();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let res = client.range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 0);
    Ok(())
}
