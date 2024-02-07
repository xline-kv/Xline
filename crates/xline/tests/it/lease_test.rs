use std::{error::Error, time::Duration};

use test_macros::abort_on_panic;
use tracing::info;
use xline_test_utils::{
    types::{
        kv::{PutRequest, RangeRequest},
        lease::{LeaseGrantRequest, LeaseKeepAliveRequest},
    },
    Client, ClientOptions, Cluster,
};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_lease_expired() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let res = client
        .lease_client()
        .grant(LeaseGrantRequest::new(1))
        .await?;
    let lease_id = res.id;
    assert!(lease_id > 0);

    let _ = client
        .kv_client()
        .put(PutRequest::new("foo", "bar").with_lease(lease_id))
        .await?;
    let res = client.kv_client().range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let res = client.kv_client().range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_lease_keep_alive() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let non_leader_ep = cluster.get_client_url(1);
    let client = cluster.client().await;

    let res = client
        .lease_client()
        .grant(LeaseGrantRequest::new(1))
        .await?;
    let lease_id = res.id;
    assert!(lease_id > 0);

    let _ = client
        .kv_client()
        .put(PutRequest::new("foo", "bar").with_lease(lease_id))
        .await?;
    let res = client.kv_client().range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    let mut c = Client::connect(vec![non_leader_ep], ClientOptions::default())
        .await?
        .lease_client();
    let (mut keeper, mut stream) = c.keep_alive(LeaseKeepAliveRequest::new(lease_id)).await?;
    let handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = keeper.keep_alive();
            if let Ok(Some(r)) = stream.message().await {
                info!("keep alive response: {:?}", r);
            };
        }
    });

    tokio::time::sleep(Duration::from_secs(3)).await;
    let res = client.kv_client().range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].value, b"bar");

    handle.abort();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let res = client.kv_client().range(RangeRequest::new("foo")).await?;
    assert_eq!(res.kvs.len(), 0);

    Ok(())
}
