//! The following tests are originally from `etcd-client`
use xline_client::{
    error::Result,
    types::{
        kv::PutRequest,
        watch::{EventType, WatchRequest},
    },
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
async fn watch_should_receive_consistent_events() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let mut watch_client = client.watch_client();
    let kv_client = client.kv_client();

    let (mut watcher, mut stream) = watch_client.watch(WatchRequest::new("watch01")).await?;

    kv_client.put(PutRequest::new("watch01", "01")).await?;

    let resp = stream.message().await?.unwrap();
    assert_eq!(resp.watch_id, watcher.watch_id());
    assert_eq!(resp.events.len(), 1);

    let kv = resp.events[0].kv.as_ref().unwrap();
    assert_eq!(kv.key, b"watch01");
    assert_eq!(kv.value, b"01");
    assert_eq!(resp.events[0].r#type(), EventType::Put);

    watcher.cancel()?;

    let resp = stream.message().await?.unwrap();
    assert_eq!(resp.watch_id, watcher.watch_id());
    assert!(resp.canceled);

    Ok(())
}

/// To ensure #505 is fixed
#[tokio::test(flavor = "multi_thread")]
async fn watch_stream_should_work_after_watcher_dropped() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let mut watch_client = client.watch_client();
    let kv_client = client.kv_client();

    let (_, mut stream) = watch_client.watch(WatchRequest::new("watch01")).await?;

    kv_client.put(PutRequest::new("watch01", "01")).await?;

    let resp = stream.message().await?.unwrap();
    assert_eq!(resp.events.len(), 1);

    let kv = resp.events[0].kv.as_ref().unwrap();
    assert_eq!(kv.key, b"watch01");
    assert_eq!(kv.value, b"01");
    assert_eq!(resp.events[0].r#type(), EventType::Put);

    Ok(())
}
