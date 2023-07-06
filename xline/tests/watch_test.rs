use std::error::Error;

use etcd_client::EventType;
use test_macros::abort_on_panic;
use xline::client::kv_types::{DeleteRangeRequest, PutRequest};
use xline_test_utils::Cluster;

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_watch() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut watch_client = client.watch_client();

    let (_watcher, mut stream) = watch_client.watch("foo", None).await?;
    let handle = tokio::spawn(async move {
        if let Ok(Some(res)) = stream.message().await {
            let event = res.events().get(0).unwrap();
            let kv = event.kv().unwrap();
            assert_eq!(event.event_type(), EventType::Put);
            assert_eq!(kv.key(), b"foo");
            assert_eq!(kv.value(), b"bar");
        }
        if let Ok(Some(res)) = stream.message().await {
            let event = res.events().get(0).unwrap();
            let kv = event.kv().unwrap();
            assert_eq!(event.event_type(), EventType::Delete);
            assert_eq!(kv.key(), b"foo");
            assert_eq!(kv.value(), b"");
        }
    });

    client.put(PutRequest::new("foo", "bar")).await?;
    client.delete(DeleteRangeRequest::new("foo")).await?;

    handle.await?;
    Ok(())
}
