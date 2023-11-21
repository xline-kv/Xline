use std::error::Error;

use test_macros::abort_on_panic;
use xline_test_utils::{
    types::{
        kv::{DeleteRangeRequest, PutRequest},
        watch::WatchRequest,
    },
    Cluster,
};
use xlineapi::EventType;

fn event_type(event_type: i32) -> EventType {
    match event_type {
        0 => EventType::Put,
        1 => EventType::Delete,
        _ => unreachable!("un"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_watch() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut watch_client = client.watch_client();
    let kv_client = client.kv_client();

    let (_watcher, mut stream) = watch_client.watch(WatchRequest::new("foo")).await?;
    let handle = tokio::spawn(async move {
        if let Ok(Some(res)) = stream.message().await {
            let event = res.events.get(0).unwrap();
            assert_eq!(event_type(event.r#type), EventType::Put);
            let kv = event.kv.clone().unwrap();
            assert_eq!(kv.key, b"foo");
            assert_eq!(kv.value, b"bar");
        }
        if let Ok(Some(res)) = stream.message().await {
            let event = res.events.get(0).unwrap();
            let kv = event.kv.clone().unwrap();
            assert_eq!(event_type(event.r#type), EventType::Delete);
            assert_eq!(kv.key, b"foo");
            assert_eq!(kv.value, b"");
        }
    });

    kv_client.put(PutRequest::new("foo", "bar")).await?;
    kv_client.delete(DeleteRangeRequest::new("foo")).await?;

    handle.await?;

    Ok(())
}
