use std::{path::PathBuf, time::Duration};

use test_macros::abort_on_panic;
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use xline::restore::restore;
use xline_client::error::XlineClientError;
use xline_test_utils::{
    types::kv::{PutRequest, RangeRequest},
    Client, ClientOptions, Cluster,
};
use xlineapi::{AlarmAction, AlarmRequest, AlarmType, execute_error::ExecuteError};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_snapshot_and_restore() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;

    let dir = PathBuf::from("/tmp/test_snapshot_and_restore");
    tokio::fs::create_dir_all(&dir).await?;
    let snapshot_path = dir.join("snapshot");
    let restore_dirs: HashMap<usize, PathBuf> = (0..3)
        .map(|i| (i, dir.join(format!("restore_{}", i))))
        .collect();
    {
        let mut cluster = Cluster::new(3).await;
        cluster.start().await;
        let client = cluster.client().await.kv_client();
        let _ignore = client.put(PutRequest::new("key", "value")).await?;
        tokio::time::sleep(Duration::from_millis(100)).await; // TODO: use `propose_index` and remove this sleep after we finished our client.
        let mut maintenance_client =
            Client::connect(vec![cluster.get_addr(0)], ClientOptions::default())
                .await?
                .maintenance_client();
        let mut stream = maintenance_client.snapshot().await?;
        let mut snapshot = tokio::fs::File::create(&snapshot_path).await?;
        while let Some(chunk) = stream.message().await? {
            snapshot.write_all(chunk.blob.as_slice()).await?;
        }
    }
    for restore_dir in restore_dirs.values() {
        restore(&snapshot_path, &restore_dir).await?;
    }
    let mut new_cluster = Cluster::new(3).await;
    new_cluster.set_paths(restore_dirs);
    new_cluster.start().await;
    let client = new_cluster.client().await.kv_client();
    let res = client.range(RangeRequest::new("key")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].key, b"key");
    assert_eq!(res.kvs[0].value, b"value");
    tokio::fs::remove_dir_all(&dir).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn leader_should_detect_no_space_alarm() {
    test_alarm(0).await;
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn follower_should_detect_no_space_alarm() {
    test_alarm(1).await;
}

async fn test_alarm(idx: usize) {
    let q = 8 * 1024;
    let mut cluster = Cluster::new(3).await;
    _ = cluster.quotas.insert(idx, q);
    cluster.start().await;
    let client = cluster.client().await;
    let mut m_client = client.maintenance_client();
    let k_client = client.kv_client();

    for i in 1..100u8 {
        let key: Vec<u8> = vec![i];
        let value: Vec<u8> = vec![i];
        let req = PutRequest::new(key, value);
        if let Err(err) = k_client.put(req).await {
            assert!(matches!(
                err,
                XlineClientError::CommandError(ExecuteError::Nospace)
            ));
            break;
        }
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    let res = m_client
        .alarm(AlarmRequest::new(AlarmAction::Get, 0, AlarmType::None))
        .await
        .unwrap();
    assert!(!res.alarms.is_empty());
}
