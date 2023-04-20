use std::{path::PathBuf, time::Duration};

use common::Cluster;
use tokio::io::AsyncWriteExt;
use xline::client::{
    kv_types::{PutRequest, RangeRequest},
    restore::restore,
};

mod common;

#[tokio::test]
async fn test_snapshot_and_restore() -> Result<(), Box<dyn std::error::Error>> {
    let dir = PathBuf::from("/tmp/test_snapshot_and_restore");
    tokio::fs::create_dir_all(&dir).await?;
    let snapshot_path = dir.join("snapshot");
    let restore_dirs = (0..3).map(|i| dir.join(format!("restore_{}", i))).collect();
    {
        let mut cluster = Cluster::new(3).await;
        cluster.start().await;
        let client = cluster.client().await;
        let _ignore = client.put(PutRequest::new("key", "value")).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut maintenance_client = client.maintenance_client();
        let mut stream = maintenance_client.snapshot().await?;
        let mut snapshot = tokio::fs::File::create(&snapshot_path).await?;
        while let Some(chunk) = stream.message().await? {
            snapshot.write_all(chunk.blob()).await?;
        }
    }
    for restore_dir in &restore_dirs {
        restore(&snapshot_path, &restore_dir).await?;
    }
    let mut new_cluster = Cluster::new(3).await;
    new_cluster.set_paths(restore_dirs);
    new_cluster.start().await;
    let client = new_cluster.client().await;
    let res = client.range(RangeRequest::new("key")).await?;
    assert_eq!(res.kvs.len(), 1);
    assert_eq!(res.kvs[0].key, b"key");
    assert_eq!(res.kvs[0].value, b"value");
    tokio::fs::remove_dir_all(&dir).await?;
    Ok(())
}
