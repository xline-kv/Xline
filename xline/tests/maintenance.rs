use std::{path::PathBuf, time::Duration};

use test_macros::abort_on_panic;
use tokio::io::AsyncWriteExt;
#[cfg(test)]
use xline::restore::restore;
use xline_test_utils::{
    types::kv::{PutRequest, RangeRequest},
    Client, ClientOptions, Cluster,
};

#[cfg(test)]
#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_snapshot_and_restore() -> Result<(), Box<dyn std::error::Error>> {
    let dir = PathBuf::from("/tmp/test_snapshot_and_restore");
    tokio::fs::create_dir_all(&dir).await?;
    let snapshot_path = dir.join("snapshot");
    let restore_dirs = (0..3).map(|i| dir.join(format!("restore_{}", i))).collect();
    {
        let mut cluster = Cluster::new(3).await;
        cluster.start().await;
        let client = cluster.client().await.kv_client();
        let _ignore = client.put(PutRequest::new("key", "value")).await?;
        tokio::time::sleep(Duration::from_millis(100)).await; // TODO: use `propose_index` and remove this sleep after we finished our client.
        let mut maintenance_client = Client::connect(
            vec![cluster.all_members()["server0"].to_string()],
            ClientOptions::default(),
        )
        .await?
        .maintenance_client();
        let mut stream = maintenance_client.snapshot().await?;
        let mut snapshot = tokio::fs::File::create(&snapshot_path).await?;
        while let Some(chunk) = stream.message().await? {
            snapshot.write_all(chunk.blob.as_slice()).await?;
        }
    }
    for restore_dir in &restore_dirs {
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
