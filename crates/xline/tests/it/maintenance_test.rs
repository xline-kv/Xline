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
use xlineapi::{execute_error::ExecuteError, AlarmAction, AlarmRequest, AlarmType};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_snapshot_and_restore() -> Result<(), Box<dyn std::error::Error>> {
    let dir = PathBuf::from("/tmp/test_snapshot_and_restore");
    tokio::fs::create_dir_all(&dir).await?;
    let snapshot_path = dir.join("snapshot");
    let restore_dirs: Vec<PathBuf> = (0..3).map(|i| dir.join(format!("restore_{}", i))).collect();
    let restore_cluster_configs = restore_dirs
        .iter()
        .cloned()
        .map(Cluster::default_rocks_config_with_path)
        .collect();
    {
        let mut cluster = Cluster::new_rocks(3).await;
        cluster.start().await;
        let client = cluster.client().await.kv_client();
        let _ignore = client.put(PutRequest::new("key", "value")).await?;
        tokio::time::sleep(Duration::from_millis(100)).await; // TODO: use `propose_index` and remove this sleep after we finished our client.
        let mut maintenance_client =
            Client::connect(vec![cluster.get_client_url(0)], ClientOptions::default())
                .await?
                .maintenance_client();
        let mut stream = maintenance_client.snapshot().await?;
        let mut snapshot = tokio::fs::File::create(&snapshot_path).await?;
        while let Some(chunk) = stream.message().await? {
            snapshot.write_all(chunk.blob.as_slice()).await?;
        }
    }
    for restore_dir in restore_dirs {
        restore(&snapshot_path, &restore_dir).await?;
    }
    let mut new_cluster = Cluster::new_with_configs(restore_cluster_configs).await;
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
    let configs = (0..3)
        .map(|i| {
            if i == idx {
                Cluster::default_quota_config(q)
            } else {
                Cluster::default_rocks_config()
            }
        })
        .collect();
    let mut cluster = Cluster::new_with_configs(configs).await;
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
                XlineClientError::ExecuteError(ExecuteError::Nospace)
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

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_status() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = Cluster::new_rocks(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut maintenance_client = client.maintenance_client();
    let res = maintenance_client.status().await?;
    assert_eq!(res.version, env!("CARGO_PKG_VERSION"));
    assert!(res.db_size > 0);
    assert!(res.db_size_in_use > 0);
    assert_ne!(res.leader, 0);
    assert!(res.raft_index >= res.raft_applied_index);
    assert_eq!(res.raft_term, 1);
    assert!(res.raft_applied_index > 0);
    assert!(!res.is_learner);

    Ok(())
}
