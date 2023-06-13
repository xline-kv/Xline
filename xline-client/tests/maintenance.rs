use xline_client::{clients::maintenance::MaintenanceClient, error::Result, Client, ClientOptions};
use xline_test_utils::Cluster;

#[tokio::test]
async fn snapshot() -> Result<()> {
    let (_cluster, mut client) = get_cluster_client().await?;

    let mut msg = client.snapshot().await?;
    loop {
        if let Some(resp) = msg.message().await? {
            assert!(!resp.blob.is_empty());
            if resp.remaining_bytes == 0 {
                break;
            }
        }
    }
    Ok(())
}

async fn get_cluster_client() -> Result<(Cluster, MaintenanceClient)> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = Client::connect(cluster.addrs().clone(), ClientOptions::default())
        .await?
        .maintenance_client();
    Ok((cluster, client))
}
