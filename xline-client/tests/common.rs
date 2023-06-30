use xline_client::{error::Result, Client, ClientOptions};
use xline_test_utils::Cluster;

pub async fn get_cluster_client() -> Result<(Cluster, Client)> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = Client::connect(cluster.addrs().clone(), ClientOptions::default()).await?;
    Ok((cluster, client))
}
