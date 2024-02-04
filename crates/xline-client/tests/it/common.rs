use xline_client::{error::XlineClientBuildError, Client, ClientOptions};
use xline_test_utils::Cluster;

pub async fn get_cluster_client() -> Result<(Cluster, Client), XlineClientBuildError> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = Client::connect(cluster.all_client_addrs(), ClientOptions::default()).await?;
    Ok((cluster, client))
}
