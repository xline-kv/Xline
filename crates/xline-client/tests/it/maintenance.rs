use xline_client::error::Result;

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_should_get_valid_data() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let mut client = client.maintenance_client();

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
