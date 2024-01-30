use anyhow::Result;
use xline_client::{Client, ClientOptions};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let mut client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .maintenance_client();

    // snapshot
    let mut msg = client.snapshot().await?;
    let mut snapshot = vec![];
    loop {
        if let Some(resp) = msg.message().await? {
            snapshot.extend_from_slice(&resp.blob);
            if resp.remaining_bytes == 0 {
                break;
            }
        }
    }
    println!("snapshot size: {}", snapshot.len());

    Ok(())
}
