use anyhow::Result;
use xline_client::{
    types::lease::{
        LeaseGrantRequest, LeaseKeepAliveRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest,
    },
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let mut client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .lease_client();

    // grant new lease
    let resp1 = client.grant(LeaseGrantRequest::new(60)).await?;
    let resp2 = client.grant(LeaseGrantRequest::new(60)).await?;
    let lease_id1 = resp1.id;
    let lease_id2 = resp2.id;
    println!("lease id 1: {}", lease_id1);
    println!("lease id 2: {}", lease_id2);

    // get the ttl of lease1
    let resp = client
        .time_to_live(LeaseTimeToLiveRequest::new(lease_id1))
        .await?;

    println!("remaining ttl: {}", resp.ttl);

    // keep alive lease2
    let (mut keeper, mut stream) = client
        .keep_alive(LeaseKeepAliveRequest::new(lease_id2))
        .await?;

    if let Some(resp) = stream.message().await? {
        println!("new ttl: {}", resp.ttl);
    }

    // keep alive lease2 again using the keeper
    keeper.keep_alive()?;

    // list all leases
    for lease in client.leases().await?.leases {
        println!("lease: {}", lease.id);
    }

    // revoke the leases
    let _resp = client.revoke(LeaseRevokeRequest::new(lease_id1)).await?;
    let _resp = client.revoke(LeaseRevokeRequest::new(lease_id2)).await?;

    Ok(())
}
