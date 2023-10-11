use anyhow::Result;
use xline_client::{Client, ClientOptions};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .auth_client();

    // enable auth
    let _resp = client.auth_enable().await?;

    // connect using the root user
    let options = ClientOptions::default().with_user("root", "rootpwd");
    let client = Client::connect(curp_members, options).await?.auth_client();

    // disable auth
    let _resp = client.auth_disable();

    // get auth status
    let resp = client.auth_status().await?;
    println!("auth status:");
    println!(
        "enabled: {}, revision: {}",
        resp.enabled, resp.auth_revision
    );

    Ok(())
}
