use anyhow::Result;
use xline_client::{
    types::lock::{LockRequest, UnlockRequest},
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .lock_client();

    // acquire a lock
    let resp = client.lock(LockRequest::new("lock-test")).await?;

    let key = resp.key;

    println!("lock key: {:?}", String::from_utf8_lossy(&key));

    // release the lock
    client.unlock(UnlockRequest::new(key)).await?;

    Ok(())
}
