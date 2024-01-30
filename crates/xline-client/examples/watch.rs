use anyhow::Result;
use xline_client::{
    types::{kv::PutRequest, watch::WatchRequest},
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default()).await?;
    let mut watch_client = client.watch_client();
    let kv_client = client.kv_client();

    // watch
    let (mut watcher, mut stream) = watch_client.watch(WatchRequest::new("key1")).await?;
    kv_client.put(PutRequest::new("key1", "value1")).await?;

    let resp = stream.message().await?.unwrap();
    let kv = resp.events[0].kv.as_ref().unwrap();

    println!(
        "got key: {}, value: {}",
        String::from_utf8_lossy(&kv.key),
        String::from_utf8_lossy(&kv.value)
    );

    // cancel the watch
    watcher.cancel()?;

    Ok(())
}
