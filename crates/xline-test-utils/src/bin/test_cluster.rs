//! The binary is to quickly setup a cluster for convenient testing
use tokio::signal;
use xline_test_utils::Cluster;

#[tokio::main]
async fn main() {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;

    println!("cluster running");

    for (id, addr) in cluster.all_members_client_urls_map() {
        println!("server id: {} addr: {}", id, addr);
    }

    if let Err(e) = signal::ctrl_c().await {
        eprintln!("Unable to listen for shutdown signal: {e}");
    }
}
