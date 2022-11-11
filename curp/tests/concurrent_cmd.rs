use crate::common::{create_servers_client, TestCommand, TestCommandType};
use curp::cmd::ProposeId;
use std::time::Duration;
use tracing::debug;

mod common;

// #[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn concurrent_cmd() {
    tracing_subscriber::fmt::init();
    let (_exe_rx, _after_sync_rx, client) = create_servers_client().await;
    debug!("started");

    for i in 1..=20 {
        let _res = client
            .propose(TestCommand::new(
                ProposeId::new(format!("id{i}")),
                TestCommandType::Get,
                vec![format!("id{i}")],
                None,
            ))
            .await;
    }

    // watch the log while doing sync
    tokio::time::sleep(Duration::from_secs(1)).await;
}
