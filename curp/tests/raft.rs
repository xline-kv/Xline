use crate::common::{create_servers_client, TestCommand, TestCommandType};
use curp::cmd::ProposeId;
use std::time::Duration;
use tracing::debug;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn watch_heartbeat() {
    tracing_subscriber::fmt::init();
    let (_exe_rx, _after_sync_rx, client) = create_servers_client().await;
    debug!("started");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let _result = client
        .propose(TestCommand::new(
            ProposeId::new("id1".to_owned()),
            TestCommandType::Get,
            vec!["A".to_owned()],
            None,
        ))
        .await;
}
