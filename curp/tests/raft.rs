use curp::client::Client;
use curp::cmd::ProposeId;
use curp::server::Rpc;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tracing::debug;

mod common;

use common::*;

pub async fn create_raft_group() -> (
    Receiver<(TestCommandType, String)>,
    Receiver<(TestCommandType, String)>,
    Client<TestCommand>,
) {
    let addrs: Vec<String> = vec![
        "127.0.0.1:8765".to_owned(),
        "127.0.0.1:8766".to_owned(),
        "127.0.0.1:8767".to_owned(),
    ];

    let (exe_tx, exe_rx) = mpsc::channel(100);
    let (after_sync_tx, after_sync_rx) = mpsc::channel(100);

    let exe_tx1 = exe_tx.clone();
    let after_sync_tx1 = after_sync_tx.clone();
    let addr0 = addrs[0].clone();
    let addr1 = vec![addrs[1].clone(), addrs[2].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(exe_tx1, after_sync_tx1);
        Rpc::<TestCommand, TestExecutor>::run(addr0.as_str(), false, 0, addr1, Some(8765), exe)
            .await
    });
    let exe_tx2 = exe_tx.clone();
    let after_sync_tx2 = after_sync_tx.clone();
    let addr1 = addrs[1].clone();
    let addr2 = vec![addrs[0].clone(), addrs[2].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(exe_tx2, after_sync_tx2);
        Rpc::<TestCommand, TestExecutor>::run(addr1.as_str(), false, 0, addr2, Some(8766), exe)
            .await
    });
    let exe_tx3 = exe_tx.clone();
    let after_sync_tx3 = after_sync_tx.clone();
    let addr2 = addrs[2].clone();
    let addr3 = vec![addrs[0].clone(), addrs[1].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(exe_tx3, after_sync_tx3);
        let _ =
            Rpc::<TestCommand, TestExecutor>::run(addr2.as_str(), false, 0, addr3, Some(8767), exe)
                .await;
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let client = Client::<TestCommand>::new(
        0,
        addrs
            .into_iter()
            .map(|a| a.parse())
            .collect::<Result<Vec<SocketAddr>, _>>()
            .unwrap(),
    )
    .await;
    (exe_rx, after_sync_rx, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn leader_elect() {
    tracing_subscriber::fmt::init();
    // watch the log while doing sync, TODO: find a better way
    let (_exe_rx, _after_sync_rx, client) = create_raft_group().await;
    debug!("started");

    let _res = client
        .propose(TestCommand::new(
            ProposeId::new("id0".to_string()),
            TestCommandType::Get,
            vec!["id0".to_string()],
            None,
        ))
        .await;

    // wait until sync completed
    tokio::time::sleep(Duration::from_secs(1)).await;
}
