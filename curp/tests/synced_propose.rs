use std::{net::SocketAddr, thread, time::Duration};

use async_trait::async_trait;
use curp::{
    client::Client,
    cmd::{Command, CommandExecutor, ConflictCheck, ProposeId},
    error::ExecuteError,
    server::RpcServerWrap,
    LogIndex,
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum TestCommandType {
    Get,
    Put,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TestCommand {
    id: ProposeId,
    t: TestCommandType,
    keys: Vec<String>,
    value: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum TestCommandResult {
    PutResult(String),
    GetResult(String),
}

impl Command for TestCommand {
    type K = String;

    type ER = TestCommandResult;

    type ASR = LogIndex;

    fn keys(&self) -> &[Self::K] {
        &self.keys
    }

    fn id(&self) -> &curp::cmd::ProposeId {
        &self.id
    }
}

impl ConflictCheck for TestCommand {
    fn is_conflict(&self, other: &Self) -> bool {
        let this_keys = self.keys();
        let other_keys = other.keys();

        this_keys
            .iter()
            .cartesian_product(other_keys.iter())
            .map(|(k1, k2)| k1.is_conflict(k2))
            .any(|conflict| conflict)
    }
}

#[derive(Debug, Clone)]
struct TestExecutor {
    sender: mpsc::Sender<(TestCommandType, String)>,
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestExecutor {
    async fn execute(&self, cmd: &TestCommand) -> Result<TestCommandResult, ExecuteError> {
        let _ = self
            .sender
            .send((cmd.t.clone(), cmd.keys()[0].clone()))
            .await;
        match cmd.t {
            TestCommandType::Get => Ok(TestCommandResult::GetResult("".to_owned())),
            TestCommandType::Put => Ok(TestCommandResult::PutResult("".to_owned())),
        }
    }

    async fn after_sync(
        &self,
        _cmd: &TestCommand,
        index: LogIndex,
    ) -> Result<LogIndex, ExecuteError> {
        Ok(index)
    }
}

impl TestExecutor {
    fn new(sender: mpsc::Sender<(TestCommandType, String)>) -> Self {
        Self { sender }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn synced_propose() {
    let addrs: Vec<SocketAddr> = vec![
        "127.0.0.1:8765".parse().expect("parse address failed"),
        "127.0.0.1:8766".parse().expect("parse address failed"),
        "127.0.0.1:8767".parse().expect("parse address failed"),
    ];
    let (tx, mut rx) = mpsc::channel(10);
    let tx1 = tx.clone();
    let addr1 = vec![addrs[1], addrs[2]];
    tokio::spawn(async move {
        let exe = TestExecutor::new(tx1);
        RpcServerWrap::<TestCommand, TestExecutor>::run(true, 0, addr1, Some(8765), exe).await
    });
    let tx2 = tx.clone();
    let addr2 = vec![addrs[0], addrs[2]];
    tokio::spawn(async move {
        let exe = TestExecutor::new(tx2);
        RpcServerWrap::<TestCommand, TestExecutor>::run(false, 0, addr2, Some(8766), exe).await
    });
    let tx3 = tx.clone();
    let addr3 = vec![addrs[0], addrs[1]];
    tokio::spawn(async move {
        let exe = TestExecutor::new(tx3);
        RpcServerWrap::<TestCommand, TestExecutor>::run(false, 0, addr3, Some(8767), exe).await
    });

    thread::sleep(Duration::from_secs(1));

    let client = Client::<TestCommand>::new(&addrs[0], &addrs).await.unwrap();
    let result = client
        .propose_indexed(TestCommand {
            id: ProposeId::new("id1".to_owned()),
            t: TestCommandType::Get,
            keys: vec!["A".to_owned()],
            value: None,
        })
        .await;

    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        (TestCommandResult::GetResult("".to_owned()), 0)
    );

    let (t, key) = rx.recv().await.unwrap();
    assert_eq!(t, TestCommandType::Get);
    assert_eq!(key, "A".to_owned());
}
