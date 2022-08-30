use std::{net::SocketAddr, thread, time::Duration};

use async_trait::async_trait;
use curp::{
    client::Client,
    cmd::{Command, CommandExecutor, ConflictCheck, ProposeId},
    error::ExecuteError,
    server::Rpc,
    LogIndex,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) enum TestCommandType {
    Get,
    Put,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct TestCommand {
    id: ProposeId,
    t: TestCommandType,
    keys: Vec<String>,
    value: Option<String>,
}

impl TestCommand {
    pub(crate) fn new(
        id: ProposeId,
        t: TestCommandType,
        keys: Vec<String>,
        value: Option<String>,
    ) -> Self {
        Self { id, t, keys, value }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) enum TestCommandResult {
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
pub(crate) struct TestExecutor {
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
    pub(crate) fn new(sender: mpsc::Sender<(TestCommandType, String)>) -> Self {
        Self { sender }
    }
}

pub(crate) async fn create_servers_client(
) -> (Receiver<(TestCommandType, String)>, Client<TestCommand>) {
    let addrs: Vec<String> = vec![
        "127.0.0.1:8765".to_owned(),
        "127.0.0.1:8766".to_owned(),
        "127.0.0.1:8767".to_owned(),
    ];

    let (tx, rx) = mpsc::channel(10);
    let tx1 = tx.clone();
    let addr1 = vec![addrs[1].clone(), addrs[2].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(tx1);
        Rpc::<TestCommand, TestExecutor>::run(true, 0, addr1, Some(8765), exe).await
    });
    let tx2 = tx.clone();
    let addr2 = vec![addrs[0].clone(), addrs[2].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(tx2);
        Rpc::<TestCommand, TestExecutor>::run(false, 0, addr2, Some(8766), exe).await
    });
    let tx3 = tx.clone();
    let addr3 = vec![addrs[0].clone(), addrs[1].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(tx3);
        let _ = Rpc::<TestCommand, TestExecutor>::run(false, 0, addr3, Some(8767), exe).await;
    });

    thread::sleep(Duration::from_secs(1));

    let client = Client::<TestCommand>::new(
        0,
        addrs
            .into_iter()
            .map(|a| a.parse())
            .collect::<Result<Vec<SocketAddr>, _>>()
            .unwrap(),
    )
    .await;
    (rx, client)
}
