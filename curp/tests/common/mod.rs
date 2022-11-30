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
pub enum TestCommandType {
    Get,
    Put,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TestCommand {
    id: ProposeId,
    t: TestCommandType,
    keys: Vec<String>,
    value: Option<String>,
}

impl TestCommand {
    pub fn new(
        id: ProposeId,
        t: TestCommandType,
        keys: Vec<String>,
        value: Option<String>,
    ) -> Self {
        Self { id, t, keys, value }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TestCommandResult {
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
pub struct TestExecutor {
    exe_sender: mpsc::Sender<(TestCommandType, String)>,
    after_sync_sender: mpsc::Sender<(TestCommandType, String)>,
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestExecutor {
    async fn execute(&self, cmd: &TestCommand) -> Result<TestCommandResult, ExecuteError> {
        let _ = self
            .exe_sender
            .send((cmd.t.clone(), cmd.keys()[0].clone()))
            .await;
        match cmd.t {
            TestCommandType::Get => Ok(TestCommandResult::GetResult("".to_owned())),
            TestCommandType::Put => Ok(TestCommandResult::PutResult(
                cmd.value
                    .as_ref()
                    .expect("push command should contain value")
                    .clone(),
            )),
        }
    }

    async fn after_sync(
        &self,
        cmd: &TestCommand,
        index: LogIndex,
    ) -> Result<LogIndex, ExecuteError> {
        let _ = self
            .after_sync_sender
            .send((cmd.t.clone(), cmd.keys()[0].clone()))
            .await;
        match cmd.t {
            TestCommandType::Get => Ok(index),
            TestCommandType::Put => Ok(index),
        }
    }
}

impl TestExecutor {
    pub fn new(
        exe_sender: mpsc::Sender<(TestCommandType, String)>,
        after_sync_sender: mpsc::Sender<(TestCommandType, String)>,
    ) -> Self {
        Self {
            exe_sender,
            after_sync_sender,
        }
    }
}

#[allow(dead_code)]
pub async fn create_servers_client() -> (
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
        Rpc::<TestCommand>::run(addr0.as_str(), true, 0, addr1, Some(8765), exe).await
    });
    let exe_tx2 = exe_tx.clone();
    let after_sync_tx2 = after_sync_tx.clone();
    let addr1 = addrs[1].clone();
    let addr2 = vec![addrs[0].clone(), addrs[2].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(exe_tx2, after_sync_tx2);
        Rpc::<TestCommand>::run(addr1.as_str(), false, 0, addr2, Some(8766), exe).await
    });
    let exe_tx3 = exe_tx.clone();
    let after_sync_tx3 = after_sync_tx.clone();
    let addr2 = addrs[2].clone();
    let addr3 = vec![addrs[0].clone(), addrs[1].clone()];
    tokio::spawn(async move {
        let exe = TestExecutor::new(exe_tx3, after_sync_tx3);
        let _ = Rpc::<TestCommand>::run(addr2.as_str(), false, 0, addr3, Some(8767), exe).await;
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
    (exe_rx, after_sync_rx, client)
}
