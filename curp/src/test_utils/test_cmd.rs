use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
use madsim::rand::{distributions::Alphanumeric, thread_rng, Rng};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::debug;

use crate::{
    cmd::{Command, CommandExecutor, ConflictCheck, ProposeId},
    error::ExecuteError,
    message::ServerId,
    LogIndex,
};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct TestCommand {
    id: ProposeId,
    keys: Vec<u32>,
    cmd_type: TestCommandType,
}

impl Default for TestCommand {
    fn default() -> Self {
        Self {
            id: random_id(),
            keys: vec![1],
            cmd_type: TestCommandType::Get,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) enum TestCommandType {
    Get,
    Put(u32),
}

pub(crate) type TestCommandResult = Vec<u32>;

impl TestCommand {
    pub(crate) fn new_get(keys: Vec<u32>) -> Self {
        Self {
            id: random_id(),
            keys,
            cmd_type: TestCommandType::Get,
        }
    }
    pub(crate) fn new_put(keys: Vec<u32>, value: u32) -> Self {
        Self {
            id: random_id(),
            keys,
            cmd_type: TestCommandType::Put(value),
        }
    }
}

impl Command for TestCommand {
    type K = u32;

    type ER = TestCommandResult;

    type ASR = LogIndex;

    fn keys(&self) -> &[Self::K] {
        &self.keys
    }

    fn id(&self) -> &ProposeId {
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
pub(crate) struct TestCE {
    server_id: ServerId,
    store: Arc<Mutex<HashMap<u32, u32>>>,
    exe_sender: mpsc::UnboundedSender<(TestCommand, TestCommandResult)>,
    after_sync_sender: mpsc::UnboundedSender<(TestCommand, LogIndex)>,
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestCE {
    async fn execute(&self, cmd: &TestCommand) -> Result<TestCommandResult, ExecuteError> {
        let mut store = self.store.lock();
        debug!("{} execute cmd {:?}", self.server_id, cmd.id());

        let result: TestCommandResult = match cmd.cmd_type {
            TestCommandType::Get => cmd
                .keys
                .iter()
                .filter_map(|key| store.get(key).copied())
                .collect(),
            TestCommandType::Put(v) => cmd
                .keys
                .iter()
                .filter_map(|key| store.insert(key.to_owned(), v))
                .collect(),
        };

        self.exe_sender
            .send((cmd.clone(), result.clone()))
            .expect("failed to send exe msg");
        Ok(result)
    }

    async fn after_sync(
        &self,
        cmd: &TestCommand,
        index: LogIndex,
    ) -> Result<LogIndex, ExecuteError> {
        self.after_sync_sender
            .send((cmd.clone(), index))
            .expect("failed to send after sync msg");
        Ok(index)
    }
}

impl TestCE {
    pub(crate) fn new(
        server_id: ServerId,
        exe_sender: mpsc::UnboundedSender<(TestCommand, TestCommandResult)>,
        after_sync_sender: mpsc::UnboundedSender<(TestCommand, LogIndex)>,
    ) -> Self {
        Self {
            server_id,
            store: Arc::new(Mutex::new(HashMap::new())),
            exe_sender,
            after_sync_sender,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TestCESimple {
    server_id: ServerId,
    store: Arc<Mutex<HashMap<u32, u32>>>,
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestCESimple {
    async fn execute(&self, cmd: &TestCommand) -> Result<TestCommandResult, ExecuteError> {
        let mut store = self.store.lock();
        debug!("{} execute cmd {:?}", self.server_id, cmd.id());

        let result: TestCommandResult = match cmd.cmd_type {
            TestCommandType::Get => cmd
                .keys
                .iter()
                .filter_map(|key| store.get(key).copied())
                .collect(),
            TestCommandType::Put(v) => cmd
                .keys
                .iter()
                .filter_map(|key| store.insert(key.to_owned(), v))
                .collect(),
        };
        Ok(result)
    }

    async fn after_sync(
        &self,
        cmd: &TestCommand,
        index: LogIndex,
    ) -> Result<LogIndex, ExecuteError> {
        debug!("{} call cmd {:?} after sync", self.server_id, cmd.id());
        Ok(index)
    }
}

impl TestCESimple {
    pub(crate) fn new(server_id: ServerId) -> Self {
        Self {
            server_id,
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub(crate) fn random_id() -> ProposeId {
    ProposeId::new(
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(4)
            .map(char::from)
            .collect(),
    )
}
