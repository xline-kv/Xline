use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
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
pub(super) struct TestCommand {
    id: ProposeId,
    keys: Vec<u32>,
    cmd_type: TestCommandType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(super) enum TestCommandType {
    Get,
    Put(u32),
}

pub(super) type TestCommandResult = Vec<u32>;

impl TestCommand {
    pub(super) fn new_get(id: u32, keys: Vec<u32>) -> Self {
        Self {
            id: ProposeId::new(id.to_string()),
            keys,
            cmd_type: TestCommandType::Get,
        }
    }
    pub(super) fn new_put(id: u32, keys: Vec<u32>, value: u32) -> Self {
        Self {
            id: ProposeId::new(id.to_string()),
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

impl ConflictCheck for u32 {
    fn is_conflict(&self, other: &Self) -> bool {
        self == other
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
pub(super) struct TestCE {
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
    pub(super) fn new(
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
