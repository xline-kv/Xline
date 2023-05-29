use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
use engine::{Engine, EngineType, Snapshot, SnapshotApi, StorageEngine, WriteOperation};
use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::mpsc, time::sleep};
use tracing::debug;

use crate::{
    cmd::{Command, CommandExecutor, ConflictCheck, ProposeId},
    LogIndex, ServerId,
};

pub(crate) const TEST_TABLE: &str = "test";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct TestCommand {
    id: ProposeId,
    keys: Vec<u32>,
    exe_dur: Duration,
    as_dur: Duration,
    exe_should_fail: bool,
    as_should_fail: bool,
    cmd_type: TestCommandType,
}

impl Default for TestCommand {
    fn default() -> Self {
        Self {
            id: next_id(),
            keys: vec![1],
            exe_dur: Duration::ZERO,
            as_dur: Duration::ZERO,
            exe_should_fail: false,
            as_should_fail: false,
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
            id: next_id(),
            keys,
            exe_dur: Duration::ZERO,
            as_dur: Duration::ZERO,
            exe_should_fail: false,
            as_should_fail: false,
            cmd_type: TestCommandType::Get,
        }
    }
    pub(crate) fn new_put(keys: Vec<u32>, value: u32) -> Self {
        Self {
            id: next_id(),
            keys,
            exe_dur: Duration::ZERO,
            as_dur: Duration::ZERO,
            exe_should_fail: false,
            as_should_fail: false,
            cmd_type: TestCommandType::Put(value),
        }
    }
    pub(crate) fn set_exe_dur(mut self, dur: Duration) -> Self {
        self.exe_dur = dur;
        self
    }
    pub(crate) fn set_as_dur(mut self, dur: Duration) -> Self {
        self.as_dur = dur;
        self
    }
    pub(crate) fn set_exe_should_fail(mut self) -> Self {
        self.exe_should_fail = true;
        self
    }
    pub(crate) fn set_asr_should_fail(mut self) -> Self {
        self.as_should_fail = true;
        self
    }
}

impl Command for TestCommand {
    type K = u32;

    type PR = i64;

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
        if self.id == other.id {
            return true;
        }

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
    last_applied: Arc<AtomicU64>,
    pub(crate) store: Arc<Engine>,
    exe_sender: mpsc::UnboundedSender<(TestCommand, TestCommandResult)>,
    after_sync_sender: mpsc::UnboundedSender<(TestCommand, LogIndex)>,
}

#[derive(Error, Debug, Clone)]
pub(crate) struct ExecuteError(String);

impl Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestCE {
    type Error = ExecuteError;

    fn prepare(
        &self,
        _cmd: &TestCommand,
        _index: LogIndex,
    ) -> Result<<TestCommand as Command>::PR, Self::Error> {
        Ok(0)
    }

    async fn execute(
        &self,
        cmd: &TestCommand,
        _index: LogIndex,
    ) -> Result<<TestCommand as Command>::ER, Self::Error> {
        sleep(cmd.exe_dur).await;
        if cmd.exe_should_fail {
            return Err(ExecuteError("fail".to_owned()));
        }

        debug!("{} execute cmd({})", self.server_id, cmd.id());

        let keys = cmd
            .keys
            .iter()
            .map(|k| k.to_be_bytes().to_vec())
            .collect_vec();
        let result: TestCommandResult = match cmd.cmd_type {
            TestCommandType::Get => self
                .store
                .get_multi(TEST_TABLE, &keys)
                .map_err(|e| ExecuteError(e.to_string()))?
                .into_iter()
                .flatten()
                .map(|v| u32::from_be_bytes(v.as_slice().try_into().unwrap()))
                .collect(),
            TestCommandType::Put(ref v) => {
                let value = v.to_be_bytes().to_vec();
                let wr_ops = keys
                    .into_iter()
                    .map(|key| WriteOperation::new_put(TEST_TABLE, key, value.clone()))
                    .collect();
                self.store
                    .write_batch(wr_ops, true)
                    .map_err(|e| ExecuteError(e.to_string()))?;
                TestCommandResult::default()
            }
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
        _pre_exe_res: Option<<TestCommand as Command>::PR>,
    ) -> Result<<TestCommand as Command>::ASR, Self::Error> {
        sleep(cmd.as_dur).await;
        if cmd.as_should_fail {
            return Err(ExecuteError("fail".to_owned()));
        }
        self.last_applied
            .store(index.numeric_cast(), Ordering::Relaxed);
        self.after_sync_sender
            .send((cmd.clone(), index))
            .expect("failed to send after sync msg");
        Ok(index)
    }

    fn last_applied(&self) -> Result<LogIndex, ExecuteError> {
        Ok(self.last_applied.load(Ordering::Relaxed))
    }

    async fn snapshot(&self) -> Result<Snapshot, Self::Error> {
        self.store
            .get_snapshot("", &[TEST_TABLE])
            .map_err(|e| ExecuteError(e.to_string()))
    }

    async fn reset(&self, snapshot: Option<(Snapshot, LogIndex)>) -> Result<(), Self::Error> {
        let Some((mut snapshot, index)) = snapshot else {
            self.last_applied.store(0, Ordering::Relaxed);
            let ops = vec![WriteOperation::new_delete_range(TEST_TABLE, &[], &[0xff])];
            self.store
                .write_batch(ops, true)
                .map_err(|e| ExecuteError(e.to_string()))?;
            return Ok(());
        };
        self.last_applied
            .store(index.numeric_cast(), Ordering::Relaxed);
        snapshot.rewind().unwrap();
        self.store
            .apply_snapshot(snapshot, &[TEST_TABLE])
            .await
            .unwrap();
        Ok(())
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
            last_applied: Arc::new(AtomicU64::new(0)),
            store: Arc::new(Engine::new(EngineType::Memory, &[TEST_TABLE]).unwrap()),
            exe_sender,
            after_sync_sender,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TestCESimple {
    server_id: ServerId,
    last_applied: Arc<AtomicU64>,
    store: Arc<Engine>,
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestCESimple {
    type Error = ExecuteError;

    fn prepare(
        &self,
        _cmd: &TestCommand,
        _index: LogIndex,
    ) -> Result<<TestCommand as Command>::PR, Self::Error> {
        Ok(0)
    }

    async fn execute(
        &self,
        cmd: &TestCommand,
        _index: LogIndex,
    ) -> Result<<TestCommand as Command>::ER, Self::Error> {
        sleep(cmd.exe_dur).await;
        if cmd.exe_should_fail {
            return Err(ExecuteError("fail".to_owned()));
        }

        debug!("{} execute cmd({})", self.server_id, cmd.id());

        let keys = cmd
            .keys
            .iter()
            .map(|k| k.to_be_bytes().to_vec())
            .collect_vec();
        let result: TestCommandResult = match cmd.cmd_type {
            TestCommandType::Get => self
                .store
                .get_multi(TEST_TABLE, &keys)
                .map_err(|e| ExecuteError(e.to_string()))?
                .into_iter()
                .flatten()
                .map(|v| u32::from_be_bytes(v.as_slice().try_into().unwrap()))
                .collect(),
            TestCommandType::Put(ref v) => {
                let value = v.to_be_bytes().to_vec();
                let wr_ops = keys
                    .into_iter()
                    .map(|key| WriteOperation::new_put(TEST_TABLE, key, value.clone()))
                    .collect();
                self.store
                    .write_batch(wr_ops, true)
                    .map_err(|e| ExecuteError(e.to_string()))?;
                TestCommandResult::default()
            }
        };
        Ok(result)
    }

    async fn after_sync(
        &self,
        cmd: &TestCommand,
        index: LogIndex,
        _pre_exe_res: Option<<TestCommand as Command>::PR>,
    ) -> Result<<TestCommand as Command>::ASR, Self::Error> {
        sleep(cmd.as_dur).await;
        if cmd.as_should_fail {
            return Err(ExecuteError("fail".to_owned()));
        }
        self.last_applied.store(index, Ordering::Relaxed);

        debug!("{} call cmd({}) after sync", self.server_id, cmd.id());
        Ok(index)
    }

    fn last_applied(&self) -> Result<LogIndex, ExecuteError> {
        Ok(self.last_applied.load(Ordering::Relaxed))
    }

    async fn snapshot(&self) -> Result<Snapshot, Self::Error> {
        self.store
            .get_snapshot("", &[TEST_TABLE])
            .map_err(|e| ExecuteError(e.to_string()))
    }

    async fn reset(&self, snapshot: Option<(Snapshot, LogIndex)>) -> Result<(), Self::Error> {
        let Some((mut snapshot, index)) = snapshot else {
            self.last_applied.store(0, Ordering::Relaxed);
            let ops = vec![WriteOperation::new_delete_range(TEST_TABLE, &[], &[0xff])];
            self.store
                .write_batch(ops, true)
                .map_err(|e| ExecuteError(e.to_string()))?;
            return Ok(());
        };
        self.last_applied
            .store(index.numeric_cast(), Ordering::Relaxed);
        snapshot.rewind().unwrap();
        self.store
            .apply_snapshot(snapshot, &[TEST_TABLE])
            .await
            .unwrap();
        Ok(())
    }
}

impl TestCESimple {
    pub(crate) fn new(server_id: ServerId) -> Self {
        Self {
            server_id,
            last_applied: Arc::new(AtomicU64::new(0)),
            store: Arc::new(Engine::new(EngineType::Memory, &[TEST_TABLE]).unwrap()),
        }
    }
}

static NEXT_ID: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));

fn next_id() -> ProposeId {
    ProposeId::new(NEXT_ID.fetch_add(1, Ordering::AcqRel).to_string())
}
