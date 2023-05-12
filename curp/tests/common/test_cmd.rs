use std::{
    collections::HashMap,
    fmt::Display,
    io::{self, Cursor, Read, SeekFrom, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use clippy_utilities::NumericCast;
use curp::{
    cmd::{Command, CommandExecutor, ConflictCheck, ProposeId},
    LogIndex,
};
use engine::{Engine, EngineType, Snapshot, SnapshotApi, StorageEngine, WriteOperation};
use itertools::Itertools;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::mpsc, time::sleep};
use tracing::debug;

use crate::common::TEST_TABLE;

static NEXT_ID: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

#[derive(Error, Debug, Clone)]
pub struct ExecuteError(String);

impl Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TestCommand {
    id: ProposeId,
    keys: Vec<u32>,
    exe_dur: Duration,
    as_dur: Duration,
    exe_should_fail: bool,
    as_should_fail: bool,
    cmd_type: TestCommandType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum TestCommandType {
    Get,
    Put(u32),
}

pub type TestCommandResult = Vec<u32>;

impl TestCommand {
    pub fn new_get(keys: Vec<u32>) -> Self {
        Self {
            id: ProposeId::new(next_id().to_string()),
            keys,
            exe_dur: Duration::ZERO,
            as_dur: Duration::ZERO,
            exe_should_fail: false,
            as_should_fail: false,
            cmd_type: TestCommandType::Get,
        }
    }

    pub fn new_put(keys: Vec<u32>, value: u32) -> Self {
        Self {
            id: ProposeId::new(next_id().to_string()),
            keys,
            exe_dur: Duration::ZERO,
            as_dur: Duration::ZERO,
            exe_should_fail: false,
            as_should_fail: false,
            cmd_type: TestCommandType::Put(value),
        }
    }

    pub fn set_exe_dur(mut self, dur: Duration) -> Self {
        self.exe_dur = dur;
        self
    }

    pub fn set_as_dur(mut self, dur: Duration) -> Self {
        self.as_dur = dur;
        self
    }

    pub fn set_exe_should_fail(mut self) -> Self {
        self.exe_should_fail = true;
        self
    }

    pub fn set_asr_should_fail(mut self) -> Self {
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
pub struct TestCE {
    server_id: String,
    last_applied: Arc<AtomicU64>,
    pub store: Arc<Engine>,
    exe_sender: mpsc::UnboundedSender<(TestCommand, TestCommandResult)>,
    after_sync_sender: mpsc::UnboundedSender<(TestCommand, LogIndex)>,
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestCE {
    type Error = ExecuteError;

    fn prepare(&self, _cmd: &TestCommand) -> Result<i64, Self::Error> {
        Ok(0)
    }

    async fn execute(&self, cmd: &TestCommand) -> Result<TestCommandResult, Self::Error> {
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
        need_run: bool,
        _prepare_res: Option<<TestCommand as Command>::PR>,
    ) -> Result<LogIndex, ExecuteError> {
        sleep(cmd.as_dur).await;
        if cmd.as_should_fail {
            return Err(ExecuteError("fail".to_owned()));
        }
        if need_run {
            self.after_sync_sender
                .send((cmd.clone(), index))
                .expect("failed to send after sync msg");
        }
        self.last_applied.store(index, Ordering::Relaxed);
        debug!(
            "{} after sync cmd({}), index: {index}",
            self.server_id,
            cmd.id()
        );
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
    pub fn new(
        server_id: String,
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
