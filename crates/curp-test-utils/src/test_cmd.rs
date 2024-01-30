use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use curp_external_api::{
    cmd::{Command, CommandExecutor, ConflictCheck, PbCodec},
    InflightId, LogIndex,
};
use engine::{Engine, EngineType, Snapshot, SnapshotApi, StorageEngine, WriteOperation};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::mpsc, time::sleep};
use tracing::debug;
use utils::config::EngineConfig;

use crate::{META_TABLE, REVISION_TABLE, TEST_TABLE};

pub(crate) const APPLIED_INDEX_KEY: &str = "applied_index";
pub(crate) const LAST_REVISION_KEY: &str = "last_revision";

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteError(pub String);

impl Display for ExecuteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// The `ExecuteError` is only for internal use, so we do not have to serialize it to protobuf format
impl PbCodec for ExecuteError {
    fn encode(&self) -> Vec<u8> {
        self.0.clone().into_bytes()
    }

    fn decode(buf: &[u8]) -> Result<Self, curp_external_api::cmd::PbSerializeError> {
        Ok(ExecuteError(String::from_utf8_lossy(buf).into()))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TestCommand {
    pub keys: Vec<u32>,
    pub exe_dur: Duration,
    pub as_dur: Duration,
    pub exe_should_fail: bool,
    pub as_should_fail: bool,
    pub cmd_type: TestCommandType,
}

impl Default for TestCommand {
    fn default() -> Self {
        Self {
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
pub enum TestCommandType {
    Get,
    Put(u32),
}

/// value and revision
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TestCommandResult {
    pub values: Vec<u32>,
    pub revisions: Vec<i64>,
}

impl TestCommandResult {
    pub fn new(values: Vec<u32>, revisions: Vec<i64>) -> Self {
        Self { values, revisions }
    }
}

// The `TestCommandResult` is only for internal use, so we do not have to serialize it to protobuf format
impl PbCodec for TestCommandResult {
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_else(|_| {
            unreachable!("test cmd result should always be successfully serialized")
        })
    }

    fn decode(buf: &[u8]) -> Result<Self, curp_external_api::cmd::PbSerializeError> {
        Ok(bincode::deserialize(buf).unwrap_or_else(|_| {
            unreachable!("test cmd result should always be successfully serialized")
        }))
    }
}

impl TestCommand {
    pub fn new_get(keys: Vec<u32>) -> Self {
        Self {
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

/// LogIndex used in Command::ASR
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogIndexResult(LogIndex);

impl From<LogIndex> for LogIndexResult {
    fn from(value: LogIndex) -> Self {
        Self(value)
    }
}

impl From<LogIndexResult> for LogIndex {
    fn from(value: LogIndexResult) -> Self {
        value.0
    }
}

// The `TestCommandResult` is only for internal use, so we do not have to serialize it to protobuf format
impl PbCodec for LogIndexResult {
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_else(|_| {
            unreachable!("test cmd result should always be successfully serialized")
        })
    }

    fn decode(buf: &[u8]) -> Result<Self, curp_external_api::cmd::PbSerializeError> {
        Ok(bincode::deserialize(buf).unwrap_or_else(|_| {
            unreachable!("test cmd result should always be successfully serialized")
        }))
    }
}

impl Command for TestCommand {
    type Error = ExecuteError;

    type K = u32;

    type PR = i64;

    type ER = TestCommandResult;

    type ASR = LogIndexResult;

    fn keys(&self) -> &[Self::K] {
        &self.keys
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

// The `TestCommand` is only for internal use, so we do not have to serialize it to protobuf format
impl PbCodec for TestCommand {
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(self)
            .unwrap_or_else(|_| unreachable!("test cmd should always be successfully serialized"))
    }

    fn decode(buf: &[u8]) -> Result<Self, curp_external_api::cmd::PbSerializeError> {
        Ok(bincode::deserialize(buf)
            .unwrap_or_else(|_| unreachable!("test cmd should always be successfully serialized")))
    }
}

#[derive(Debug, Clone)]
pub struct TestCE {
    server_name: String,
    revision: Arc<AtomicI64>,
    pub store: Arc<Engine>,
    exe_sender: mpsc::UnboundedSender<(TestCommand, TestCommandResult)>,
    after_sync_sender: mpsc::UnboundedSender<(TestCommand, LogIndex)>,
}

#[async_trait]
impl CommandExecutor<TestCommand> for TestCE {
    fn prepare(
        &self,
        cmd: &TestCommand,
    ) -> Result<<TestCommand as Command>::PR, <TestCommand as Command>::Error> {
        let rev = if let TestCommandType::Put(_) = cmd.cmd_type {
            let rev = self.revision.fetch_add(1, Ordering::Relaxed);
            let wr_ops = vec![WriteOperation::new_put(
                META_TABLE,
                LAST_REVISION_KEY.into(),
                rev.to_le_bytes().to_vec(),
            )];
            self.store
                .write_batch(wr_ops, true)
                .map_err(|e| ExecuteError(e.to_string()))?;
            rev
        } else {
            -1
        };
        Ok(rev)
    }

    async fn execute(
        &self,
        cmd: &TestCommand,
    ) -> Result<<TestCommand as Command>::ER, <TestCommand as Command>::Error> {
        sleep(cmd.exe_dur).await;
        if cmd.exe_should_fail {
            return Err(ExecuteError("fail".to_owned()));
        }

        debug!("{} execute cmd({:?})", self.server_name, cmd);

        let keys = cmd
            .keys
            .iter()
            .map(|k| k.to_be_bytes().to_vec())
            .collect_vec();
        let result: TestCommandResult = match cmd.cmd_type {
            TestCommandType::Get => {
                let value = self
                    .store
                    .get_multi(TEST_TABLE, &keys)
                    .map_err(|e| ExecuteError(e.to_string()))?
                    .into_iter()
                    .flatten()
                    .map(|v| u32::from_be_bytes(v.as_slice().try_into().unwrap()))
                    .collect();
                let revision = self
                    .store
                    .get_multi(REVISION_TABLE, &keys)
                    .map_err(|e| ExecuteError(e.to_string()))?
                    .into_iter()
                    .flatten()
                    .map(|v| i64::from_be_bytes(v.as_slice().try_into().unwrap()))
                    .collect_vec();
                TestCommandResult::new(value, revision)
            }
            TestCommandType::Put(_) => TestCommandResult::default(),
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
        revision: <TestCommand as Command>::PR,
    ) -> Result<<TestCommand as Command>::ASR, <TestCommand as Command>::Error> {
        sleep(cmd.as_dur).await;
        if cmd.as_should_fail {
            return Err(ExecuteError("fail".to_owned()));
        }
        self.after_sync_sender
            .send((cmd.clone(), index))
            .expect("failed to send after sync msg");
        let mut wr_ops = vec![WriteOperation::new_put(
            META_TABLE,
            APPLIED_INDEX_KEY.into(),
            index.to_le_bytes().to_vec(),
        )];
        if let TestCommandType::Put(v) = cmd.cmd_type {
            debug!("cmd {:?}-{:?} revision is {}", cmd.cmd_type, cmd, revision);
            let value = v.to_be_bytes().to_vec();
            let keys = cmd
                .keys
                .iter()
                .map(|k| k.to_be_bytes().to_vec())
                .collect_vec();
            wr_ops.extend(
                keys.clone()
                    .into_iter()
                    .map(|key| WriteOperation::new_put(TEST_TABLE, key, value.clone()))
                    .chain(keys.into_iter().map(|key| {
                        WriteOperation::new_put(
                            REVISION_TABLE,
                            key,
                            revision.to_be_bytes().to_vec(),
                        )
                    })),
            );
            self.store
                .write_batch(wr_ops, true)
                .map_err(|e| ExecuteError(e.to_string()))?;
        }
        debug!(
            "{} after sync cmd({:?} - {:?}), index: {index}",
            self.server_name, cmd.cmd_type, cmd
        );
        Ok(index.into())
    }

    fn set_last_applied(&self, index: LogIndex) -> Result<(), <TestCommand as Command>::Error> {
        let ops = vec![WriteOperation::new_put(
            META_TABLE,
            APPLIED_INDEX_KEY.into(),
            index.to_le_bytes().to_vec(),
        )];
        self.store
            .write_batch(ops, true)
            .map_err(|e| ExecuteError(e.to_string()))?;
        Ok(())
    }

    fn last_applied(&self) -> Result<LogIndex, ExecuteError> {
        let Some(index) = self
            .store
            .get(META_TABLE, APPLIED_INDEX_KEY)
            .map_err(|e| ExecuteError(e.to_string()))? else {
            return Ok(0);
        };
        let index = LogIndex::from_le_bytes(index.as_slice().try_into().unwrap());
        Ok(index)
    }

    async fn snapshot(&self) -> Result<Snapshot, <TestCommand as Command>::Error> {
        self.store
            .get_snapshot("", &[TEST_TABLE])
            .map_err(|e| ExecuteError(e.to_string()))
    }

    async fn reset(
        &self,
        snapshot: Option<(Snapshot, LogIndex)>,
    ) -> Result<(), <TestCommand as Command>::Error> {
        let Some((mut snapshot, index)) = snapshot else {
            let ops = vec![WriteOperation::new_delete_range(TEST_TABLE, &[], &[0xff]),WriteOperation::new_delete(META_TABLE, APPLIED_INDEX_KEY.as_ref())];
            self.store
                .write_batch(ops, true)
                .map_err(|e| ExecuteError(e.to_string()))?;
            return Ok(());
        };
        let ops = vec![WriteOperation::new_put(
            META_TABLE,
            APPLIED_INDEX_KEY.into(),
            index.to_le_bytes().to_vec(),
        )];
        self.store
            .write_batch(ops, true)
            .map_err(|e| ExecuteError(e.to_string()))?;
        snapshot.rewind().unwrap();
        self.store
            .apply_snapshot(snapshot, &[TEST_TABLE])
            .await
            .unwrap();
        Ok(())
    }

    fn trigger(&self, _id: InflightId, _index: LogIndex) {}
}

impl TestCE {
    pub fn new(
        server_name: String,
        exe_sender: mpsc::UnboundedSender<(TestCommand, TestCommandResult)>,
        after_sync_sender: mpsc::UnboundedSender<(TestCommand, LogIndex)>,
        engine_cfg: EngineConfig,
    ) -> Self {
        let engine_type = match engine_cfg {
            EngineConfig::Memory => EngineType::Memory,
            EngineConfig::RocksDB(path) => EngineType::Rocks(path),
            _ => unreachable!("Not supported storage type"),
        };
        let store =
            Arc::new(Engine::new(engine_type, &[TEST_TABLE, REVISION_TABLE, META_TABLE]).unwrap());
        let rev = store
            .get(META_TABLE, LAST_REVISION_KEY)
            .unwrap()
            .map(|r| i64::from_le_bytes(r.as_slice().try_into().unwrap()))
            .unwrap_or(0);
        Self {
            server_name,
            revision: Arc::new(AtomicI64::new(rev + 1)),
            store,
            exe_sender,
            after_sync_sender,
        }
    }
}
