use std::{
    ops::{
        Bound::{self, Excluded, Included},
        RangeBounds,
    },
    sync::Arc,
};

use curp::{
    cmd::{
        Command as CurpCommand, CommandExecutor as CurpCommandExecutor, ConflictCheck, ProposeId,
    },
    error, LogIndex,
};
use itertools::Itertools;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::rpc::ResponseOp;
use crate::storage::KvStore;

/// Range end to get all keys
const ALL_KEYS: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];

/// Key Range for Command
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub(crate) struct KeyRange {
    /// Start of range
    pub(crate) start: Vec<u8>,
    /// End of range
    pub(crate) end: Vec<u8>,
}

impl KeyRange {
    /// Return if `KeyRange` is conflicted with another
    pub(crate) fn is_conflicted(&self, other: &Self) -> bool {
        // All keys conflict with any key range
        if self.end == ALL_KEYS || other.end == ALL_KEYS {
            true
        } else {
            match self.end.as_slice() {
                ONE_KEY => match other.end.as_slice() {
                    ONE_KEY => self.start == other.start,
                    _ => (self.start >= other.start) && (self.start < other.end),
                },
                _ => match other.end.as_slice() {
                    ONE_KEY => (other.start >= self.start) && (other.start < self.end),
                    _ => (self.start < other.end) && (self.end > other.start),
                },
            }
        }
    }

    /// Check if `KeyRange` contains a key
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        match self.end.as_slice() {
            ONE_KEY => self.start == key,
            ALL_KEYS => true,
            _ => (key >= self.start.as_slice()) && (key < self.end.as_slice()),
        }
    }
}

impl RangeBounds<Vec<u8>> for KeyRange {
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        Included(&self.start)
    }
    fn end_bound(&self) -> Bound<&Vec<u8>> {
        Excluded(&self.end)
    }
}

/// Command Executor
#[derive(Debug, Clone)]
pub(crate) struct CommandExecutor {
    /// Kv Storage
    storage: Arc<KvStore>,
}

impl CommandExecutor {
    /// New `CommandExecutor`
    pub(crate) fn new(storage: Arc<KvStore>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl CurpCommandExecutor<Command> for CommandExecutor {
    async fn execute(&self, cmd: &Command) -> Result<CommandResponse, error::ExecuteError> {
        let receiver = self.storage.send_req(cmd.clone()).await;
        receiver
            .await
            .or_else(|_| panic!("Failed to receive response from storage"))
    }

    async fn after_sync(
        &self,
        cmd: &Command,
        _index: LogIndex,
    ) -> Result<SyncResponse, error::ExecuteError> {
        let receiver = self.storage.send_sync(cmd.id().clone()).await;
        receiver
            .await
            .or_else(|_| panic!("Failed to receive response from storage"))
    }
}

/// Command to run consensus protocal
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Command {
    /// Keys of request
    keys: Vec<KeyRange>,
    /// Encoded request data
    request: Vec<u8>,
    /// Propose id
    id: ProposeId,
}

impl ConflictCheck for Command {
    fn is_conflict(&self, other: &Self) -> bool {
        let this_keys = self.keys();
        let other_keys = other.keys();
        this_keys
            .iter()
            .cartesian_product(other_keys.iter())
            .any(|(k1, k2)| k1.is_conflicted(k2))
    }
}

impl ConflictCheck for KeyRange {
    fn is_conflict(&self, other: &Self) -> bool {
        self.is_conflicted(other)
    }
}

impl Command {
    /// New `Command`
    pub(crate) fn new(keys: Vec<KeyRange>, request: Vec<u8>, id: ProposeId) -> Self {
        Self { keys, request, id }
    }

    /// Consume `Command` and get ownership of each field
    pub(crate) fn unpack(self) -> (Vec<KeyRange>, Vec<u8>, ProposeId) {
        let Self { keys, request, id } = self;
        (keys, request, id)
    }
}

/// Command to run consensus protocal
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CommandResponse {
    /// Encoded response data
    response: Vec<u8>,
}

impl CommandResponse {
    /// New `ResponseOp` from `CommandResponse`
    pub(crate) fn new(res: &ResponseOp) -> Self {
        Self {
            response: res.encode_to_vec(),
        }
    }

    /// Decode `CommandResponse` and get `ResponseOp`
    pub(crate) fn decode(&self) -> ResponseOp {
        ResponseOp::decode(self.response.as_slice())
            .unwrap_or_else(|e| panic!("Failed to decode CommandResponse, error is {:?}", e))
    }
}

/// Sync Request
#[derive(Debug)]
pub(crate) struct SyncRequest {
    /// Propose id to sync
    propose_id: ProposeId,
    /// Command response sender
    res_sender: oneshot::Sender<SyncResponse>,
}

impl SyncRequest {
    /// New `SyncRequest`
    pub(crate) fn new(propose_id: ProposeId) -> (Self, oneshot::Receiver<SyncResponse>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                propose_id,
                res_sender: tx,
            },
            rx,
        )
    }

    /// Consume `ExecutionRequest` and get ownership of each field
    pub(crate) fn unpack(self) -> (ProposeId, oneshot::Sender<SyncResponse>) {
        let Self {
            propose_id,
            res_sender,
        } = self;
        (propose_id, res_sender)
    }
}
/// Sync Response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SyncResponse {
    /// Revision of this request
    revision: i64,
}
impl SyncResponse {
    /// New `SyncRequest`
    pub(crate) fn new(revision: i64) -> Self {
        Self { revision }
    }

    /// Get revision field
    pub(crate) fn revision(&self) -> i64 {
        self.revision
    }
}

/// Execution Request
#[derive(Debug)]
pub(crate) struct ExecutionRequest {
    /// Command to execute
    cmd: Command,
    /// Command response sender
    res_sender: oneshot::Sender<CommandResponse>,
}

impl ExecutionRequest {
    /// New `ExectionRequest`
    pub(crate) fn new(cmd: Command) -> (Self, oneshot::Receiver<CommandResponse>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cmd,
                res_sender: tx,
            },
            rx,
        )
    }

    /// Consume `ExecutionRequest` and get ownership of each field
    pub(crate) fn unpack(self) -> (Command, oneshot::Sender<CommandResponse>) {
        let Self { cmd, res_sender } = self;
        (cmd, res_sender)
    }
}

#[async_trait::async_trait]
impl CurpCommand for Command {
    type K = KeyRange;
    type ER = CommandResponse;
    type ASR = SyncResponse;

    fn keys(&self) -> &[Self::K] {
        self.keys.as_slice()
    }

    fn id(&self) -> &ProposeId {
        &self.id
    }
}
