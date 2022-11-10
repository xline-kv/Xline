use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use curp::{
    cmd::{
        Command as CurpCommand, CommandExecutor as CurpCommandExecutor, ConflictCheck, ProposeId,
    },
    error::ExecuteError,
    LogIndex,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    rpc::{RequestWrapper, ResponseWrapper},
    storage::KvStore,
};

/// Range start and end to get all keys
const UNBOUNDED: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];

/// Type of `KeyRange`
pub(crate) enum RangeType {
    /// `KeyRange` contains only one key
    OneKey,
    /// `KeyRange` contains all keys
    AllKeys,
    /// `KeyRange` contains the keys in the range
    Range,
}

impl RangeType {
    /// Get `RangeType` by given `key` and `range_end`
    pub(crate) fn get_range_type(key: &[u8], range_end: &[u8]) -> Self {
        if key == ONE_KEY {
            RangeType::OneKey
        } else if key == UNBOUNDED && range_end == UNBOUNDED {
            RangeType::AllKeys
        } else {
            RangeType::Range
        }
    }
}

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
        // s1 < s2 ?
        if match (self.start_bound(), other.start_bound()) {
            (Bound::Included(s1), Bound::Included(s2)) => {
                if s1 == s2 {
                    return true;
                }
                s1 < s2
            }
            (Bound::Included(_), Bound::Unbounded) => false,
            (Bound::Unbounded, Bound::Included(_)) => true,
            (Bound::Unbounded, Bound::Unbounded) => return true,
            _ => unreachable!("KeyRange::start_bound() cannot be Excluded"),
        } {
            // s1 < s2
            // s2 < e1 ?
            match (other.start_bound(), self.end_bound()) {
                (Bound::Included(s2), Bound::Included(e1)) => s2 <= e1,
                (Bound::Included(s2), Bound::Excluded(e1)) => s2 < e1,
                (Bound::Included(_), Bound::Unbounded) => true,
                // if other.start_bound() is Unbounded, programe cannot enter this branch
                // KeyRange::start_bound() cannot be Excluded
                _ => unreachable!("other.start_bound() should be Include"),
            }
        } else {
            // s2 < s1
            // s1 < e2 ?
            match (self.start_bound(), other.end_bound()) {
                (Bound::Included(s1), Bound::Included(e2)) => s1 <= e2,
                (Bound::Included(s1), Bound::Excluded(e2)) => s1 < e2,
                (Bound::Included(_), Bound::Unbounded) => true,
                // if self.start_bound() is Unbounded, programe cannnot enter this branch
                // KeyRange::start_bound() cannot be Excluded
                _ => unreachable!("self.start_bound() should be Include"),
            }
        }
    }

    /// Check if `KeyRange` contains a key
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.contains(key)
    }
}

impl RangeBounds<[u8]> for KeyRange {
    fn start_bound(&self) -> Bound<&[u8]> {
        match self.start.as_slice() {
            UNBOUNDED => Bound::Unbounded,
            _ => Bound::Included(&self.start),
        }
    }
    fn end_bound(&self) -> Bound<&[u8]> {
        match self.end.as_slice() {
            UNBOUNDED => Bound::Unbounded,
            ONE_KEY => Bound::Included(&self.start),
            _ => Bound::Excluded(&self.end),
        }
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
    async fn execute(&self, cmd: &Command) -> Result<CommandResponse, ExecuteError> {
        let (_, request_data, id) = cmd.clone().unpack();
        let wrapper: RequestWrapper = bincode::deserialize(&request_data)
            .unwrap_or_else(|e| panic!("Failed to serialize RequestWrapper, error: {e}"));
        let receiver = self.storage.send_req(id, wrapper).await;
        receiver
            .await
            .unwrap_or_else(|_| panic!("Failed to receive response from storage"))
    }

    async fn after_sync(
        &self,
        cmd: &Command,
        _index: LogIndex,
    ) -> Result<SyncResponse, ExecuteError> {
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
    pub(crate) fn new(res: &ResponseWrapper) -> Self {
        Self {
            response: bincode::serialize(res)
                .unwrap_or_else(|e| panic!("Failed to serialize ResponseWrapper, error: {e}")),
        }
    }

    /// Decode `CommandResponse` and get `ResponseOp`
    pub(crate) fn decode(&self) -> ResponseWrapper {
        bincode::deserialize(&self.response)
            .unwrap_or_else(|e| panic!("Failed to deserialize ResponseWrapper, error: {e}"))
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
    /// Propose Id
    id: ProposeId,
    /// Request to execute
    req: RequestWrapper,
    /// Command response sender
    res_sender: oneshot::Sender<Result<CommandResponse, ExecuteError>>,
}

impl ExecutionRequest {
    /// New `ExectionRequest`
    pub(crate) fn new(
        id: ProposeId,
        req: RequestWrapper,
    ) -> (
        Self,
        oneshot::Receiver<Result<CommandResponse, ExecuteError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                id,
                req,
                res_sender: tx,
            },
            rx,
        )
    }

    /// Consume `ExecutionRequest` and get ownership of each field
    pub(crate) fn unpack(
        self,
    ) -> (
        ProposeId,
        RequestWrapper,
        oneshot::Sender<Result<CommandResponse, ExecuteError>>,
    ) {
        let Self {
            id,
            req,
            res_sender,
        } = self;
        (id, req, res_sender)
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
