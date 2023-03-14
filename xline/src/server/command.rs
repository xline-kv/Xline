use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use curp::{
    cmd::{
        Command as CurpCommand, CommandExecutor as CurpCommandExecutor, ConflictCheck, ProposeId,
    },
    LogIndex,
};
use engine::WriteOperation;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    rpc::{RequestBackend, RequestWithToken, RequestWrapper, ResponseWrapper},
    storage::{storage_api::StorageApi, AuthStore, ExecuteError, KvStore, LeaseStore},
};

/// Meta table name
pub(crate) const META_TABLE: &str = "meta";
/// Key of applied index
const APPLIED_INDEX_KEY: &str = "applied_index";

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
        if range_end == ONE_KEY {
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
    /// New `KeyRange`
    pub(crate) fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

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
                // if other.start_bound() is Unbounded, program cannot enter this branch
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
                // if self.start_bound() is Unbounded, program cannot enter this branch
                // KeyRange::start_bound() cannot be Excluded
                _ => unreachable!("self.start_bound() should be Include"),
            }
        }
    }

    /// Check if `KeyRange` contains a key
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.contains(key)
    }

    /// Check if `KeyRange` contains another `KeyRange`
    pub(crate) fn contains_range(&self, other: &Self) -> bool {
        if other.end.is_empty() {
            self.contains_key(other.start.as_slice())
        } else {
            let s1_lt_s2 = match (self.start_bound(), other.start_bound()) {
                (Bound::Included(s1), Bound::Included(s2)) => s1 <= s2,
                (Bound::Unbounded, _) => true,
                (_, Bound::Unbounded) => false,
                _ => unreachable!("KeyRange::start_bound() cannot be Excluded"),
            };
            let e1_gt_e2 = match (self.end_bound(), other.end_bound()) {
                (Bound::Excluded(e1) | Bound::Included(e1), Bound::Excluded(e2))
                | (Bound::Included(e1), Bound::Included(e2)) => e1 >= e2,
                (Bound::Excluded(e1), Bound::Included(e2)) => e1 > e2,
                (Bound::Unbounded, _) => true,
                (_, Bound::Unbounded) => false,
            };
            s1_lt_s2 && e1_gt_e2
        }
    }

    /// Get end of range with prefix
    /// User will provide a start key when prefix is true, we need calculate the end key of `KeyRange`
    #[allow(clippy::indexing_slicing)] // end[i] is always valid
    pub(crate) fn get_prefix(key: &[u8]) -> Vec<u8> {
        let mut end = key.to_vec();
        for i in (0..key.len()).rev() {
            if key[i] < 0xFF {
                end[i] = end[i].wrapping_add(1);
                end.truncate(i.wrapping_add(1));
                return end;
            }
        }
        // next prefix does not exist (e.g., 0xffff);
        vec![0]
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
pub(crate) struct CommandExecutor<S>
where
    S: StorageApi,
{
    /// Kv Storage
    kv_storage: Arc<KvStore<S>>,
    /// Auth Storage
    auth_storage: Arc<AuthStore<S>>,
    /// Lease Storage
    lease_storage: Arc<LeaseStore<S>>,
    /// persistent storage
    persistent: Arc<S>,
}

impl<S> CommandExecutor<S>
where
    S: StorageApi,
{
    /// New `CommandExecutor`
    pub(crate) fn new(
        kv_storage: Arc<KvStore<S>>,
        auth_storage: Arc<AuthStore<S>>,
        lease_storage: Arc<LeaseStore<S>>,
        persistent: Arc<S>,
    ) -> Self {
        Self {
            kv_storage,
            auth_storage,
            lease_storage,
            persistent,
        }
    }
}

#[async_trait::async_trait]
impl<S> CurpCommandExecutor<Command> for CommandExecutor<S>
where
    S: StorageApi + Clone,
{
    type Error = ExecuteError;

    async fn execute(&self, cmd: &Command) -> Result<CommandResponse, ExecuteError> {
        let wrapper = cmd.request();
        self.auth_storage.check_permission(wrapper).await?;
        match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.execute(wrapper),
            RequestBackend::Auth => self.auth_storage.execute(wrapper),
            RequestBackend::Lease => self.lease_storage.execute(wrapper),
        }
    }

    async fn after_sync(
        &self,
        cmd: &Command,
        index: LogIndex,
    ) -> Result<SyncResponse, ExecuteError> {
        let wrapper = cmd.request();
        self.auth_storage.check_permission(wrapper).await?;
        let mut wr_ops = vec![WriteOperation::new_put(
            META_TABLE,
            APPLIED_INDEX_KEY,
            index.to_le_bytes(),
        )];
        let (res, mut ops) = match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.after_sync(wrapper).await?,
            RequestBackend::Auth => self.auth_storage.after_sync(wrapper)?,
            RequestBackend::Lease => self.lease_storage.after_sync(wrapper).await?,
        };
        wr_ops.append(&mut ops);
        self.persistent.write_batch(wr_ops, false)?;
        Ok(res)
    }

    // TODO
    async fn reset(&self) {}

    fn last_applied(&self) -> Result<LogIndex, ExecuteError> {
        let Some(index_bytes) = self.persistent.get_value("meta", "applied_index")? else {
            return Ok(0);
        };
        let buf: [u8; 8] = index_bytes
            .try_into()
            .unwrap_or_else(|e| panic!("cannot decode index from backend, {e:?}"));
        Ok(u64::from_le_bytes(buf))
    }
}

/// Command to run consensus protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Command {
    /// Keys of request
    keys: Vec<KeyRange>,
    /// Request data
    request: RequestWithToken,
    /// Propose id
    id: ProposeId,
}

impl ConflictCheck for Command {
    fn is_conflict(&self, other: &Self) -> bool {
        if self.id == other.id {
            return true;
        }
        let this_req = &self.request.request;
        let other_req = &other.request.request;
        // auth read request will not conflict with any request except the auth write request
        if (this_req.is_auth_read_request() && other_req.is_auth_read_request())
            || (this_req.is_kv_request() && other_req.is_auth_read_request())
            || (this_req.is_auth_read_request() && other_req.is_kv_request())
        {
            return false;
        }
        // any two requests that don't meet the above conditions will conflict with each other
        // because the auth write request will make all previous token invalid
        if (this_req.is_auth_request()) || (other_req.is_auth_request()) {
            return true;
        }

        if (this_req.is_lease_request()) && (other_req.is_lease_request()) {
            #[allow(clippy::wildcard_enum_match_arm)]
            let lease_id1 = match *this_req {
                RequestWrapper::LeaseGrantRequest(ref req) => req.id,
                RequestWrapper::LeaseRevokeRequest(ref req) => req.id,
                _ => unreachable!("other request can not in this match"),
            };
            #[allow(clippy::wildcard_enum_match_arm)]
            let lease_id2 = match *other_req {
                RequestWrapper::LeaseGrantRequest(ref req) => req.id,
                RequestWrapper::LeaseRevokeRequest(ref req) => req.id,
                _ => unreachable!("other request can not in this match"),
            };
            if lease_id1 == lease_id2 {
                return true;
            }
        }

        self.keys()
            .iter()
            .cartesian_product(other.keys().iter())
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
    pub(crate) fn new(keys: Vec<KeyRange>, request: RequestWithToken, id: ProposeId) -> Self {
        Self { keys, request, id }
    }

    /// get request
    pub(crate) fn request(&self) -> &RequestWithToken {
        &self.request
    }
}

/// Command to run consensus protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CommandResponse {
    /// Response data
    response: ResponseWrapper,
}

impl CommandResponse {
    /// New `ResponseOp` from `CommandResponse`
    pub(crate) fn new(response: ResponseWrapper) -> Self {
        Self { response }
    }

    /// Decode `CommandResponse` and get `ResponseOp`
    pub(crate) fn decode(self) -> ResponseWrapper {
        self.response
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
