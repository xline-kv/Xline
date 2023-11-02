use std::{
    collections::{HashSet, VecDeque},
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use curp::{
    cmd::{
        Command as CurpCommand, CommandExecutor as CurpCommandExecutor, ConflictCheck, PbCodec,
        PbSerializeError, ProposeId,
    },
    error::ClientError,
    LogIndex,
};
use engine::Snapshot;
use itertools::Itertools;
use prost::Message;
use serde::{Deserialize, Serialize};
use xlineapi::{PbCommand, PbCommandResponse, PbKeyRange, PbSyncResponse};

use super::barriers::{IdBarrier, IndexBarrier};
use crate::{
    revision_number::RevisionNumberGenerator,
    rpc::{Request, RequestBackend, RequestWithToken, RequestWrapper, ResponseWrapper},
    storage::{db::WriteOp, storage_api::StorageApi, AuthStore, ExecuteError, KvStore, LeaseStore},
};

/// Meta table name
pub(crate) const META_TABLE: &str = "meta";
/// Key of applied index
pub(crate) const APPLIED_INDEX_KEY: &str = "applied_index";

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
    #[inline]
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

/// Command Executor
#[derive(Debug)]
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
    /// Barrier for applied index
    index_barrier: Arc<IndexBarrier>,
    /// Barrier for propose id
    id_barrier: Arc<IdBarrier>,
    /// Revision Number generator for KV request and Lease request
    general_rev: Arc<RevisionNumberGenerator>,
    /// Revision Number generator for Auth request
    auth_rev: Arc<RevisionNumberGenerator>,
}

impl<S> CommandExecutor<S>
where
    S: StorageApi,
{
    /// New `CommandExecutor`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kv_storage: Arc<KvStore<S>>,
        auth_storage: Arc<AuthStore<S>>,
        lease_storage: Arc<LeaseStore<S>>,
        persistent: Arc<S>,
        index_barrier: Arc<IndexBarrier>,
        id_barrier: Arc<IdBarrier>,
        general_rev: Arc<RevisionNumberGenerator>,
        auth_rev: Arc<RevisionNumberGenerator>,
    ) -> Self {
        Self {
            kv_storage,
            auth_storage,
            lease_storage,
            persistent,
            index_barrier,
            id_barrier,
            general_rev,
            auth_rev,
        }
    }
}

#[async_trait::async_trait]
impl<S> CurpCommandExecutor<Command> for CommandExecutor<S>
where
    S: StorageApi,
{
    fn prepare(
        &self,
        cmd: &Command,
        index: LogIndex,
    ) -> Result<<Command as CurpCommand>::PR, <Command as CurpCommand>::Error> {
        let wrapper = cmd.request();
        if let Err(e) = self.auth_storage.check_permission(wrapper) {
            self.id_barrier.trigger(cmd.id());
            self.index_barrier.trigger(index);
            return Err(e);
        }
        let revision = match wrapper.request.backend() {
            RequestBackend::Auth => {
                if wrapper.request.skip_auth_revision() {
                    -1
                } else {
                    self.auth_rev.next()
                }
            }
            RequestBackend::Kv | RequestBackend::Lease => {
                if wrapper.request.skip_general_revision() {
                    -1
                } else {
                    self.general_rev.next()
                }
            }
        };
        Ok(revision)
    }

    async fn execute(
        &self,
        cmd: &Command,
        index: LogIndex,
    ) -> Result<<Command as CurpCommand>::ER, <Command as CurpCommand>::Error> {
        let wrapper = cmd.request();
        let res = match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.execute(wrapper),
            RequestBackend::Auth => self.auth_storage.execute(wrapper),
            RequestBackend::Lease => self.lease_storage.execute(wrapper),
        };
        match res {
            Ok(res) => Ok(res),
            Err(e) => {
                self.id_barrier.trigger(cmd.id());
                self.index_barrier.trigger(index);
                Err(e)
            }
        }
    }

    async fn after_sync(
        &self,
        cmd: &Command,
        index: LogIndex,
        revision: i64,
    ) -> Result<<Command as CurpCommand>::ASR, <Command as CurpCommand>::Error> {
        let mut ops = vec![WriteOp::PutAppliedIndex(index)];
        let wrapper = cmd.request();
        let (res, mut wr_ops) = match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.after_sync(wrapper, revision).await?,
            RequestBackend::Auth => self.auth_storage.after_sync(wrapper, revision)?,
            RequestBackend::Lease => self.lease_storage.after_sync(wrapper, revision).await?,
        };
        ops.append(&mut wr_ops);
        let key_revisions = self.persistent.flush_ops(ops)?;
        if !key_revisions.is_empty() {
            self.kv_storage.insert_index(key_revisions);
        }
        self.lease_storage.mark_lease_synced(&wrapper.request);
        self.id_barrier.trigger(cmd.id());
        self.index_barrier.trigger(index);
        Ok(res)
    }

    async fn reset(
        &self,
        snapshot: Option<(Snapshot, LogIndex)>,
    ) -> Result<(), <Command as CurpCommand>::Error> {
        let s = if let Some((snapshot, index)) = snapshot {
            _ = self
                .persistent
                .flush_ops(vec![WriteOp::PutAppliedIndex(index)])?;
            Some(snapshot)
        } else {
            None
        };
        self.persistent.reset(s).await
    }

    async fn snapshot(&self) -> Result<Snapshot, <Command as CurpCommand>::Error> {
        let path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
        self.persistent.get_snapshot(path)
    }

    fn set_last_applied(&self, index: LogIndex) -> Result<(), <Command as CurpCommand>::Error> {
        _ = self
            .persistent
            .flush_ops(vec![WriteOp::PutAppliedIndex(index)])?;
        Ok(())
    }

    fn last_applied(&self) -> Result<LogIndex, <Command as CurpCommand>::Error> {
        let Some(index_bytes) = self.persistent.get_value(META_TABLE, APPLIED_INDEX_KEY)? else {
            return Ok(0);
        };
        let buf: [u8; 8] = index_bytes
            .try_into()
            .unwrap_or_else(|e| panic!("cannot decode index from backend, {e:?}"));
        Ok(u64::from_le_bytes(buf))
    }
}

/// Generate `Command` proposal from `Request`
pub(super) fn command_from_request_wrapper<S>(
    propose_id: ProposeId,
    wrapper: RequestWithToken,
    lease_storage: Option<&LeaseStore<S>>,
) -> Command
where
    S: StorageApi,
{
    #[allow(clippy::wildcard_enum_match_arm)]
    let keys = match wrapper.request {
        RequestWrapper::RangeRequest(ref req) => {
            vec![KeyRange::new(req.key.as_slice(), req.range_end.as_slice())]
        }
        RequestWrapper::PutRequest(ref req) => vec![KeyRange::new_one_key(req.key.as_slice())],
        RequestWrapper::DeleteRangeRequest(ref req) => {
            vec![KeyRange::new(req.key.as_slice(), req.range_end.as_slice())]
        }
        RequestWrapper::TxnRequest(ref req) => req
            .compare
            .iter()
            .map(|cmp| KeyRange::new(cmp.key.as_slice(), cmp.range_end.as_slice()))
            .collect(),
        RequestWrapper::LeaseRevokeRequest(ref req) => {
            let Some(lease_storage) = lease_storage else {
                panic!("lease_storage should be Some(_) when creating command of LeaseRevokeRequest")
            };
            lease_storage
                .get_keys(req.id)
                .into_iter()
                .map(|k| KeyRange::new(k, ""))
                .collect()
        }
        _ => vec![],
    };
    Command::new(keys, wrapper, propose_id)
}

/// Convert `ClientError` to `tonic::Status`
pub(super) fn client_err_to_status(err: ClientError<Command>) -> tonic::Status {
    #[allow(clippy::wildcard_enum_match_arm)]
    match err {
        ClientError::CommandError(e) => {
            // If an error occurs during the `prepare` or `execute` stages, `after_sync` will
            // not be invoked. In this case, `wait_synced` will return the errors generated
            // in the first two stages. Therefore, if the response from `slow_round` arrives
            // earlier than `fast_round`, the `propose` function will return a `SyncedError`,
            // even though `after_sync` is not called.
            tonic::Status::from(e)
        }
        ClientError::ShuttingDown => tonic::Status::unavailable("Curp Server is shutting down"),
        ClientError::OutOfBound(status) => status,
        ClientError::EncodeDecode(msg) => tonic::Status::internal(msg),
        ClientError::Timeout => tonic::Status::unavailable("request timed out"),

        _ => unreachable!("curp client error {err:?}"),
    }
}
