use std::sync::Arc;

use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{
    client::Client,
    cmd::{Command as CurpCommand, CommandExecutor as CurpCommandExecutor},
    error::ClientError,
    members::ServerId,
    InflightId, LogIndex,
};
use dashmap::DashMap;
use engine::Snapshot;
use event_listener::Event;
use tracing::warn;
use xlineapi::{
    command::Command, execute_error::ExecuteError, AlarmAction, AlarmRequest, AlarmType,
    PutRequest, Request, TxnRequest,
};

use super::barriers::{IdBarrier, IndexBarrier};
use crate::{
    revision_number::RevisionNumberGenerator,
    rpc::{RequestBackend, RequestWithToken, RequestWrapper},
    storage::{db::WriteOp, storage_api::StorageApi, AlarmStore, AuthStore, KvStore, LeaseStore},
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
    /// Alarm Storage
    alarm_storage: Arc<AlarmStore<S>>,
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
    /// Compact events
    compact_events: Arc<DashMap<u64, Arc<Event>>>,
    /// Quota checker
    quota_checker: Arc<dyn QuotaChecker>,
    /// Alarmer
    alarmer: Alarmer,
}

/// Quota checker
pub(crate) trait QuotaChecker: Sync + Send + std::fmt::Debug {
    /// Check if the command executor has enough quota to execute the command
    fn check(&self, cmd: &Command) -> bool;
}

/// Quota checker for `Command`
#[derive(Debug)]
struct CommandQuotaChecker<S>
where
    S: StorageApi,
{
    /// Quota size
    quota: u64,
    /// persistent storage
    persistent: Arc<S>,
}

impl<S> CommandQuotaChecker<S>
where
    S: StorageApi,
{
    /// Create a new `CommandQuotaChecker`
    fn new(quota: u64, persistent: Arc<S>) -> Self {
        Self { quota, persistent }
    }

    /// Estimate the put size
    fn put_size(req: &PutRequest) -> u64 {
        let rev_size = 16; // size of `Revision` struct
        let kv_size = req.key.len().overflow_add(req.value.len()).overflow_add(32); // size of `KeyValue` struct
        1010 // padding(1008) + cf_handle(2)
            .overflow_add(rev_size.overflow_mul(2))
            .overflow_add(kv_size.cast())
    }

    /// Estimate the txn size
    fn txn_size(req: &TxnRequest) -> u64 {
        let success_size = req
            .success
            .iter()
            .map(|req_op| match req_op.request {
                Some(Request::RequestPut(ref r)) => Self::put_size(r),
                Some(Request::RequestTxn(ref r)) => Self::txn_size(r),
                _ => 0,
            })
            .sum::<u64>();
        let failure_size = req
            .failure
            .iter()
            .map(|req_op| match req_op.request {
                Some(Request::RequestPut(ref r)) => Self::put_size(r),
                Some(Request::RequestTxn(ref r)) => Self::txn_size(r),
                _ => 0,
            })
            .sum::<u64>();

        success_size.max(failure_size)
    }

    /// Estimate the size that may increase after the request is written
    fn cmd_size(req: &RequestWrapper) -> u64 {
        match *req {
            RequestWrapper::PutRequest(ref req) => Self::put_size(req),
            RequestWrapper::TxnRequest(ref req) => Self::txn_size(req),
            RequestWrapper::LeaseGrantRequest(_) => {
                // padding(1008) + cf_handle(5) + lease_id_size(8) * 2 + lease_size(24)
                1053
            }
            RequestWrapper::RangeRequest(_)
            | RequestWrapper::DeleteRangeRequest(_)
            | RequestWrapper::CompactionRequest(_)
            | RequestWrapper::AuthEnableRequest(_)
            | RequestWrapper::AuthDisableRequest(_)
            | RequestWrapper::AuthStatusRequest(_)
            | RequestWrapper::AuthRoleAddRequest(_)
            | RequestWrapper::AuthRoleDeleteRequest(_)
            | RequestWrapper::AuthRoleGetRequest(_)
            | RequestWrapper::AuthRoleGrantPermissionRequest(_)
            | RequestWrapper::AuthRoleListRequest(_)
            | RequestWrapper::AuthRoleRevokePermissionRequest(_)
            | RequestWrapper::AuthUserAddRequest(_)
            | RequestWrapper::AuthUserChangePasswordRequest(_)
            | RequestWrapper::AuthUserDeleteRequest(_)
            | RequestWrapper::AuthUserGetRequest(_)
            | RequestWrapper::AuthUserGrantRoleRequest(_)
            | RequestWrapper::AuthUserListRequest(_)
            | RequestWrapper::AuthUserRevokeRoleRequest(_)
            | RequestWrapper::AuthenticateRequest(_)
            | RequestWrapper::LeaseRevokeRequest(_)
            | RequestWrapper::LeaseLeasesRequest(_)
            | RequestWrapper::AlarmRequest(_) => 0,
        }
    }
}

impl<S> QuotaChecker for CommandQuotaChecker<S>
where
    S: StorageApi,
{
    fn check(&self, cmd: &Command) -> bool {
        if !cmd.need_check_quota() {
            return true;
        }
        let cmd_size = Self::cmd_size(&cmd.request().request);
        if self.persistent.size().overflow_add(cmd_size) > self.quota {
            if let Ok(file_size) = self.persistent.file_size() {
                if file_size.overflow_add(cmd_size) > self.quota {
                    warn!(
                        "Quota exceeded, file size: {}, cmd size: {}, quota: {}",
                        file_size, cmd_size, self.quota
                    );
                    return false;
                }
            }
        }
        true
    }
}

/// Alarmer
#[derive(Debug, Clone)]
struct Alarmer {
    /// Node id
    id: ServerId,
    /// Client
    client: Arc<Client<Command>>,
}

impl Alarmer {
    /// Create a new `Alarmer`
    fn new(id: ServerId, client: Arc<Client<Command>>) -> Self {
        Self { id, client }
    }

    /// Propose alarm request to other nodes
    async fn alarm(
        &self,
        action: AlarmAction,
        alarm: AlarmType,
    ) -> Result<(), ClientError<Command>> {
        let alarm_req = AlarmRequest::new(action, self.id, alarm);
        _ = self
            .client
            .propose(
                Command::new(
                    vec![],
                    RequestWithToken::new(RequestWrapper::AlarmRequest(alarm_req)),
                ),
                true,
            )
            .await?;
        Ok(())
    }
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
        alarm_storage: Arc<AlarmStore<S>>,
        persistent: Arc<S>,
        index_barrier: Arc<IndexBarrier>,
        id_barrier: Arc<IdBarrier>,
        general_rev: Arc<RevisionNumberGenerator>,
        auth_rev: Arc<RevisionNumberGenerator>,
        compact_events: Arc<DashMap<u64, Arc<Event>>>,
        quota: u64,
        node_id: ServerId,
        client: Arc<Client<Command>>,
    ) -> Self {
        let alarmer = Alarmer::new(node_id, client);
        let quota_checker = Arc::new(CommandQuotaChecker::new(quota, Arc::clone(&persistent)));
        Self {
            kv_storage,
            auth_storage,
            lease_storage,
            alarm_storage,
            persistent,
            index_barrier,
            id_barrier,
            general_rev,
            auth_rev,
            compact_events,
            quota_checker,
            alarmer,
        }
    }

    /// Check if the alarm is activated
    fn check_alarm(&self, cmd: &Command) -> Result<(), ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        match cmd.request().request {
            RequestWrapper::PutRequest(_)
            | RequestWrapper::TxnRequest(_)
            | RequestWrapper::LeaseGrantRequest(_) => match self.alarm_storage.current_alarm() {
                AlarmType::Corrupt => Err(ExecuteError::DbError("Corrupt".to_owned())),
                AlarmType::Nospace => Err(ExecuteError::Nospace),
                AlarmType::None => Ok(()),
            },

            RequestWrapper::RangeRequest(_)
            | RequestWrapper::DeleteRangeRequest(_)
            | RequestWrapper::LeaseRevokeRequest(_)
            | RequestWrapper::CompactionRequest(_) => match self.alarm_storage.current_alarm() {
                AlarmType::Corrupt => Err(ExecuteError::DbError("Corrupt".to_owned())),
                AlarmType::Nospace | AlarmType::None => Ok(()),
            },

            _ => Ok(()),
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
    ) -> Result<<Command as CurpCommand>::PR, <Command as CurpCommand>::Error> {
        self.check_alarm(cmd)?;
        let wrapper = cmd.request();
        self.auth_storage.check_permission(wrapper)?;
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
            RequestBackend::Alarm => -1,
        };
        Ok(revision)
    }

    async fn execute(
        &self,
        cmd: &Command,
    ) -> Result<<Command as CurpCommand>::ER, <Command as CurpCommand>::Error> {
        let wrapper = cmd.request();
        match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.execute(wrapper),
            RequestBackend::Auth => self.auth_storage.execute(wrapper),
            RequestBackend::Lease => self.lease_storage.execute(wrapper),
            RequestBackend::Alarm => Ok(self.alarm_storage.execute(wrapper)),
        }
    }

    async fn after_sync(
        &self,
        cmd: &Command,
        index: LogIndex,
        revision: i64,
    ) -> Result<<Command as CurpCommand>::ASR, <Command as CurpCommand>::Error> {
        let quota_enough = self.quota_checker.check(cmd);
        let mut ops = vec![WriteOp::PutAppliedIndex(index)];
        let wrapper = cmd.request();
        let (res, mut wr_ops) = match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.after_sync(wrapper, revision).await?,
            RequestBackend::Auth => self.auth_storage.after_sync(wrapper, revision)?,
            RequestBackend::Lease => self.lease_storage.after_sync(wrapper, revision).await?,
            RequestBackend::Alarm => self.alarm_storage.after_sync(wrapper, revision),
        };
        if let RequestWrapper::CompactionRequest(ref compact_req) = wrapper.request {
            if compact_req.physical {
                if let Some(n) = self.compact_events.get(&cmd.compact_id()) {
                    n.notify(usize::MAX);
                }
            }
        };
        if let RequestWrapper::CompactionRequest(ref compact_req) = wrapper.request {
            if compact_req.physical {
                if let Some(n) = self.compact_events.get(&cmd.compact_id()) {
                    n.notify(usize::MAX);
                }
            }
        };
        ops.append(&mut wr_ops);
        let key_revisions = self.persistent.flush_ops(ops)?;
        if !key_revisions.is_empty() {
            self.kv_storage.insert_index(key_revisions);
        }
        self.lease_storage.mark_lease_synced(&wrapper.request);
        if !quota_enough {
            let alarmer = self.alarmer.clone();
            let _ig = tokio::spawn(async move {
                if let Err(e) = alarmer
                    .alarm(AlarmAction::Activate, AlarmType::Nospace)
                    .await
                {
                    warn!("{} propose alarm failed: {:?}", alarmer.id, e);
                }
            });
        }
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

    fn trigger(&self, id: InflightId, index: LogIndex) {
        self.id_barrier.trigger(id);
        self.index_barrier.trigger(index);
    }
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
