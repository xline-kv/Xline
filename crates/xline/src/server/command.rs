use std::{fmt::Debug, sync::Arc};

use clippy_utilities::OverflowArithmetic;
use curp::{
    cmd::{Command as CurpCommand, CommandExecutor as CurpCommandExecutor},
    members::ServerId,
    InflightId, LogIndex,
};
use dashmap::DashMap;
use engine::Snapshot;
use event_listener::Event;
use parking_lot::RwLock;
use tracing::warn;
use utils::table_names::META_TABLE;
use xlineapi::{
    command::{Command, CurpClient},
    execute_error::ExecuteError,
    AlarmAction, AlarmRequest, AlarmType,
};

use super::barriers::{IdBarrier, IndexBarrier};
use crate::{
    revision_number::RevisionNumberGenerator,
    rpc::{RequestBackend, RequestWrapper},
    storage::{db::WriteOp, storage_api::StorageApi, AlarmStore, AuthStore, KvStore, LeaseStore},
};

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
    alarmer: RwLock<Option<Alarmer>>,
}

/// Quota checker
pub(crate) trait QuotaChecker: Sync + Send + Debug {
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

/// functions used to estimate request write size
mod size_estimate {
    use clippy_utilities::{NumericCast, OverflowArithmetic};
    use xlineapi::{PutRequest, Request, RequestWrapper, TxnRequest};

    /// Estimate the put size
    fn put_size(req: &PutRequest) -> u64 {
        let rev_size = 16; // size of `Revision` struct
        let kv_size = req.key.len().overflow_add(req.value.len()).overflow_add(32); // size of `KeyValue` struct
        1010 // padding(1008) + cf_handle(2)
            .overflow_add(rev_size.overflow_mul(2))
            .overflow_add(kv_size.numeric_cast())
    }

    /// Estimate the txn size
    fn txn_size(req: &TxnRequest) -> u64 {
        let success_size = req
            .success
            .iter()
            .map(|req_op| match req_op.request {
                Some(Request::RequestPut(ref r)) => put_size(r),
                Some(Request::RequestTxn(ref r)) => txn_size(r),
                _ => 0,
            })
            .sum::<u64>();
        let failure_size = req
            .failure
            .iter()
            .map(|req_op| match req_op.request {
                Some(Request::RequestPut(ref r)) => put_size(r),
                Some(Request::RequestTxn(ref r)) => txn_size(r),
                _ => 0,
            })
            .sum::<u64>();

        success_size.max(failure_size)
    }

    /// Estimate the size that may increase after the request is written
    pub(super) fn cmd_size(req: &RequestWrapper) -> u64 {
        #[allow(clippy::wildcard_enum_match_arm)]
        match *req {
            RequestWrapper::PutRequest(ref req) => put_size(req),
            RequestWrapper::TxnRequest(ref req) => txn_size(req),
            RequestWrapper::LeaseGrantRequest(_) => {
                // padding(1008) + cf_handle(5) + lease_id_size(8) * 2 + lease_size(24)
                1053
            }
            _ => 0,
        }
    }
}

impl<S> CommandQuotaChecker<S>
where
    S: StorageApi,
{
    /// Create a new `CommandQuotaChecker`
    fn new(quota: u64, persistent: Arc<S>) -> Self {
        Self { quota, persistent }
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
        let cmd_size = size_estimate::cmd_size(cmd.request());
        if self.persistent.estimated_file_size().overflow_add(cmd_size) > self.quota {
            let Ok(file_size) = self.persistent.file_size() else {
                return false
            };
            if file_size.overflow_add(cmd_size) > self.quota {
                warn!(
                    "Quota exceeded, file size: {}, cmd size: {}, quota: {}",
                    file_size, cmd_size, self.quota
                );
                return false;
            }
        }
        true
    }
}

/// Alarmer
#[derive(Clone)]
pub(crate) struct Alarmer {
    /// Node id
    id: ServerId,
    /// Client
    client: Arc<CurpClient>,
}

impl Debug for Alarmer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Alarmer").field("id", &self.id).finish()
    }
}

impl Alarmer {
    /// Create a new `Alarmer`
    pub(super) fn new(id: ServerId, client: Arc<CurpClient>) -> Self {
        Self { id, client }
    }

    /// Propose alarm request to other nodes
    async fn alarm(&self, action: AlarmAction, alarm: AlarmType) -> Result<(), tonic::Status> {
        let request = RequestWrapper::from(AlarmRequest::new(action, self.id, alarm));
        let cmd = Command::new(request.keys(), request);
        let _ig = self.client.propose(&cmd, None, true).await?;
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
    ) -> Self {
        let alarmer = RwLock::new(None);
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

    /// Set alarmer
    pub(crate) fn set_alarmer(&self, alarmer: Alarmer) {
        *self.alarmer.write() = Some(alarmer);
    }

    /// Check if the alarm is activated
    fn check_alarm(&self, cmd: &Command) -> Result<(), ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        match *cmd.request() {
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
        let auth_info = cmd.auth_info();
        self.auth_storage.check_permission(wrapper, auth_info)?;
        let revision = match wrapper.backend() {
            RequestBackend::Auth => {
                if wrapper.skip_auth_revision() {
                    -1
                } else {
                    self.auth_rev.next()
                }
            }
            RequestBackend::Kv | RequestBackend::Lease => {
                if wrapper.skip_general_revision() {
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
        match wrapper.backend() {
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
        let (res, mut wr_ops) = match wrapper.backend() {
            RequestBackend::Kv => self.kv_storage.after_sync(wrapper, revision).await?,
            RequestBackend::Auth => self.auth_storage.after_sync(wrapper, revision)?,
            RequestBackend::Lease => self.lease_storage.after_sync(wrapper, revision).await?,
            RequestBackend::Alarm => self.alarm_storage.after_sync(wrapper, revision),
        };
        if let RequestWrapper::CompactionRequest(ref compact_req) = *wrapper {
            if compact_req.physical {
                if let Some(n) = self.compact_events.get(&cmd.compact_id()) {
                    n.notify(usize::MAX);
                }
            }
        };
        if let RequestWrapper::CompactionRequest(ref compact_req) = *wrapper {
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
        self.lease_storage.mark_lease_synced(wrapper);
        if !quota_enough {
            if let Some(alarmer) = self.alarmer.read().clone() {
                let _ig = tokio::spawn(async move {
                    if let Err(e) = alarmer
                        .alarm(AlarmAction::Activate, AlarmType::Nospace)
                        .await
                    {
                        warn!("{} propose alarm failed: {:?}", alarmer.id, e);
                    }
                });
            }
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

#[cfg(test)]
mod test {
    use xlineapi::{LeaseGrantRequest, PutRequest, Request, RequestOp, TxnRequest};

    use super::*;
    #[test]
    fn cmd_size_should_return_size_of_command() {
        let put_req1 = PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            ..Default::default()
        };
        let put_req2 = PutRequest {
            key: b"abcde".to_vec(),
            value: b"abcdefg".to_vec(),
            ..Default::default()
        };
        let lease_req = LeaseGrantRequest { id: 123, ttl: 10 };
        let txn_req = TxnRequest {
            compare: vec![],
            success: vec![
                RequestOp {
                    request: Some(Request::RequestPut(put_req1.clone())),
                },
                RequestOp {
                    request: Some(Request::RequestPut(put_req2.clone())),
                },
            ],
            failure: vec![RequestOp {
                request: Some(Request::RequestPut(put_req2.clone())),
            }],
        };
        let testcase: &[(RequestWrapper, u64)] = &[
            (put_req1.into(), 1082),
            (put_req2.into(), 1086),
            (lease_req.into(), 1053),
            (txn_req.into(), 2168),
        ];
        for &(ref req, size) in testcase {
            assert_eq!(size_estimate::cmd_size(req), size);
        }
    }
}
