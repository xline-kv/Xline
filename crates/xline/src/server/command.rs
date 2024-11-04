use std::{fmt::Debug, iter, sync::Arc};

use clippy_utilities::OverflowArithmetic;
use curp::{
    cmd::{
        AfterSyncCmd, AfterSyncOk, Command as CurpCommand, CommandExecutor as CurpCommandExecutor,
    },
    members::ServerId,
    InflightId, LogIndex,
};
use dashmap::DashMap;
use engine::{Snapshot, TransactionApi};
use event_listener::Event;
use parking_lot::RwLock;
use tracing::warn;
use utils::{barrier::IdBarrier, table_names::META_TABLE};
use xlineapi::{
    classifier::RequestClassifier,
    command::{Command, CurpClient, SyncResponse},
    execute_error::ExecuteError,
    AlarmAction, AlarmRequest, AlarmType,
};

use crate::{
    revision_number::RevisionNumberGeneratorState,
    rpc::RequestWrapper,
    storage::{
        db::{WriteOp, DB},
        index::IndexOperate,
        storage_api::XlineStorageOps,
        AlarmStore, AuthStore, KvStore, LeaseStore,
    },
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
pub(crate) struct CommandExecutor {
    /// Kv Storage
    kv_storage: Arc<KvStore>,
    /// Auth Storage
    auth_storage: Arc<AuthStore>,
    /// Lease Storage
    lease_storage: Arc<LeaseStore>,
    /// Alarm Storage
    alarm_storage: Arc<AlarmStore>,
    /// persistent storage
    db: Arc<DB>,
    /// Barrier for propose id
    id_barrier: Arc<IdBarrier<InflightId>>,
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
struct CommandQuotaChecker {
    /// Quota size
    quota: u64,
    /// persistent storage
    db: Arc<DB>,
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

impl CommandQuotaChecker {
    /// Create a new `CommandQuotaChecker`
    fn new(quota: u64, db: Arc<DB>) -> Self {
        Self { quota, db }
    }
}

impl QuotaChecker for CommandQuotaChecker {
    fn check(&self, cmd: &Command) -> bool {
        if !cmd.need_check_quota() {
            return true;
        }
        let cmd_size = size_estimate::cmd_size(cmd.request());
        if self.db.estimated_file_size().overflow_add(cmd_size) > self.quota {
            let Ok(file_size) = self.db.file_size() else {
                return false;
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
        let cmd = Command::new(request);
        let _ig = self.client.propose(&cmd, None, true).await?;
        Ok(())
    }
}

impl CommandExecutor {
    /// New `CommandExecutor`
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kv_storage: Arc<KvStore>,
        auth_storage: Arc<AuthStore>,
        lease_storage: Arc<LeaseStore>,
        alarm_storage: Arc<AlarmStore>,
        db: Arc<DB>,
        id_barrier: Arc<IdBarrier<InflightId>>,
        compact_events: Arc<DashMap<u64, Arc<Event>>>,
        quota: u64,
    ) -> Self {
        let alarmer = RwLock::new(None);
        let quota_checker = Arc::new(CommandQuotaChecker::new(quota, Arc::clone(&db)));
        Self {
            kv_storage,
            auth_storage,
            lease_storage,
            alarm_storage,
            db,
            id_barrier,
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

    /// After sync KV commands
    fn after_sync_kv<T>(
        &self,
        wrapper: &RequestWrapper,
        txn_db: &T,
        index: &(dyn IndexOperate + Send + Sync),
        revision_gen: &RevisionNumberGeneratorState<'_>,
        to_execute: bool,
    ) -> Result<
        (
            <Command as CurpCommand>::ASR,
            Option<<Command as CurpCommand>::ER>,
        ),
        ExecuteError,
    >
    where
        T: XlineStorageOps + TransactionApi,
    {
        let (asr, er) =
            self.kv_storage
                .after_sync(wrapper, txn_db, index, revision_gen, to_execute)?;
        Ok((asr, er))
    }

    /// After sync other type of commands
    fn after_sync_others<T, I>(
        &self,
        wrapper: &RequestWrapper,
        txn_db: &T,
        index: &I,
        general_revision: &RevisionNumberGeneratorState<'_>,
        auth_revision: &RevisionNumberGeneratorState<'_>,
        to_execute: bool,
    ) -> Result<
        (
            <Command as CurpCommand>::ASR,
            Option<<Command as CurpCommand>::ER>,
        ),
        ExecuteError,
    >
    where
        T: XlineStorageOps + TransactionApi,
        I: IndexOperate,
    {
        let er = to_execute
            .then(|| match wrapper {
                x if x.is_auth_backend() => self.auth_storage.execute(wrapper),
                x if x.is_lease_backend() => self.lease_storage.execute(wrapper),
                x if x.is_alarm_backend() => Ok(self.alarm_storage.execute(wrapper)),
                _ => unreachable!("Should not execute kv commands"),
            })
            .transpose()?;

        let (asr, wr_ops) = match wrapper {
            x if x.is_auth_backend() => self.auth_storage.after_sync(wrapper, auth_revision)?,
            x if x.is_lease_backend() => {
                self.lease_storage
                    .after_sync(wrapper, general_revision, txn_db, index)?
            }
            x if x.is_alarm_backend() => self.alarm_storage.after_sync(wrapper, general_revision),
            _ => unreachable!("Should not sync kv commands"),
        };

        txn_db.write_ops(wr_ops)?;

        Ok((asr, er))
    }
}

/// After Sync Result
type AfterSyncResult = Result<AfterSyncOk<Command>, <Command as CurpCommand>::Error>;

/// Collection of after sync results
struct ASResults<'a> {
    /// After sync cmds and there execution results
    cmd_results: Vec<(AfterSyncCmd<'a, Command>, Option<AfterSyncResult>)>,
}

impl<'a> ASResults<'a> {
    /// Creates a new [`ASResultStates`].
    fn new(cmds: Vec<AfterSyncCmd<'a, Command>>) -> Self {
        Self {
            // Initially all commands have no results
            cmd_results: cmds.into_iter().map(|cmd| (cmd, None)).collect(),
        }
    }

    #[allow(clippy::pattern_type_mismatch)] // can't be fixed
    /// Updates the results of commands that have errors by applying a given
    /// operation.
    fn update_err<F>(&mut self, op: F)
    where
        F: Fn(&AfterSyncCmd<'_, Command>) -> Result<(), ExecuteError>,
    {
        self.for_each_none_result(|(cmd, result_opt)| {
            if let Err(e) = op(cmd) {
                let _ignore = result_opt.replace(Err(e));
            }
        });
    }

    /// Updates the results of commands by applying a given operation.
    #[allow(clippy::pattern_type_mismatch)] // can't be fixed
    fn update_result<F>(&mut self, op: F)
    where
        F: Fn(&AfterSyncCmd<'_, Command>) -> AfterSyncResult,
    {
        self.for_each_none_result(|(cmd, result_opt)| {
            let _ignore = result_opt.replace(op(cmd));
        });
    }

    /// Applies the provided operation to each command-result pair in `cmd_results` where the result is `None`.
    #[allow(clippy::pattern_type_mismatch)] // can't be fixed
    fn for_each_none_result<F>(&mut self, op: F)
    where
        F: FnMut(&mut (AfterSyncCmd<'_, Command>, Option<AfterSyncResult>)),
    {
        self.cmd_results
            .iter_mut()
            .filter(|(_cmd, res)| res.is_none())
            .for_each(op);
    }

    /// Converts into errors.
    fn into_errors(self, err: <Command as CurpCommand>::Error) -> Vec<AfterSyncResult> {
        iter::repeat(err)
            .map(Err)
            .take(self.cmd_results.len())
            .collect()
    }

    /// Converts into results.
    fn into_results(self) -> Vec<AfterSyncResult> {
        self.cmd_results
            .into_iter()
            .filter_map(|(_cmd, res)| res)
            .collect()
    }
}

#[async_trait::async_trait]
impl CurpCommandExecutor<Command> for CommandExecutor {
    fn execute(
        &self,
        cmd: &Command,
    ) -> Result<<Command as CurpCommand>::ER, <Command as CurpCommand>::Error> {
        self.check_alarm(cmd)?;
        let auth_info = cmd.auth_info();
        let wrapper = cmd.request();
        self.auth_storage.check_permission(wrapper, auth_info)?;
        match &wrapper {
            x if x.is_kv_backend() => self.kv_storage.execute(wrapper, None),
            x if x.is_auth_backend() => self.auth_storage.execute(wrapper),
            x if x.is_lease_backend() => self.lease_storage.execute(wrapper),
            x if x.is_alarm_backend() => Ok(self.alarm_storage.execute(wrapper)),
            _ => unreachable!("Must be one of kv, auth, lease, alarm"),
        }
    }

    fn execute_ro(
        &self,
        cmd: &Command,
    ) -> Result<
        (<Command as CurpCommand>::ER, <Command as CurpCommand>::ASR),
        <Command as CurpCommand>::Error,
    > {
        let er = self.execute(cmd)?;
        let wrapper = cmd.request();
        let rev = match wrapper {
            x if x.is_auth_backend() => self.auth_storage.revision_gen().get(),
            x if (x.is_kv_backend() || x.is_lease_backend() || x.is_alarm_backend()) => {
                self.kv_storage.revision_gen().get()
            }
            _ => unreachable!("Must be one of kv, auth, lease, alarm"),
        };
        Ok((er, SyncResponse::new(rev)))
    }

    fn after_sync(
        &self,
        cmds: Vec<AfterSyncCmd<'_, Command>>,
        highest_index: Option<LogIndex>,
    ) -> Vec<AfterSyncResult> {
        if cmds.is_empty() {
            return Vec::new();
        }
        let quota_enough = cmds
            .iter()
            .map(AfterSyncCmd::cmd)
            .all(|c| self.quota_checker.check(c));

        let mut states = ASResults::new(cmds);
        states.update_err(|c| self.check_alarm(c.cmd()));
        states.update_err(|c| {
            self.auth_storage
                .check_permission(c.cmd().request(), c.cmd().auth_info())
        });

        let index = self.kv_storage.index();
        let index_state = index.state();
        let general_revision_gen = self.kv_storage.revision_gen();
        let auth_revision_gen = self.auth_storage.revision_gen();
        let general_revision_state = general_revision_gen.state();
        let auth_revision_state = auth_revision_gen.state();

        let txn_db = self.db.transaction();
        if let Some(i) = highest_index {
            if let Err(e) = txn_db.write_op(WriteOp::PutAppliedIndex(i)) {
                return states.into_errors(e);
            }
        }

        states.update_result(|c| {
            let (cmd, to_execute) = c.into_parts();
            let wrapper = cmd.request();
            let (asr, er) = match wrapper {
                x if x.is_kv_backend() => self.after_sync_kv(
                    wrapper,
                    &txn_db,
                    &index_state,
                    &general_revision_state,
                    to_execute,
                ),
                x if x.is_auth_backend() || x.is_lease_backend() || x.is_alarm_backend() => self
                    .after_sync_others(
                        wrapper,
                        &txn_db,
                        &index_state,
                        &general_revision_state,
                        &auth_revision_state,
                        to_execute,
                    ),
                _ => unreachable!("Must be one of kv, auth, lease, alarm"),
            }?;

            if let RequestWrapper::CompactionRequest(ref compact_req) = *wrapper {
                if compact_req.physical {
                    if let Some(n) = self.compact_events.get(&cmd.compact_id()) {
                        let _ignore = n.notify(usize::MAX);
                    }
                }
            };

            self.lease_storage.mark_lease_synced(wrapper);

            Ok(AfterSyncOk::new(asr, er))
        });

        if let Err(e) = txn_db.commit() {
            return states.into_errors(ExecuteError::DbError(e.to_string()));
        }
        index_state.commit();
        general_revision_state.commit();
        auth_revision_state.commit();

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

        states.into_results()
    }

    async fn reset(
        &self,
        snapshot: Option<(Snapshot, LogIndex)>,
    ) -> Result<(), <Command as CurpCommand>::Error> {
        let s = if let Some((snapshot, index)) = snapshot {
            self.db.write_ops(vec![WriteOp::PutAppliedIndex(index)])?;
            Some(snapshot)
        } else {
            None
        };
        self.db.reset(s).await?;
        self.kv_storage.recover().await
    }

    async fn snapshot(&self) -> Result<Snapshot, <Command as CurpCommand>::Error> {
        let path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
        self.db.get_snapshot(path)
    }

    fn set_last_applied(&self, index: LogIndex) -> Result<(), <Command as CurpCommand>::Error> {
        self.db.write_ops(vec![WriteOp::PutAppliedIndex(index)])?;
        Ok(())
    }

    fn last_applied(&self) -> Result<LogIndex, <Command as CurpCommand>::Error> {
        let Some(index_bytes) = self.db.get_value(META_TABLE, APPLIED_INDEX_KEY)? else {
            return Ok(0);
        };
        let buf: [u8; 8] = index_bytes
            .try_into()
            .unwrap_or_else(|e| panic!("cannot decode index from backend, {e:?}"));
        Ok(u64::from_le_bytes(buf))
    }

    fn trigger(&self, id: InflightId) {
        self.id_barrier.trigger(&id);
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
