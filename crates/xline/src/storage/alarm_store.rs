use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use curp::members::ServerId;
use parking_lot::RwLock;
use prost::Message;
use utils::table_names::ALARM_TABLE;
use xlineapi::{
    command::{CommandResponse, SyncResponse},
    execute_error::ExecuteError,
    AlarmAction, AlarmMember, AlarmResponse, AlarmType, RequestWrapper, ResponseWrapper,
};

use super::{db::WriteOp, storage_api::StorageApi};
use crate::header_gen::HeaderGenerator;

/// Alarm store
#[derive(Debug)]
pub(crate) struct AlarmStore<DB>
where
    DB: StorageApi,
{
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Persistent storage
    db: Arc<DB>,
    /// Alarm types
    types: RwLock<HashMap<AlarmType, HashMap<ServerId, AlarmMember>>>,
    /// Current alarm
    current_alarm: AtomicI32,
}

impl<DB> AlarmStore<DB>
where
    DB: StorageApi,
{
    /// execute a alarm request
    pub(crate) fn execute(&self, request: &RequestWrapper) -> CommandResponse {
        #[allow(clippy::wildcard_enum_match_arm)]
        let alarms = match *request {
            RequestWrapper::AlarmRequest(ref req) => match req.action() {
                AlarmAction::Get => self.handle_alarm_get(req.alarm()),
                AlarmAction::Activate => {
                    if req.alarm() == AlarmType::None {
                        vec![]
                    } else {
                        self.handle_alarm_activate(req.member_id, req.alarm())
                    }
                }
                AlarmAction::Deactivate => self.handle_alarm_deactivate(req.member_id, req.alarm()),
            },
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        };
        CommandResponse::new(ResponseWrapper::AlarmResponse(AlarmResponse {
            header: Some(self.header_gen.gen_header()),
            alarms,
        }))
    }

    /// sync a alarm request
    pub(crate) fn after_sync(
        &self,
        request: &RequestWrapper,
        revision: i64,
    ) -> (SyncResponse, Vec<WriteOp>) {
        #[allow(clippy::wildcard_enum_match_arm)]
        let ops = match *request {
            RequestWrapper::AlarmRequest(ref req) => match req.action() {
                AlarmAction::Get => vec![],
                AlarmAction::Activate => self.sync_alarm_activate(req.member_id, req.alarm()),
                AlarmAction::Deactivate => self.sync_alarm_deactivate(req.member_id, req.alarm()),
            },
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        };
        (SyncResponse::new(revision), ops)
    }

    /// Recover data form persistent storage
    pub(crate) fn recover(&self) -> Result<(), ExecuteError> {
        let alarms = self.get_all_alarms_from_db()?;
        let mut types_w = self.types.write();
        for alarm in alarms {
            _ = types_w
                .entry(alarm.alarm())
                .or_default()
                .insert(alarm.member_id, alarm);
        }
        Ok(())
    }
}

impl<DB> AlarmStore<DB>
where
    DB: StorageApi,
{
    /// Create a new alarm store
    pub(crate) fn new(header_gen: Arc<HeaderGenerator>, db: Arc<DB>) -> Self {
        Self {
            header_gen,
            db,
            types: RwLock::new(HashMap::new()),
            current_alarm: AtomicI32::new(i32::from(AlarmType::None)),
        }
    }

    /// Get current alarm
    pub(crate) fn current_alarm(&self) -> AlarmType {
        let current_alarm = self.current_alarm.load(Ordering::Relaxed);
        AlarmType::try_from(current_alarm)
            .unwrap_or_else(|e| unreachable!("current alarm should be valid, err: {e}"))
    }

    /// Refresh current alarm
    fn refresh_current_alarm(&self, types: &HashMap<AlarmType, HashMap<ServerId, AlarmMember>>) {
        let corrupt_alarms = types
            .get(&AlarmType::Corrupt)
            .is_some_and(|e| !e.is_empty());
        if corrupt_alarms {
            self.current_alarm
                .store(i32::from(AlarmType::Corrupt), Ordering::Relaxed);
            return;
        }

        let no_space_alarms = types
            .get(&AlarmType::Nospace)
            .is_some_and(|e| !e.is_empty());
        if no_space_alarms {
            self.current_alarm
                .store(i32::from(AlarmType::Nospace), Ordering::Relaxed);
            return;
        }

        self.current_alarm
            .store(i32::from(AlarmType::None), Ordering::Relaxed);
    }

    /// Get all alarms
    pub(crate) fn get_all_alarms(&self) -> Vec<AlarmMember> {
        self.handle_alarm_get(AlarmType::None)
    }

    /// Get all alarms from persistent storage
    fn get_all_alarms_from_db(&self) -> Result<Vec<AlarmMember>, ExecuteError> {
        let alarms = self
            .db
            .get_all(ALARM_TABLE)?
            .into_iter()
            .map(|(alarm, _)| {
                AlarmMember::decode(alarm.as_slice()).unwrap_or_else(|e| {
                    panic!("Failed to decode alarm from value, error: {e:?}, alarm: {alarm:?}");
                })
            })
            .collect();
        Ok(alarms)
    }

    /// Handle alarm get request
    fn handle_alarm_get(&self, alarm: AlarmType) -> Vec<AlarmMember> {
        let types = self.types.read();
        match alarm {
            AlarmType::None => types.values().flat_map(HashMap::values).cloned().collect(),
            a @ (AlarmType::Nospace | AlarmType::Corrupt) => types
                .get(&a)
                .map(|s| s.values().cloned().collect())
                .unwrap_or_default(),
        }
    }

    /// Handle alarm activate request
    fn handle_alarm_activate(&self, member_id: ServerId, alarm: AlarmType) -> Vec<AlarmMember> {
        let new_alarm = AlarmMember::new(member_id, alarm);
        self.types
            .read()
            .get(&alarm)
            .and_then(|e| e.get(&member_id))
            .map_or_else(|| vec![new_alarm], |m| vec![m.clone()])
    }

    /// Handle alarm deactivate request
    fn handle_alarm_deactivate(&self, member_id: ServerId, alarm: AlarmType) -> Vec<AlarmMember> {
        self.types
            .read()
            .get(&alarm)
            .and_then(|e| e.get(&member_id))
            .map(|m| vec![m.clone()])
            .unwrap_or_default()
    }

    /// Sync alarm activate request
    fn sync_alarm_activate(&self, member_id: ServerId, alarm: AlarmType) -> Vec<WriteOp> {
        let new_alarm: AlarmMember = AlarmMember::new(member_id, alarm);
        let mut types_w = self.types.write();
        let e = types_w.entry(alarm).or_default();
        let mut ops = vec![];
        if e.get(&member_id).is_none() {
            _ = e.insert(new_alarm.member_id, new_alarm.clone());
            ops.push(WriteOp::PutAlarm(new_alarm));
        }
        self.refresh_current_alarm(&types_w);
        ops
    }

    /// Sync alarm deactivate request
    fn sync_alarm_deactivate(&self, member_id: ServerId, alarm: AlarmType) -> Vec<WriteOp> {
        let mut types_w = self.types.write();
        let e = types_w.entry(alarm).or_default();
        let mut ops = vec![];
        if let Some(m) = e.remove(&member_id) {
            ops.push(WriteOp::DeleteAlarm(m));
        }
        self.refresh_current_alarm(&types_w);
        ops
    }
}
