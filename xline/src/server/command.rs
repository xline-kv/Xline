use std::{
    collections::{HashSet, VecDeque},
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use curp::{
    cmd::{
        Command as CurpCommand, CommandExecutor as CurpCommandExecutor, ConflictCheck, ProposeId,
    },
    LogIndex,
};
use engine::Snapshot;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

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
pub struct KeyRange {
    /// Start of range
    key: Bound<Vec<u8>>,
    /// End of range
    range_end: Bound<Vec<u8>>,
}

impl KeyRange {
    /// New `KeyRange`
    #[inline]
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        let key_vec = start.into();
        let range_end_vec = end.into();
        let range_end = match range_end_vec.as_slice() {
            UNBOUNDED => Bound::Unbounded,
            ONE_KEY => Bound::Included(key_vec.clone()),
            _ => Bound::Excluded(range_end_vec),
        };
        let key = match key_vec.as_slice() {
            UNBOUNDED => Bound::Unbounded,
            _ => Bound::Included(key_vec),
        };
        KeyRange { key, range_end }
    }

    /// New `KeyRange` only contains one key
    ///
    /// # Panics
    ///
    /// Will panic if key is equal to `UNBOUNDED`
    #[inline]
    pub fn new_one_key(key: impl Into<Vec<u8>>) -> Self {
        let key_vec = key.into();
        assert!(
            key_vec.as_slice() != UNBOUNDED,
            "Unbounded key is not allowed: {key_vec:?}",
        );
        Self {
            key: Bound::Included(key_vec.clone()),
            range_end: Bound::Included(key_vec),
        }
    }

    /// Return if `KeyRange` is conflicted with another
    #[must_use]
    #[inline]
    pub fn is_conflicted(&self, other: &Self) -> bool {
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
    #[must_use]
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        (match self.start_bound() {
            Bound::Included(start) => start.as_slice() <= key,
            Bound::Excluded(start) => start.as_slice() < key,
            Bound::Unbounded => true,
        }) && (match self.end_bound() {
            Bound::Included(end) => key <= end.as_slice(),
            Bound::Excluded(end) => key < end.as_slice(),
            Bound::Unbounded => true,
        })
    }

    /// Get end of range with prefix
    /// User will provide a start key when prefix is true, we need calculate the end key of `KeyRange`
    #[allow(clippy::indexing_slicing)] // end[i] is always valid
    #[must_use]
    #[inline]
    pub fn get_prefix(key: &[u8]) -> Vec<u8> {
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

    /// unpack `KeyRange` to tuple
    #[must_use]
    #[inline]
    pub fn unpack(self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (self.key, self.range_end)
    }

    /// start key of `KeyRange`
    #[must_use]
    #[inline]
    pub fn range_start(&self) -> &[u8] {
        match self.key {
            Bound::Included(ref k) => k.as_slice(),
            Bound::Excluded(_) => unreachable!("KeyRange::start_bound() cannot be Excluded"),
            Bound::Unbounded => &[0],
        }
    }

    /// end key of `KeyRange`
    #[must_use]
    #[inline]
    pub fn range_end(&self) -> &[u8] {
        match self.range_end {
            Bound::Included(_) => &[],
            Bound::Excluded(ref k) => k.as_slice(),
            Bound::Unbounded => &[0],
        }
    }
}

impl RangeBounds<Vec<u8>> for KeyRange {
    #[inline]
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        match self.key {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(ref k) => Bound::Included(k),
            Bound::Excluded(_) => unreachable!("KeyRange::start_bound() cannot be Excluded"),
        }
    }
    #[inline]
    fn end_bound(&self) -> Bound<&Vec<u8>> {
        match self.range_end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(ref k) => Bound::Included(k),
            Bound::Excluded(ref k) => Bound::Excluded(k),
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
    type Error = ExecuteError;

    fn prepare(
        &self,
        cmd: &Command,
        index: LogIndex,
    ) -> Result<<Command as CurpCommand>::PR, Self::Error> {
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
    ) -> Result<<Command as CurpCommand>::ER, Self::Error> {
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
    ) -> Result<<Command as CurpCommand>::ASR, Self::Error> {
        let mut ops = vec![WriteOp::PutAppliedIndex(index)];
        let wrapper = cmd.request();
        let (res, mut wr_ops) = match wrapper.request.backend() {
            RequestBackend::Kv => self.kv_storage.after_sync(wrapper, revision).await?,
            RequestBackend::Auth => self.auth_storage.after_sync(wrapper, revision)?,
            RequestBackend::Lease => self.lease_storage.after_sync(wrapper, revision).await?,
        };
        ops.append(&mut wr_ops);
        self.persistent.flush_ops(ops)?;
        self.kv_storage.mark_index_available(res.revision());
        self.id_barrier.trigger(cmd.id());
        self.index_barrier.trigger(index);
        Ok(res)
    }

    async fn reset(&self, snapshot: Option<(Snapshot, LogIndex)>) -> Result<(), Self::Error> {
        let s = if let Some((snapshot, index)) = snapshot {
            self.persistent
                .flush_ops(vec![WriteOp::PutAppliedIndex(index)])?;
            Some(snapshot)
        } else {
            None
        };
        self.persistent.reset(s).await
    }

    async fn snapshot(&self) -> Result<Snapshot, Self::Error> {
        let path = format!("/tmp/snapshot-{}", uuid::Uuid::new_v4());
        self.persistent.get_snapshot(path)
    }

    fn last_applied(&self) -> Result<LogIndex, Self::Error> {
        let Some(index_bytes) = self.persistent.get_value(META_TABLE, APPLIED_INDEX_KEY)? else {
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
pub struct Command {
    /// Keys of request
    keys: Vec<KeyRange>,
    /// Request data
    request: RequestWithToken,
    /// Propose id
    id: ProposeId,
}

/// get all lease ids in the request wrapper
fn get_lease_ids(wrapper: &RequestWrapper) -> HashSet<i64> {
    match *wrapper {
        RequestWrapper::LeaseGrantRequest(ref req) => HashSet::from_iter(vec![req.id]),
        RequestWrapper::LeaseRevokeRequest(ref req) => HashSet::from_iter(vec![req.id]),
        RequestWrapper::PutRequest(ref req) if req.lease != 0 => {
            HashSet::from_iter(vec![req.lease])
        }
        RequestWrapper::TxnRequest(ref txn_req) => {
            let mut lease_ids = HashSet::new();
            let mut reqs = txn_req
                .success
                .iter()
                .chain(txn_req.failure.iter())
                .filter_map(|op| op.request.as_ref())
                .collect::<VecDeque<_>>();
            while let Some(req) = reqs.pop_front() {
                match *req {
                    Request::RequestPut(ref req) => {
                        if req.lease != 0 {
                            let _ignore = lease_ids.insert(req.lease);
                        }
                    }
                    Request::RequestTxn(ref req) => reqs.extend(
                        &mut req
                            .success
                            .iter()
                            .chain(req.failure.iter())
                            .filter_map(|op| op.request.as_ref()),
                    ),
                    Request::RequestRange(_) | Request::RequestDeleteRange(_) => {}
                }
            }
            lease_ids
        }
        RequestWrapper::PutRequest(_)
        | RequestWrapper::RangeRequest(_)
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
        | RequestWrapper::LeaseLeasesRequest(_) => HashSet::new(),
    }
}

impl ConflictCheck for Command {
    #[inline]
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

        // Lease leases request is conflict with Lease grant and revoke requests
        if (this_req.is_lease_read_request() && other_req.is_lease_write_request())
            || (this_req.is_lease_write_request() && other_req.is_lease_read_request())
        {
            return true;
        }

        let this_lease_ids = get_lease_ids(this_req);
        let other_lease_ids = get_lease_ids(other_req);
        let lease_conflict = !this_lease_ids.is_disjoint(&other_lease_ids);
        let key_conflict = self
            .keys()
            .iter()
            .cartesian_product(other.keys().iter())
            .any(|(k1, k2)| k1.is_conflicted(k2));
        lease_conflict || key_conflict
    }
}

impl ConflictCheck for KeyRange {
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        self.is_conflicted(other)
    }
}

impl Command {
    /// New `Command`
    #[must_use]
    #[inline]
    pub fn new(keys: Vec<KeyRange>, request: RequestWithToken, id: ProposeId) -> Self {
        Self { keys, request, id }
    }

    /// get request
    #[must_use]
    #[inline]
    pub fn request(&self) -> &RequestWithToken {
        &self.request
    }
}

/// Command to run consensus protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandResponse {
    /// Response data
    response: ResponseWrapper,
}

impl CommandResponse {
    /// New `ResponseOp` from `CommandResponse`
    #[inline]
    pub fn new(response: ResponseWrapper) -> Self {
        Self { response }
    }

    /// Decode `CommandResponse` and get `ResponseOp`
    #[inline]
    pub fn decode(self) -> ResponseWrapper {
        self.response
    }
}

/// Sync Response
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    /// Revision of this request
    revision: i64,
}
impl SyncResponse {
    /// New `SyncRequest`
    #[inline]
    pub fn new(revision: i64) -> Self {
        Self { revision }
    }

    /// Get revision field
    #[inline]
    pub fn revision(self) -> i64 {
        self.revision
    }
}

#[async_trait::async_trait]
impl CurpCommand for Command {
    type K = KeyRange;
    type PR = i64;
    type ER = CommandResponse;
    type ASR = SyncResponse;

    #[inline]
    fn keys(&self) -> &[Self::K] {
        self.keys.as_slice()
    }

    #[inline]
    fn id(&self) -> &ProposeId {
        &self.id
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::rpc::{
        AuthEnableRequest, AuthStatusRequest, LeaseGrantRequest, LeaseLeasesRequest,
        LeaseRevokeRequest, PutRequest, RequestOp, TxnRequest,
    };

    #[test]
    fn test_key_range_conflict() {
        let kr1 = KeyRange::new("a", "e");
        let kr2 = KeyRange::new_one_key("c");
        let kr3 = KeyRange::new_one_key("z");
        assert!(kr1.is_conflict(&kr2));
        assert!(!kr1.is_conflict(&kr3));
    }

    #[test]
    fn test_key_range_prefix() {
        assert_eq!(KeyRange::get_prefix(b"key"), b"kez");
        assert_eq!(KeyRange::get_prefix(b"z"), b"\x7b");
        assert_eq!(KeyRange::get_prefix(&[255]), b"\0");
    }

    #[test]
    fn test_key_range_contains() {
        let kr1 = KeyRange::new("a", "e");
        assert!(kr1.contains_key(b"b"));
        assert!(!kr1.contains_key(b"e"));
        let kr2 = KeyRange::new_one_key("c");
        assert!(kr2.contains_key(b"c"));
        assert!(!kr2.contains_key(b"d"));
        let kr3 = KeyRange::new("c", [0]);
        assert!(kr3.contains_key(b"d"));
        assert!(!kr3.contains_key(b"a"));
        let kr4 = KeyRange::new([0], "e");
        assert!(kr4.contains_key(b"d"));
        assert!(!kr4.contains_key(b"e"));
    }

    #[test]
    fn test_command_conflict() {
        let cmd1 = Command::new(
            vec![KeyRange::new("a", "e")],
            RequestWithToken::new(RequestWrapper::PutRequest(PutRequest::default())),
            ProposeId::new("id".to_owned()),
        );
        let cmd2 = Command::new(
            vec![],
            RequestWithToken::new(RequestWrapper::AuthStatusRequest(
                AuthStatusRequest::default(),
            )),
            ProposeId::new("id".to_owned()),
        );
        let cmd3 = Command::new(
            vec![KeyRange::new("c", "g")],
            RequestWithToken::new(RequestWrapper::PutRequest(PutRequest::default())),
            ProposeId::new("id2".to_owned()),
        );
        let cmd4 = Command::new(
            vec![],
            RequestWithToken::new(RequestWrapper::AuthEnableRequest(
                AuthEnableRequest::default(),
            )),
            ProposeId::new("id3".to_owned()),
        );
        let cmd5 = Command::new(
            vec![],
            RequestWithToken::new(RequestWrapper::LeaseGrantRequest(LeaseGrantRequest {
                ttl: 1,
                id: 1,
            })),
            ProposeId::new("id3".to_owned()),
        );
        let cmd6 = Command::new(
            vec![],
            RequestWithToken::new(RequestWrapper::LeaseRevokeRequest(LeaseRevokeRequest {
                id: 1,
            })),
            ProposeId::new("id3".to_owned()),
        );

        let lease_grant_cmd = Command::new(
            vec![],
            RequestWithToken::new(RequestWrapper::LeaseGrantRequest(LeaseGrantRequest {
                ttl: 1,
                id: 123,
            })),
            ProposeId::new("id4".to_owned()),
        );
        let put_with_lease_cmd = Command::new(
            vec![KeyRange::new_one_key("foo")],
            RequestWithToken::new(RequestWrapper::PutRequest(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                lease: 123,
                ..Default::default()
            })),
            ProposeId::new("id5".to_owned()),
        );
        let txn_with_lease_id_cmd = Command::new(
            vec![KeyRange::new_one_key("key")],
            RequestWithToken::new(RequestWrapper::TxnRequest(TxnRequest {
                compare: vec![],
                success: vec![RequestOp {
                    request: Some(Request::RequestPut(PutRequest {
                        key: b"key".to_vec(),
                        value: b"value".to_vec(),
                        lease: 123,
                        ..Default::default()
                    })),
                }],
                failure: vec![],
            })),
            ProposeId::new("id6".to_owned()),
        );
        let lease_leases_cmd = Command::new(
            vec![],
            RequestWithToken::new(RequestWrapper::LeaseLeasesRequest(LeaseLeasesRequest {})),
            ProposeId::new("id4".to_owned()),
        );

        assert!(lease_grant_cmd.is_conflict(&put_with_lease_cmd)); // lease id
        assert!(lease_grant_cmd.is_conflict(&txn_with_lease_id_cmd)); // lease id
        assert!(put_with_lease_cmd.is_conflict(&txn_with_lease_id_cmd)); // lease id
        assert!(cmd1.is_conflict(&cmd2)); // id
        assert!(cmd1.is_conflict(&cmd3)); // keys
        assert!(!cmd2.is_conflict(&cmd3)); // auth read and kv
        assert!(cmd2.is_conflict(&cmd4)); // auth and auth
        assert!(cmd5.is_conflict(&cmd6)); // lease id
        assert!(lease_leases_cmd.is_conflict(&cmd5)); // lease read and write
        assert!(cmd6.is_conflict(&lease_leases_cmd)); // lease read and write
    }
}
