use std::{
    collections::{HashSet, VecDeque},
    ops::{Bound, RangeBounds},
};

use curp::{client::ClientApi, cmd::Command as CurpCommand};
use curp_external_api::cmd::{ConflictCheck, PbCodec, PbSerializeError};
use itertools::Itertools;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::{
    execute_error::ExecuteError, AuthInfo, PbCommand, PbCommandResponse, PbKeyRange,
    PbSyncResponse, Request, RequestWrapper, ResponseWrapper,
};

/// The curp client trait object on the command of xline
/// TODO: use `type CurpClient = impl ClientApi<...>` when `type_alias_impl_trait` stabilized
pub type CurpClient = dyn ClientApi<Error = tonic::Status, Cmd = Command> + Sync + Send + 'static;

/// Range start and end to get all keys
const UNBOUNDED: &[u8] = &[0_u8];
/// Range end to get one key
const ONE_KEY: &[u8] = &[];

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

impl From<PbKeyRange> for KeyRange {
    #[inline]
    fn from(range: PbKeyRange) -> Self {
        Self::new(range.key, range.range_end)
    }
}

impl From<KeyRange> for PbKeyRange {
    #[inline]
    fn from(range: KeyRange) -> Self {
        Self {
            key: range.range_start().to_vec(),
            range_end: range.range_end().to_vec(),
        }
    }
}

/// Command to run consensus protocol
#[cfg_attr(test, derive(PartialEq))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command {
    /// Request data
    request: RequestWrapper,
    /// Keys of request
    keys: Vec<KeyRange>,
    /// Compact Id
    compact_id: u64,
    /// Auth info
    auth_info: Option<AuthInfo>,
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
        | RequestWrapper::LeaseLeasesRequest(_)
        | RequestWrapper::AlarmRequest(_) => HashSet::new(),
    }
}

impl ConflictCheck for Command {
    #[inline]
    fn is_conflict(&self, other: &Self) -> bool {
        let this_req = &self.request;
        let other_req = &other.request;
        // auth read request will not conflict with any request except the auth write request
        if (this_req.is_auth_read_request() && other_req.is_auth_read_request())
            || (this_req.is_kv_request() && other_req.is_auth_read_request())
            || (this_req.is_auth_read_request() && other_req.is_kv_request())
        {
            return false;
        }
        // any two requests that don't meet the above conditions will conflict with each other
        // because the auth write request will make all previous token invalid
        if this_req.is_auth_request()
            || other_req.is_auth_request()
            || this_req.is_alarm_request()
            || other_req.is_alarm_request()
        {
            return true;
        }

        // Lease leases request is conflict with Lease grant and revoke requests
        if (this_req.is_lease_read_request() && other_req.is_lease_write_request())
            || (this_req.is_lease_write_request() && other_req.is_lease_read_request())
        {
            return true;
        }

        if this_req.is_compaction_request() && other_req.is_compaction_request() {
            return true;
        }

        if (this_req.is_txn_request() && other_req.is_compaction_request())
            || (this_req.is_compaction_request() && other_req.is_txn_request())
        {
            match (this_req, other_req) {
                (
                    &RequestWrapper::CompactionRequest(ref com_req),
                    &RequestWrapper::TxnRequest(ref txn_req),
                )
                | (
                    &RequestWrapper::TxnRequest(ref txn_req),
                    &RequestWrapper::CompactionRequest(ref com_req),
                ) => {
                    let target_revision = com_req.revision;
                    return txn_req.is_conflict_with_rev(target_revision)
                }
                _ => unreachable!("The request must be either a transaction or a compaction request! \nthis_req = {this_req:?} \nother_req = {other_req:?}")
            }
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
    pub fn new(keys: Vec<KeyRange>, request: RequestWrapper) -> Self {
        Self {
            request,
            keys,
            compact_id: 0,
            auth_info: None,
        }
    }

    /// New `Command` with auth info
    #[must_use]
    #[inline]
    pub fn new_with_auth_info(
        keys: Vec<KeyRange>,
        request: RequestWrapper,
        auth_info: Option<AuthInfo>,
    ) -> Self {
        Self {
            request,
            keys,
            compact_id: 0,
            auth_info,
        }
    }

    /// With `compact_id``
    #[must_use]
    #[inline]
    pub fn with_compact_id(mut self, compact_id: u64) -> Self {
        self.compact_id = compact_id;
        self
    }

    /// Get compact id
    #[must_use]
    #[inline]
    pub fn compact_id(&self) -> u64 {
        self.compact_id
    }

    /// get request
    #[must_use]
    #[inline]
    pub fn request(&self) -> &RequestWrapper {
        &self.request
    }

    /// get auth_info
    #[must_use]
    #[inline]
    pub fn auth_info(&self) -> Option<&AuthInfo> {
        self.auth_info.as_ref()
    }

    /// set auth_info
    #[inline]
    pub fn set_auth_info(&mut self, auth_info: AuthInfo) {
        self.auth_info = Some(auth_info)
    }

    /// need check quota
    #[must_use]
    #[inline]
    pub fn need_check_quota(&self) -> bool {
        matches!(
            self.request,
            RequestWrapper::LeaseGrantRequest(_)
                | RequestWrapper::PutRequest(_)
                | RequestWrapper::TxnRequest(_)
        )
    }
}

/// Command to run consensus protocol
#[cfg_attr(test, derive(PartialEq))]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandResponse {
    /// Response data
    response: ResponseWrapper,
}

impl PbCodec for CommandResponse {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        PbCommandResponse {
            response_wrapper: Some(self.response.clone()),
        }
        .encode_to_vec()
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError> {
        let pb_cmd_resp = PbCommandResponse::decode(buf)?;

        Ok(CommandResponse {
            response: pb_cmd_resp
                .response_wrapper
                .ok_or(PbSerializeError::EmptyField)?,
        })
    }
}

impl CommandResponse {
    /// New `ResponseOp` from `CommandResponse`
    #[inline]
    #[must_use]
    pub fn new(response: ResponseWrapper) -> Self {
        Self { response }
    }

    /// Decode `CommandResponse` and get `ResponseOp`
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> ResponseWrapper {
        self.response
    }
}

/// Sync Response
#[cfg_attr(test, derive(PartialEq, Eq))]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    /// Revision of this request
    revision: i64,
}
impl SyncResponse {
    /// New `SyncRequest`
    #[inline]
    #[must_use]
    pub fn new(revision: i64) -> Self {
        Self { revision }
    }

    /// Get revision field
    #[inline]
    #[must_use]
    pub fn revision(self) -> i64 {
        self.revision
    }
}

impl From<PbSyncResponse> for SyncResponse {
    #[inline]
    fn from(resp: PbSyncResponse) -> Self {
        Self {
            revision: resp.revision,
        }
    }
}

impl From<SyncResponse> for PbSyncResponse {
    #[inline]
    fn from(resp: SyncResponse) -> Self {
        Self {
            revision: resp.revision,
        }
    }
}

impl PbCodec for SyncResponse {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        PbSyncResponse::from(*self).encode_to_vec()
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError> {
        Ok(PbSyncResponse::decode(buf)?.into())
    }
}

#[async_trait::async_trait]
impl CurpCommand for Command {
    type Error = ExecuteError;
    type K = KeyRange;
    type PR = i64;
    type ER = CommandResponse;
    type ASR = SyncResponse;

    #[inline]
    fn keys(&self) -> &[Self::K] {
        self.keys.as_slice()
    }
}

impl PbCodec for Command {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        let rpc_cmd = PbCommand {
            keys: self.keys.iter().cloned().map(Into::into).collect(),
            compact_id: self.compact_id,
            auth_info: self.auth_info.clone(),
            request_wrapper: Some(self.request.clone()),
        };
        rpc_cmd.encode_to_vec()
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<Self, PbSerializeError> {
        let rpc_cmd = PbCommand::decode(buf)?;
        Ok(Self {
            keys: rpc_cmd.keys.into_iter().map(Into::into).collect(),
            compact_id: rpc_cmd.compact_id,
            auth_info: rpc_cmd.auth_info,
            request: rpc_cmd
                .request_wrapper
                .ok_or(PbSerializeError::EmptyField)?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        AuthEnableRequest, AuthStatusRequest, CommandKeys, CompactionRequest, Compare,
        LeaseGrantRequest, LeaseLeasesRequest, LeaseRevokeRequest, PutRequest, PutResponse,
        RangeRequest, RequestOp, TxnRequest,
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
            RequestWrapper::PutRequest(PutRequest::default()),
        );
        let cmd2 = Command::new(
            vec![],
            RequestWrapper::AuthStatusRequest(AuthStatusRequest::default()),
        );
        let cmd3 = Command::new(
            vec![KeyRange::new("c", "g")],
            RequestWrapper::PutRequest(PutRequest::default()),
        );
        let cmd4 = Command::new(
            vec![],
            RequestWrapper::AuthEnableRequest(AuthEnableRequest::default()),
        );
        let cmd5 = Command::new(
            vec![],
            RequestWrapper::LeaseGrantRequest(LeaseGrantRequest { ttl: 1, id: 1 }),
        );
        let cmd6 = Command::new(
            vec![],
            RequestWrapper::LeaseRevokeRequest(LeaseRevokeRequest { id: 1 }),
        );

        let lease_grant_cmd = Command::new(
            vec![],
            RequestWrapper::LeaseGrantRequest(LeaseGrantRequest { ttl: 1, id: 123 }),
        );
        let put_with_lease_cmd = Command::new(
            vec![KeyRange::new_one_key("foo")],
            RequestWrapper::PutRequest(PutRequest {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                lease: 123,
                ..Default::default()
            }),
        );
        let txn_with_lease_id_cmd = Command::new(
            vec![KeyRange::new_one_key("key")],
            RequestWrapper::TxnRequest(TxnRequest {
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
            }),
        );
        let lease_leases_cmd = Command::new(
            vec![],
            RequestWrapper::LeaseLeasesRequest(LeaseLeasesRequest {}),
        );

        assert!(lease_grant_cmd.is_conflict(&put_with_lease_cmd)); // lease id
        assert!(lease_grant_cmd.is_conflict(&txn_with_lease_id_cmd)); // lease id
        assert!(put_with_lease_cmd.is_conflict(&txn_with_lease_id_cmd)); // lease id
        assert!(cmd1.is_conflict(&cmd3)); // keys
        assert!(!cmd2.is_conflict(&cmd3)); // auth read and kv
        assert!(cmd2.is_conflict(&cmd4)); // auth and auth
        assert!(cmd5.is_conflict(&cmd6)); // lease id
        assert!(lease_leases_cmd.is_conflict(&cmd5)); // lease read and write
        assert!(cmd6.is_conflict(&lease_leases_cmd)); // lease read and write
    }

    fn generate_txn_command(
        keys: Vec<KeyRange>,
        compare: Vec<Compare>,
        success: Vec<RequestOp>,
        failure: Vec<RequestOp>,
    ) -> Command {
        Command::new(
            keys,
            RequestWrapper::TxnRequest(TxnRequest {
                compare,
                success,
                failure,
            }),
        )
    }

    #[test]
    fn test_compaction_txn_conflict() {
        let compaction_cmd_1 = Command::new(
            vec![],
            RequestWrapper::CompactionRequest(CompactionRequest {
                revision: 3,
                physical: false,
            }),
        );

        let compaction_cmd_2 = Command::new(
            vec![],
            RequestWrapper::CompactionRequest(CompactionRequest {
                revision: 5,
                physical: false,
            }),
        );

        let txn_with_lease_id_cmd = generate_txn_command(
            vec![KeyRange::new_one_key("key")],
            vec![],
            vec![RequestOp {
                request: Some(Request::RequestPut(PutRequest {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                    lease: 123,
                    ..Default::default()
                })),
            }],
            vec![],
        );

        let txn_cmd_1 = generate_txn_command(
            vec![KeyRange::new_one_key("key")],
            vec![],
            vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest {
                    key: b"key".to_vec(),
                    ..Default::default()
                })),
            }],
            vec![],
        );

        let txn_cmd_2 = generate_txn_command(
            vec![KeyRange::new_one_key("key")],
            vec![],
            vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest {
                    key: b"key".to_vec(),
                    revision: 3,
                    ..Default::default()
                })),
            }],
            vec![],
        );

        let txn_cmd_3 = generate_txn_command(
            vec![KeyRange::new_one_key("key")],
            vec![],
            vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest {
                    key: b"key".to_vec(),
                    revision: 7,
                    ..Default::default()
                })),
            }],
            vec![],
        );

        assert!(compaction_cmd_1.is_conflict(&compaction_cmd_2));
        assert!(compaction_cmd_2.is_conflict(&compaction_cmd_1));
        assert!(!compaction_cmd_1.is_conflict(&txn_with_lease_id_cmd));
        assert!(!compaction_cmd_2.is_conflict(&txn_with_lease_id_cmd));

        assert!(!compaction_cmd_2.is_conflict(&txn_cmd_1));
        assert!(compaction_cmd_2.is_conflict(&txn_cmd_2));
        assert!(!compaction_cmd_2.is_conflict(&txn_cmd_3));
    }

    #[test]
    fn command_serialization_is_ok() {
        let cmd = Command::new(
            vec![KeyRange::new("a", "e")],
            RequestWrapper::PutRequest(PutRequest::default()),
        );
        let decoded_cmd =
            <Command as PbCodec>::decode(&cmd.encode()).expect("decode should success");
        assert_eq!(cmd, decoded_cmd);
    }

    #[test]
    fn command_resp_serialization_is_ok() {
        let cmd_resp = CommandResponse::new(ResponseWrapper::PutResponse(PutResponse::default()));
        let decoded_cmd_resp = <CommandResponse as PbCodec>::decode(&cmd_resp.encode())
            .expect("decode should success");
        assert_eq!(cmd_resp, decoded_cmd_resp);
    }

    #[test]
    fn sync_resp_serialization_is_ok() {
        let sync_resp = SyncResponse::new(1);
        let decoded_sync_resp =
            <SyncResponse as PbCodec>::decode(&sync_resp.encode()).expect("decode should success");
        assert_eq!(sync_resp, decoded_sync_resp);
    }

    #[test]
    fn txn_command_keys_is_ok() {
        let txn_req = TxnRequest {
            compare: vec![Compare {
                key: b"a".to_vec(),
                ..Default::default()
            }],
            success: vec![
                RequestOp {
                    request: Some(Request::RequestRange(RangeRequest {
                        key: b"1".to_vec(),
                        ..Default::default()
                    })),
                },
                RequestOp {
                    request: Some(Request::RequestTxn(TxnRequest {
                        compare: vec![Compare {
                            key: b"b".to_vec(),
                            range_end: b"e".to_vec(),
                            ..Default::default()
                        }],
                        success: vec![RequestOp {
                            request: Some(Request::RequestRange(RangeRequest {
                                key: b"3".to_vec(),
                                range_end: b"4".to_vec(),
                                ..Default::default()
                            })),
                        }],
                        failure: vec![],
                    })),
                },
            ],
            failure: vec![RequestOp {
                request: Some(Request::RequestPut(PutRequest {
                    key: b"2".to_vec(),
                    ..Default::default()
                })),
            }],
        };

        let keys = txn_req.keys();
        assert!(keys.contains(&KeyRange::new_one_key("a")));
        assert!(keys.contains(&KeyRange::new("b", "e")));
        assert!(keys.contains(&KeyRange::new_one_key("1")));
        assert!(keys.contains(&KeyRange::new_one_key("2")));
        assert!(keys.contains(&KeyRange::new("3", "4")));
    }
}
