use std::collections::HashSet;

use curp::{client::ClientApi, cmd::Command as CurpCommand};
use curp_external_api::cmd::{ConflictCheck, PbCodec, PbSerializeError};
use itertools::Itertools;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::keyrange::KeyRange;
use crate::{
    execute_error::ExecuteError, AuthInfo, PbCommand, PbCommandResponse, PbSyncResponse,
    RequestWrapper, ResponseWrapper,
};

/// The curp client trait object on the command of xline
///
/// TODO: use `type CurpClient = impl ClientApi<...>` when `type_alias_impl_trait` stabilized
pub type CurpClient = dyn ClientApi<Error = tonic::Status, Cmd = Command> + Sync + Send + 'static;

/// Command to run consensus protocol
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Command {
    /// Request data
    request: RequestWrapper,
    /// Compact Id
    compact_id: u64,
    /// Auth info
    auth_info: Option<AuthInfo>,
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

        let this_lease_ids = this_req.leases().into_iter().collect::<HashSet<_>>();
        let other_lease_ids = other_req.leases().into_iter().collect::<HashSet<_>>();
        let lease_conflict = !this_lease_ids.is_disjoint(&other_lease_ids);
        let key_conflict = self
            .keys()
            .iter()
            .cartesian_product(other.keys().iter())
            .any(|(k1, k2)| k1.is_conflict(k2));
        lease_conflict || key_conflict
    }
}

impl Command {
    /// New `Command`
    #[must_use]
    #[inline]
    pub fn new(request: RequestWrapper) -> Self {
        Self {
            request,
            compact_id: 0,
            auth_info: None,
        }
    }

    /// New `Command` with auth info
    #[must_use]
    #[inline]
    pub fn new_with_auth_info(request: RequestWrapper, auth_info: Option<AuthInfo>) -> Self {
        Self {
            request,
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
    fn keys(&self) -> Vec<Self::K> {
        self.request().keys()
    }

    #[inline]
    fn is_read_only(&self) -> bool {
        self.request().is_read_only()
    }
}

impl Command {
    /// Get leases of the command
    pub fn leases(&self) -> Vec<i64> {
        self.request().leases()
    }
}

impl PbCodec for Command {
    #[inline]
    fn encode(&self) -> Vec<u8> {
        let rpc_cmd = PbCommand {
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
        AuthEnableRequest, AuthStatusRequest, CommandAttr, CompactionRequest, Compare,
        DeleteRangeRequest, LeaseGrantRequest, LeaseLeasesRequest, LeaseRevokeRequest, PutRequest,
        PutResponse, RangeRequest, Request, RequestOp, TxnRequest,
    };

    #[test]
    fn test_command_conflict() {
        let cmd1 = Command::new(RequestWrapper::DeleteRangeRequest(DeleteRangeRequest {
            key: "a".into(),
            range_end: "e".into(),
            ..Default::default()
        }));
        let cmd2 = Command::new(RequestWrapper::AuthStatusRequest(
            AuthStatusRequest::default(),
        ));
        let cmd3 = Command::new(RequestWrapper::DeleteRangeRequest(DeleteRangeRequest {
            key: "c".into(),
            range_end: "g".into(),
            ..Default::default()
        }));
        let cmd4 = Command::new(RequestWrapper::AuthEnableRequest(
            AuthEnableRequest::default(),
        ));
        let cmd5 = Command::new(RequestWrapper::LeaseGrantRequest(LeaseGrantRequest {
            ttl: 1,
            id: 1,
        }));
        let cmd6 = Command::new(RequestWrapper::LeaseRevokeRequest(LeaseRevokeRequest {
            id: 1,
        }));

        let lease_grant_cmd = Command::new(RequestWrapper::LeaseGrantRequest(LeaseGrantRequest {
            ttl: 1,
            id: 123,
        }));
        let put_with_lease_cmd = Command::new(RequestWrapper::PutRequest(PutRequest {
            key: b"foo".to_vec(),
            value: b"value".to_vec(),
            lease: 123,
            ..Default::default()
        }));
        let txn_with_lease_id_cmd = Command::new(RequestWrapper::TxnRequest(TxnRequest {
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
        }));
        let lease_leases_cmd =
            Command::new(RequestWrapper::LeaseLeasesRequest(LeaseLeasesRequest {}));

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
        compare: Vec<Compare>,
        success: Vec<RequestOp>,
        failure: Vec<RequestOp>,
    ) -> Command {
        Command::new(RequestWrapper::TxnRequest(TxnRequest {
            compare,
            success,
            failure,
        }))
    }

    #[test]
    fn test_compaction_txn_conflict() {
        let compaction_cmd_1 = Command::new(RequestWrapper::CompactionRequest(CompactionRequest {
            revision: 3,
            physical: false,
        }));

        let compaction_cmd_2 = Command::new(RequestWrapper::CompactionRequest(CompactionRequest {
            revision: 5,
            physical: false,
        }));

        let txn_with_lease_id_cmd = generate_txn_command(
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
        let cmd = Command::new(RequestWrapper::PutRequest(PutRequest::default()));
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
        assert!(keys.contains(&KeyRange::new_etcd("b", "e")));
        assert!(keys.contains(&KeyRange::new_one_key("1")));
        assert!(keys.contains(&KeyRange::new_one_key("2")));
        assert!(keys.contains(&KeyRange::new_etcd("3", "4")));
    }
}
