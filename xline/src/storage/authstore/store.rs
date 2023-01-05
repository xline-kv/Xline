use std::{collections::VecDeque, sync::Arc};

use anyhow::Result;
use curp::{cmd::ProposeId, error::ExecuteError};
use jsonwebtoken::{DecodingKey, EncodingKey};
use tokio::sync::mpsc;

use super::backend::{ROOT_ROLE, ROOT_USER};
use crate::{
    header_gen::HeaderGenerator,
    rpc::{
        DeleteRangeRequest, LeaseRevokeRequest, PutRequest, RangeRequest, Request, RequestOp,
        RequestWithToken, RequestWrapper, TxnRequest, Type,
    },
    server::command::{CommandResponse, KeyRange, SyncResponse},
    storage::{authstore::backend::AuthStoreBackend, leasestore::LeaseMessage},
};

/// Auth store
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct AuthStore {
    /// Auth store Backend
    inner: Arc<AuthStoreBackend>,
}

impl AuthStore {
    /// New `AuthStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new(
        lease_cmd_tx: mpsc::Sender<LeaseMessage>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
        header_gen: Arc<HeaderGenerator>,
    ) -> Self {
        Self {
            inner: Arc::new(AuthStoreBackend::new(lease_cmd_tx, key_pair, header_gen)),
        }
    }

    /// execute a auth request
    pub(crate) fn execute(
        &self,
        id: ProposeId,
        request: RequestWithToken,
    ) -> Result<CommandResponse, ExecuteError> {
        self.inner
            .handle_auth_req(id, request.request)
            .map(CommandResponse::new)
    }

    /// sync a auth request
    pub(crate) fn after_sync(&self, id: &ProposeId) -> SyncResponse {
        SyncResponse::new(self.inner.sync_request(id))
    }

    /// Auth revision
    pub(crate) fn revision(&self) -> i64 {
        self.inner.revision()
    }

    /// Check password
    pub(crate) fn check_password(
        &self,
        username: &str,
        password: &str,
    ) -> Result<i64, ExecuteError> {
        self.inner.check_password(username, password)
    }

    /// check if the request is permitted
    pub(crate) async fn check_permission(
        &self,
        wrapper: &RequestWithToken,
    ) -> Result<(), ExecuteError> {
        if !self.inner.is_enabled() {
            return Ok(());
        }
        if let RequestWrapper::AuthenticateRequest(_) = wrapper.request {
            return Ok(());
        }
        let claims = match wrapper.token {
            Some(ref token) => self.inner.verify_token(token)?,
            None => {
                // TODO: some requests are allowed without token when auth is enabled
                return Err(ExecuteError::InvalidCommand(
                    "token is not provided".to_owned(),
                ));
            }
        };
        if claims.revision < self.revision() {
            return Err(ExecuteError::InvalidCommand(
                "request's revision is older than current revision".to_owned(),
            ));
        }
        let username = claims.username;
        #[allow(clippy::wildcard_enum_match_arm)]
        match wrapper.request {
            RequestWrapper::RangeRequest(ref range_req) => {
                self.check_range_permission(&username, range_req)?;
            }
            RequestWrapper::PutRequest(ref put_req) => {
                self.check_put_permission(&username, put_req).await?;
            }
            RequestWrapper::DeleteRangeRequest(ref del_range_req) => {
                self.check_delete_permission(&username, del_range_req)?;
            }
            RequestWrapper::TxnRequest(ref txn_req) => {
                self.check_txn_permission(&username, txn_req).await?;
            }
            RequestWrapper::LeaseRevokeRequest(ref lease_revoke_req) => {
                self.check_lease_revoke_permission(&username, lease_revoke_req)
                    .await?;
            }
            RequestWrapper::AuthUserGetRequest(ref user_get_req) => {
                self.check_admin_permission(&username).map_or_else(
                    |e| {
                        if user_get_req.name == username {
                            Ok(())
                        } else {
                            Err(e)
                        }
                    },
                    |_| Ok(()),
                )?;
            }
            RequestWrapper::AuthRoleGetRequest(ref role_get_req) => {
                self.check_admin_permission(&username).map_or_else(
                    |e| {
                        let user = self.inner.get_user(&username)?;
                        if user.has_role(&role_get_req.role) {
                            Ok(())
                        } else {
                            Err(e)
                        }
                    },
                    |_| Ok(()),
                )?;
            }
            _ => {
                self.check_admin_permission(&username)?;
            }
        }

        Ok(())
    }

    /// check if range request is permitted
    fn check_range_permission(
        &self,
        username: &str,
        req: &RangeRequest,
    ) -> Result<(), ExecuteError> {
        self.check_op_permission(username, &req.key, &req.range_end, Type::Read)
    }

    /// check if put request is permitted
    async fn check_put_permission(
        &self,
        username: &str,
        req: &PutRequest,
    ) -> Result<(), ExecuteError> {
        if req.prev_kv {
            self.check_op_permission(username, &req.key, &[], Type::Read)?;
        }
        self.check_lease(username, req.lease).await?;
        self.check_op_permission(username, &req.key, &[], Type::Write)
    }

    /// check if delete request is permitted
    fn check_delete_permission(
        &self,
        username: &str,
        req: &DeleteRangeRequest,
    ) -> Result<(), ExecuteError> {
        if req.prev_kv {
            self.check_op_permission(username, &req.key, &req.range_end, Type::Read)?;
        }
        self.check_op_permission(username, &req.key, &req.range_end, Type::Write)
    }

    /// check if txn request is permitted
    async fn check_txn_permission(
        &self,
        username: &str,
        req: &TxnRequest,
    ) -> Result<(), ExecuteError> {
        let mut check_queque = VecDeque::new();
        let req = RequestOp {
            request: Some(Request::RequestTxn(req.clone())),
        };
        check_queque.push_back(&req);
        while let Some(req_op) = check_queque.pop_front() {
            match req_op.request {
                Some(Request::RequestRange(ref range_req)) => {
                    self.check_range_permission(username, range_req)?;
                }
                Some(Request::RequestPut(ref put_req)) => {
                    self.check_put_permission(username, put_req).await?;
                }
                Some(Request::RequestDeleteRange(ref del_range_req)) => {
                    self.check_delete_permission(username, del_range_req)?;
                }
                Some(Request::RequestTxn(ref txn_req)) => {
                    for compare in &txn_req.compare {
                        self.check_op_permission(
                            username,
                            &compare.key,
                            &compare.range_end,
                            Type::Read,
                        )?;
                    }
                    for op in txn_req.success.iter().chain(txn_req.failure.iter()) {
                        check_queque.push_back(op);
                    }
                }
                None => unreachable!("txn operation should have request"),
            }
        }
        Ok(())
    }

    /// check if lease revoke request is permitted
    async fn check_lease_revoke_permission(
        &self,
        username: &str,
        req: &LeaseRevokeRequest,
    ) -> Result<(), ExecuteError> {
        self.check_lease(username, req.id).await
    }

    /// check if user can revoke lease
    async fn check_lease(&self, username: &str, lease_id: i64) -> Result<(), ExecuteError> {
        let lease = self.inner.get_lease(lease_id).await;
        if let Some(lease) = lease {
            let keys = lease.keys();
            for key in keys {
                self.check_op_permission(username, &key, &[], Type::Write)?;
            }
        }
        Ok(())
    }

    /// Check if the user has admin permission
    fn check_admin_permission(&self, username: &str) -> Result<(), ExecuteError> {
        if !self.inner.is_enabled() {
            return Ok(());
        }
        let user = self.inner.get_user(username)?;
        if user.has_role(ROOT_ROLE) {
            return Ok(());
        }
        Err(ExecuteError::InvalidCommand("permission denied".to_owned()))
    }

    /// check permission for a kv operation
    fn check_op_permission(
        &self,
        username: &str,
        key: &[u8],
        range_end: &[u8],
        perm_type: Type,
    ) -> Result<(), ExecuteError> {
        let user = self.inner.get_user(username)?;
        if user.has_role(ROOT_ROLE) {
            return Ok(());
        }
        let user_perms = self.inner.get_user_permissions_from_cache(username)?;
        match perm_type {
            Type::Read => {
                if user_perms.read.iter().any(|kr| {
                    kr.contains_range(&KeyRange {
                        start: key.to_vec(),
                        end: range_end.to_vec(),
                    })
                }) {
                    return Ok(());
                }
            }
            Type::Write => {
                if user_perms.write.iter().any(|kr| {
                    kr.contains_range(&KeyRange {
                        start: key.to_vec(),
                        end: range_end.to_vec(),
                    })
                }) {
                    return Ok(());
                }
            }
            Type::Readwrite => {
                unreachable!("Readwrite is unreachable");
            }
        }
        Err(ExecuteError::InvalidCommand("premission denied".to_owned()))
    }

    /// Assign root token
    pub(crate) fn root_token(&self) -> Result<String, ExecuteError> {
        self.inner.assign(ROOT_USER)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, error::Error};

    use super::*;
    use crate::{
        rpc::{
            AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGrantPermissionRequest,
            AuthRoleRevokePermissionRequest, AuthUserAddRequest, AuthUserDeleteRequest,
            AuthUserGrantRoleRequest, Permission,
        },
        storage::authstore::perms::{PermissionCache, UserPermissions},
    };

    #[test]
    fn test_role_grant_permission() {
        let store = init_auth_store();
        let req = RequestWithToken::new(
            AuthRoleGrantPermissionRequest {
                name: "r".to_owned(),
                perm: Some(Permission {
                    #[allow(clippy::as_conversions)] // This cast is always valid
                    perm_type: Type::Write as i32,
                    key: "fop".into(),
                    range_end: "foz".into(),
                }),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req).is_ok());
        assert_eq!(
            store.inner.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([(
                    "u".to_owned(),
                    UserPermissions {
                        read: vec![KeyRange::new("foo", "")],
                        write: vec![KeyRange::new("foo", ""), KeyRange::new("fop", "foz")],
                    },
                )]),
                role_to_users_map: HashMap::from([("r".to_owned(), vec!["u".to_owned()])]),
            },
        );
    }

    #[test]
    fn test_role_revoke_permission() {
        let store = init_auth_store();
        let req = RequestWithToken::new(
            AuthRoleRevokePermissionRequest {
                role: "r".to_owned(),
                key: "foo".into(),
                range_end: "".into(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req).is_ok());
        assert_eq!(
            store.inner.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([("u".to_owned(), UserPermissions::new())]),
                role_to_users_map: HashMap::from([("r".to_owned(), vec!["u".to_owned()])]),
            },
        );
    }

    #[test]
    fn test_role_delete() {
        let store = init_auth_store();
        let req = RequestWithToken::new(
            AuthRoleDeleteRequest {
                role: "r".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req).is_ok());
        assert_eq!(
            store.inner.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([("u".to_owned(), UserPermissions::new(),)]),
                role_to_users_map: HashMap::new(),
            },
        );
    }

    #[test]
    fn test_user_delete() {
        let store = init_auth_store();
        let req = RequestWithToken::new(
            AuthUserDeleteRequest {
                name: "u".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req).is_ok());
        assert_eq!(
            store.inner.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::new(),
                role_to_users_map: HashMap::from([("r".to_owned(), vec![])]),
            },
        );
    }

    fn init_auth_store() -> AuthStore {
        let key_pair = test_key_pair();
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let (lease_cmd_tx, _) = mpsc::channel(1);
        let store = AuthStore::new(lease_cmd_tx, key_pair, header_gen);

        let req1 = RequestWithToken::new(
            AuthRoleAddRequest {
                name: "r".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req1).is_ok());
        let req2 = RequestWithToken::new(
            AuthUserAddRequest {
                name: "u".to_owned(),
                password: "".to_owned(),
                hashed_password: "123".to_owned(),
                options: None,
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req2).is_ok());
        let req3 = RequestWithToken::new(
            AuthUserGrantRoleRequest {
                user: "u".to_owned(),
                role: "r".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req3).is_ok());
        let req4 = RequestWithToken::new(
            AuthRoleGrantPermissionRequest {
                name: "r".to_owned(),
                perm: Some(Permission {
                    #[allow(clippy::as_conversions)] // This cast is always valid
                    perm_type: Type::Readwrite as i32,
                    key: b"foo".to_vec(),
                    range_end: vec![],
                }),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, req4).is_ok());
        assert_eq!(
            store.inner.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([(
                    "u".to_owned(),
                    UserPermissions {
                        read: vec![KeyRange::new("foo", "")],
                        write: vec![KeyRange::new("foo", "")],
                    },
                )]),
                role_to_users_map: HashMap::from([("r".to_owned(), vec!["u".to_owned()])]),
            },
        );
        store
    }

    fn exe_and_sync(
        store: &AuthStore,
        req: RequestWithToken,
    ) -> Result<(CommandResponse, SyncResponse), Box<dyn Error>> {
        let id = ProposeId::new("test-id".to_owned());
        let cmd_res = store.execute(id.clone(), req)?;
        let sync_res = store.after_sync(&id);
        Ok((cmd_res, sync_res))
    }

    fn test_key_pair() -> Option<(EncodingKey, DecodingKey)> {
        let private_key = include_bytes!("../../../tests/private.pem");
        let public_key = include_bytes!("../../../tests/public.pem");
        let encoding_key = EncodingKey::from_rsa_pem(private_key).ok()?;
        let decoding_key = DecodingKey::from_rsa_pem(public_key).ok()?;
        Some((encoding_key, decoding_key))
    }
}
