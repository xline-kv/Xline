use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use anyhow::Result;
use clippy_utilities::{Cast, OverflowArithmetic};
use curp::{cmd::ProposeId, error::ExecuteError};
use jsonwebtoken::{
    errors::Error as JwtError, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use log::debug;
use parking_lot::Mutex;
use pbkdf2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Pbkdf2,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::index::IndexOperate;
use super::{db::DB, index::Index};
use crate::rpc::{
    AuthDisableRequest, AuthDisableResponse, AuthEnableRequest, AuthEnableResponse,
    AuthRoleAddRequest, AuthRoleAddResponse, AuthRoleDeleteRequest, AuthRoleDeleteResponse,
    AuthRoleGetRequest, AuthRoleGetResponse, AuthRoleGrantPermissionRequest,
    AuthRoleGrantPermissionResponse, AuthRoleListRequest, AuthRoleListResponse,
    AuthRoleRevokePermissionRequest, AuthRoleRevokePermissionResponse, AuthStatusRequest,
    AuthStatusResponse, AuthUserAddRequest, AuthUserAddResponse, AuthUserChangePasswordRequest,
    AuthUserChangePasswordResponse, AuthUserDeleteRequest, AuthUserDeleteResponse,
    AuthUserGetRequest, AuthUserGetResponse, AuthUserGrantRoleRequest, AuthUserGrantRoleResponse,
    AuthUserListRequest, AuthUserListResponse, AuthUserRevokeRoleRequest,
    AuthUserRevokeRoleResponse, AuthenticateRequest, AuthenticateResponse, DeleteRangeRequest,
    KeyValue, Permission, PutRequest, RangeRequest, Request, RequestWithToken, RequestWrapper,
    ResponseHeader, ResponseWrapper, Role, TxnRequest, Type, User,
};
use crate::server::command::{
    CommandResponse, ExecutionRequest, KeyRange, SyncRequest, SyncResponse,
};

/// Default channel size
const CHANNEL_SIZE: usize = 128;
/// Key prefix of user
const USER_PREFIX: &[u8] = b"user/";
/// Key prefix of role
const ROLE_PREFIX: &[u8] = b"role/";
/// Key of `AuthEnable`
const AUTH_ENABLE_KEY: &[u8] = b"auth_enable";
/// Root user
const ROOT_USER: &str = "root";
/// Root role
const ROOT_ROLE: &str = "root";
/// default tolen ttl
const DEFAULT_TOKEN_TTL: u64 = 300;

/// Auth store
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct AuthStore {
    /// Auth store Backend
    inner: Arc<AuthStoreBackend>,
    // TODO: check if this can be moved into Inner
    /// Sender to send command
    exec_tx: mpsc::Sender<ExecutionRequest>,
    /// Sender to send sync request
    sync_tx: mpsc::Sender<SyncRequest>,
}

/// Auth store inner
pub(crate) struct AuthStoreBackend {
    /// Key Index
    index: Index,
    /// DB to store key value
    db: DB,
    /// Revision
    revision: Mutex<i64>,
    /// Speculative execution pool. Mapping from propose id to request
    sp_exec_pool: Mutex<HashMap<ProposeId, RequestWithToken>>,
    /// Enabled
    enabled: Mutex<bool>,
    /// Permission cache
    permission_cache: Mutex<HashMap<String, UserPermissions>>,
    /// The manager of token
    token_manager: Option<JwtTokenManager>,
}

impl fmt::Debug for AuthStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthStoreBackend")
            .field("index", &self.index)
            .field("db", &self.db)
            .field("revision", &self.revision)
            .field("sp_exec_pool", &self.sp_exec_pool)
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl AuthStore {
    /// New `AuthStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new(key_pair: Option<(EncodingKey, DecodingKey)>) -> Self {
        let (exec_tx, mut exec_rx) = mpsc::channel(CHANNEL_SIZE);
        let (sync_tx, mut sync_rx) = mpsc::channel(CHANNEL_SIZE);
        let inner = Arc::new(AuthStoreBackend::new(key_pair));

        let inner_clone = Arc::clone(&inner);
        let _handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    cmd_req = exec_rx.recv() => {
                        if let Some(req) = cmd_req {
                            inner.speculative_exec(req);
                        }
                    }
                    sync_req = sync_rx.recv() => {
                        if let Some(req) = sync_req {
                            inner.sync_cmd(req).await;
                        }
                    }
                }
            }
        });

        Self {
            inner: inner_clone,
            exec_tx,
            sync_tx,
        }
    }

    /// Send execution request to Auth store
    pub(crate) async fn send_req(
        &self,
        id: ProposeId,
        req: RequestWithToken,
    ) -> oneshot::Receiver<Result<CommandResponse, ExecuteError>> {
        let (req, receiver) = ExecutionRequest::new(id, req);
        assert!(
            self.exec_tx.send(req).await.is_ok(),
            "Command receiver dropped"
        );
        receiver
    }

    /// Send sync request to Auth store
    pub(crate) async fn send_sync(&self, propose_id: ProposeId) -> oneshot::Receiver<SyncResponse> {
        let (req, receiver) = SyncRequest::new(propose_id);
        assert!(
            self.sync_tx.send(req).await.is_ok(),
            "Command receiver dropped"
        );
        receiver
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
    pub(crate) fn check_permission(&self, wrapper: &RequestWithToken) -> Result<(), ExecuteError> {
        if !self.inner.is_enabled() {
            return Ok(());
        }
        if let RequestWrapper::AuthenticateRequest(_) = wrapper.request {
            return Ok(());
        }
        let claims = match wrapper.token {
            Some(ref token) => self.inner.verify_token(token)?,
            None => {
                return Err(ExecuteError::InvalidCommand(
                    "token is not provided".to_owned(),
                ))
            }
        };
        if claims.revision < self.inner.revision() {
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
                self.check_put_permission(&username, put_req)?;
            }
            RequestWrapper::DeleteRangeRequest(ref del_range_req) => {
                self.check_delete_permission(&username, del_range_req)?;
            }
            RequestWrapper::TxnRequest(ref txn_req) => {
                self.check_txn_permission(&username, txn_req)?;
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
    fn check_put_permission(&self, username: &str, req: &PutRequest) -> Result<(), ExecuteError> {
        if req.prev_kv {
            self.check_op_permission(username, &req.key, &[], Type::Read)?;
        }
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
    fn check_txn_permission(&self, username: &str, req: &TxnRequest) -> Result<(), ExecuteError> {
        for compare in &req.compare {
            self.check_op_permission(username, &compare.key, &compare.range_end, Type::Read)?;
        }
        for op in req.success.iter().chain(req.failure.iter()) {
            match op.request {
                Some(Request::RequestRange(ref range_req)) => {
                    self.check_range_permission(username, range_req)?;
                }
                Some(Request::RequestPut(ref put_req)) => {
                    self.check_put_permission(username, put_req)?;
                }
                Some(Request::RequestDeleteRange(ref del_range_req)) => {
                    self.check_delete_permission(username, del_range_req)?;
                }
                Some(Request::RequestTxn(ref txn_req)) => {
                    self.check_txn_permission(username, txn_req)?;
                }
                None => unreachable!("txn operation should have request"),
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
        Err(ExecuteError::InvalidCommand("premission denied".to_owned()))
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
        let user_perms = self.inner.get_user_permissions(username)?;
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
}

impl AuthStoreBackend {
    /// New `AuthStoreBackend`
    pub(crate) fn new(key_pair: Option<(EncodingKey, DecodingKey)>) -> Self {
        Self {
            index: Index::new(),
            db: DB::new(),
            revision: Mutex::new(1),
            sp_exec_pool: Mutex::new(HashMap::new()),
            enabled: Mutex::new(false),
            token_manager: key_pair.map(|(encoding_key, decoding_key)| {
                JwtTokenManager::new(encoding_key, decoding_key)
            }),
            permission_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Get revision of Auth store
    pub(crate) fn revision(&self) -> i64 {
        *self.revision.lock()
    }

    /// Get enabled of Auth store
    fn is_enabled(&self) -> bool {
        *self.enabled.lock()
    }

    /// Check password
    fn check_password(&self, username: &str, password: &str) -> Result<i64, ExecuteError> {
        if !self.is_enabled() {
            return Err(ExecuteError::InvalidCommand(
                "auth is not enabled".to_owned(),
            ));
        }
        let user = self.get_user(username)?;
        let need_password = user.options.as_ref().map_or(true, |o| !o.no_password);
        if !need_password {
            return Err(ExecuteError::InvalidCommand(
                "password was given for no password user".to_owned(),
            ));
        }

        let hash = String::from_utf8_lossy(&user.password);
        let hash = PasswordHash::new(&hash)
            .unwrap_or_else(|e| panic!("Failed to parse password hash, error: {e}"));
        Pbkdf2
            .verify_password(password.as_bytes(), &hash)
            .map_err(|e| ExecuteError::InvalidCommand(format!("verify password error: {e}")))?;

        Ok(self.revision())
    }

    /// Assign token
    fn assign(&self, username: &str) -> Result<String, ExecuteError> {
        match self.token_manager {
            Some(ref token_manager) => token_manager
                .assign(username, self.revision())
                .map_err(|e| ExecuteError::InvalidCommand(format!("assign token error: {e}"))),
            None => Err(ExecuteError::InvalidCommand(
                "token_manager is not initialized".to_owned(),
            )),
        }
    }

    /// verify token
    fn verify_token(&self, token: &str) -> Result<TokenClaims, ExecuteError> {
        match self.token_manager {
            Some(ref token_manager) => token_manager
                .verify(token)
                .map_err(|e| ExecuteError::InvalidCommand(format!("verify token error: {e}"))),
            None => Err(ExecuteError::InvalidCommand(
                "token_manager is not initialized".to_owned(),
            )),
        }
    }

    /// refresh permission cache
    fn refresh_permission_cache(&self) {
        let mut permission_cache = HashMap::new();
        for user in self.get_all_users() {
            let mut user_permisiion = UserPermissions::new();
            for role_name in user.roles {
                let role = match self.get_role(&role_name) {
                    Ok(role) => role,
                    Err(_) => continue,
                };
                for permission in role.key_permission {
                    let key_range = KeyRange {
                        start: permission.key,
                        end: permission.range_end,
                    };
                    #[allow(clippy::unwrap_used)] // safe unwrap
                    match Type::from_i32(permission.perm_type).unwrap() {
                        Type::Readwrite => {
                            user_permisiion.read.push(key_range.clone());
                            user_permisiion.write.push(key_range.clone());
                        }
                        Type::Write => {
                            user_permisiion.write.push(key_range.clone());
                        }
                        Type::Read => {
                            user_permisiion.read.push(key_range.clone());
                        }
                    }
                }
            }
            if !user_permisiion.is_empty() {
                let username = String::from_utf8_lossy(&user.name).to_string();
                let _ignore = permission_cache.insert(username, user_permisiion);
            }
        }
        *self.permission_cache.lock() = permission_cache;
    }

    /// get user permissions
    fn get_user_permissions(&self, username: &str) -> Result<UserPermissions, ExecuteError> {
        let permission_cache = self.permission_cache.lock();
        match permission_cache.get(username) {
            Some(user_permissions) => Ok(user_permissions.clone()),
            None => Err(ExecuteError::InvalidCommand(
                "user permissions not found".to_owned(),
            )),
        }
    }

    /// get `KeyValue` in `AuthStore`
    fn get(&self, key: &[u8]) -> Option<KeyValue> {
        let revisions = self.index.get(key, &[], 0);
        assert!(revisions.len() <= 1);
        self.db.get_values(&revisions).pop()
    }

    /// get user by username
    fn get_user(&self, username: &str) -> Result<User, ExecuteError> {
        let key = [USER_PREFIX, username.as_bytes()].concat();
        match self.get(&key) {
            Some(kv) => Ok(User::decode(kv.value.as_slice()).unwrap_or_else(|e| {
                panic!(
                    "Failed to decode user from kv value, error: {:?}, kv: {:?}",
                    e, kv
                )
            })),
            None => Err(ExecuteError::InvalidCommand("user not found".to_owned())),
        }
    }

    /// get role by rolename
    fn get_role(&self, rolename: &str) -> Result<Role, ExecuteError> {
        let key = [ROLE_PREFIX, rolename.as_bytes()].concat();
        match self.get(&key) {
            Some(kv) => Ok(Role::decode(kv.value.as_slice()).unwrap_or_else(|e| {
                panic!(
                    "Failed to decode role from kv value, error: {:?}, kv: {:?}",
                    e, kv
                )
            })),
            None => Err(ExecuteError::InvalidCommand("role not found".to_owned())),
        }
    }

    /// get `KeyValue` to `AuthStore`
    fn put(&self, key: Vec<u8>, value: Vec<u8>, revision: i64, sub_revision: i64) {
        let new_rev = self
            .index
            .insert_or_update_revision(&key, revision, sub_revision);
        let kv = KeyValue {
            key,
            value,
            create_revision: new_rev.create_revision,
            mod_revision: new_rev.mod_revision,
            version: new_rev.version,
            ..KeyValue::default()
        };
        let _prev = self.db.insert(new_rev.as_revision(), kv);
    }

    /// put user to `AuthStore`
    fn put_user(&self, user: &User, revision: i64, sub_revision: i64) {
        let key = [USER_PREFIX, &user.name].concat();
        let value = user.encode_to_vec();
        self.put(key, value, revision, sub_revision);
    }

    /// put role to `AuthStore`
    fn put_role(&self, role: &Role, revision: i64, sub_revision: i64) {
        let key = [ROLE_PREFIX, &role.name].concat();
        let value = role.encode_to_vec();
        self.put(key, value, revision, sub_revision);
    }

    /// delete `KeyValue` in `AuthStore`
    fn delete(&self, key: &[u8], revision: i64, sub_revision: i64) {
        let revisions = self.index.delete(key, &[], revision, sub_revision);
        let _prev_kv = self.db.mark_deletions(&revisions);
    }

    /// delete user in `AuthStore`
    fn delete_user(&self, username: &str, revision: i64, sub_revision: i64) {
        let key = [USER_PREFIX, username.as_bytes()].concat();
        self.delete(&key, revision, sub_revision);
    }

    /// delete role in `AuthStore`
    fn delete_role(&self, rolename: &str, revision: i64, sub_revision: i64) {
        let key = [ROLE_PREFIX, rolename.as_bytes()].concat();
        self.delete(&key, revision, sub_revision);
    }

    /// Get all users in the `AuthStore`
    fn get_all_users(&self) -> Vec<User> {
        let range_end = KeyRange::get_prefix(USER_PREFIX);
        let revisions = self.index.get(USER_PREFIX, &range_end, 0);
        self.db
            .get_values(&revisions)
            .into_iter()
            .map(|kv| {
                User::decode(kv.value.as_slice()).unwrap_or_else(|e| {
                    panic!(
                        "Failed to decode user from kv value, error: {:?}, kv: {:?}",
                        e, kv
                    )
                })
            })
            .collect()
    }

    /// Get all roles in the `AuthStore`
    fn get_all_roles(&self) -> Vec<Role> {
        let range_end = KeyRange::get_prefix(ROLE_PREFIX);
        let revisions = self.index.get(ROLE_PREFIX, &range_end, 0);
        self.db
            .get_values(&revisions)
            .into_iter()
            .map(|kv| {
                Role::decode(kv.value.as_slice()).unwrap_or_else(|e| {
                    panic!(
                        "Failed to decode role from kv value, error: {:?}, kv: {:?}",
                        e, kv
                    )
                })
            })
            .collect()
    }

    /// speculative execute command
    pub(crate) fn speculative_exec(&self, execution_req: ExecutionRequest) {
        debug!("Receive Execution Request {:?}", execution_req);
        let (id, req, res_sender) = execution_req.unpack();
        let result = self
            .handle_auth_req(id, &req)
            .map(|res| CommandResponse::new(&res));
        assert!(res_sender.send(result).is_ok(), "Failed to send response");
    }

    /// Handle `InternalRequest`
    fn handle_auth_req(
        &self,
        id: ProposeId,
        wrapper: &RequestWithToken,
    ) -> Result<ResponseWrapper, ExecuteError> {
        let _prev = self.sp_exec_pool.lock().insert(id, wrapper.clone());
        #[allow(clippy::wildcard_enum_match_arm)]
        let response = match wrapper.request {
            RequestWrapper::AuthEnableRequest(ref req) => {
                ResponseWrapper::AuthEnableResponse(self.handle_auth_enable_request(req)?)
            }
            RequestWrapper::AuthDisableRequest(ref req) => {
                ResponseWrapper::AuthDisableResponse(self.handle_auth_disable_request(req))
            }
            RequestWrapper::AuthStatusRequest(ref req) => {
                ResponseWrapper::AuthStatusResponse(self.handle_auth_status_request(req))
            }
            RequestWrapper::AuthUserAddRequest(ref req) => {
                ResponseWrapper::AuthUserAddResponse(self.handle_user_add_request(req)?)
            }
            RequestWrapper::AuthUserGetRequest(ref req) => {
                ResponseWrapper::AuthUserGetResponse(self.handle_user_get_request(req)?)
            }
            RequestWrapper::AuthUserListRequest(ref req) => {
                ResponseWrapper::AuthUserListResponse(self.handle_user_list_request(req))
            }
            RequestWrapper::AuthUserGrantRoleRequest(ref req) => {
                ResponseWrapper::AuthUserGrantRoleResponse(
                    self.handle_user_grant_role_request(req)?,
                )
            }
            RequestWrapper::AuthUserRevokeRoleRequest(ref req) => {
                ResponseWrapper::AuthUserRevokeRoleResponse(
                    self.handle_user_revoke_role_request(req)?,
                )
            }
            RequestWrapper::AuthUserChangePasswordRequest(ref req) => {
                ResponseWrapper::AuthUserChangePasswordResponse(
                    self.handle_user_change_password_request(req)?,
                )
            }
            RequestWrapper::AuthUserDeleteRequest(ref req) => {
                ResponseWrapper::AuthUserDeleteResponse(self.handle_user_delete_request(req)?)
            }
            RequestWrapper::AuthRoleAddRequest(ref req) => {
                ResponseWrapper::AuthRoleAddResponse(self.handle_role_add_request(req)?)
            }
            RequestWrapper::AuthRoleGetRequest(ref req) => {
                ResponseWrapper::AuthRoleGetResponse(self.handle_role_get_request(req)?)
            }
            RequestWrapper::AuthRoleGrantPermissionRequest(ref req) => {
                ResponseWrapper::AuthRoleGrantPermissionResponse(
                    self.handle_role_grant_permission_request(req)?,
                )
            }
            RequestWrapper::AuthRoleRevokePermissionRequest(ref req) => {
                ResponseWrapper::AuthRoleRevokePermissionResponse(
                    self.handle_role_revoke_permission_request(req)?,
                )
            }
            RequestWrapper::AuthRoleDeleteRequest(ref req) => {
                ResponseWrapper::AuthRoleDeleteResponse(self.handle_role_delete_request(req)?)
            }
            RequestWrapper::AuthRoleListRequest(ref req) => {
                ResponseWrapper::AuthRoleListResponse(self.handle_role_list_request(req))
            }
            RequestWrapper::AuthenticateRequest(ref req) => {
                ResponseWrapper::AuthenticateResponse(self.handle_authenticate_request(req)?)
            }
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        };
        Ok(response)
    }

    /// Handle `AuthEnableRequest`
    fn handle_auth_enable_request(
        &self,
        _req: &AuthEnableRequest,
    ) -> Result<AuthEnableResponse, ExecuteError> {
        debug!("handle_auth_enable");
        let res = Ok(AuthEnableResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        });
        if self.is_enabled() {
            debug!("auth is already enabled");
            return res;
        }
        let user = self
            .get_user(ROOT_USER)
            .map_err(|_ignore| ExecuteError::InvalidCommand("root user is not exist".to_owned()))?;
        if user.roles.binary_search(&ROOT_ROLE.to_owned()).is_err() {
            return Err(ExecuteError::InvalidCommand(
                "root user does not have root role".to_owned(),
            ));
        }
        res
    }

    /// Handle `AuthDisableRequest`
    fn handle_auth_disable_request(&self, _req: &AuthDisableRequest) -> AuthDisableResponse {
        debug!("handle_auth_disable");
        if !self.is_enabled() {
            debug!("auth is already disabled");
        }
        AuthDisableResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        }
    }

    /// Handle `AuthStatusRequest`
    fn handle_auth_status_request(&self, _req: &AuthStatusRequest) -> AuthStatusResponse {
        debug!("handle_auth_status");
        AuthStatusResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            auth_revision: self.revision().cast(),
            enabled: self.is_enabled(),
        }
    }

    /// Handle `AuthenticateRequest`
    fn handle_authenticate_request(
        &self,
        req: &AuthenticateRequest,
    ) -> Result<AuthenticateResponse, ExecuteError> {
        debug!("handle_authenticate_request");
        if !self.is_enabled() {
            return Err(ExecuteError::InvalidCommand(
                "auth is not enabled".to_owned(),
            ));
        }
        let token = self.assign(&req.name)?;
        Ok(AuthenticateResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            token,
        })
    }

    /// Handle `AuthUserAddRequest`
    fn handle_user_add_request(
        &self,
        req: &AuthUserAddRequest,
    ) -> Result<AuthUserAddResponse, ExecuteError> {
        debug!("handle_user_add_request");
        if self.get_user(&req.name).is_ok() {
            return Err(ExecuteError::InvalidCommand(
                "user already exists".to_owned(),
            ));
        }
        Ok(AuthUserAddResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthUserGetRequest`
    fn handle_user_get_request(
        &self,
        req: &AuthUserGetRequest,
    ) -> Result<AuthUserGetResponse, ExecuteError> {
        debug!("handle_user_add_request");
        let user = self.get_user(&req.name)?;
        Ok(AuthUserGetResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            roles: user.roles,
        })
    }

    /// Handle `AuthUserListRequest`
    fn handle_user_list_request(&self, _req: &AuthUserListRequest) -> AuthUserListResponse {
        debug!("handle_user_list_request");
        let users = self
            .get_all_users()
            .into_iter()
            .map(|u| String::from_utf8_lossy(&u.name).to_string())
            .collect();
        AuthUserListResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            users,
        }
    }

    /// Handle `AuthUserDeleteRequest`
    fn handle_user_delete_request(
        &self,
        req: &AuthUserDeleteRequest,
    ) -> Result<AuthUserDeleteResponse, ExecuteError> {
        debug!("handle_user_delete_request");
        if self.is_enabled() && (req.name == ROOT_USER) {
            return Err(ExecuteError::InvalidCommand(
                "root user cannot be deleted".to_owned(),
            ));
        }
        let _user = self.get_user(&req.name)?;
        Ok(AuthUserDeleteResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthUserChangePasswordRequest`
    fn handle_user_change_password_request(
        &self,
        req: &AuthUserChangePasswordRequest,
    ) -> Result<AuthUserChangePasswordResponse, ExecuteError> {
        debug!("handle_user_change_password_request");
        let user = self.get_user(&req.name)?;
        let need_password = user.options.as_ref().map_or(true, |o| !o.no_password);
        if need_password && req.hashed_password.is_empty() {
            return Err(ExecuteError::InvalidCommand(
                "password is required but not provided".to_owned(),
            ));
        }
        Ok(AuthUserChangePasswordResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthUserGrantRoleRequest`
    fn handle_user_grant_role_request(
        &self,
        req: &AuthUserGrantRoleRequest,
    ) -> Result<AuthUserGrantRoleResponse, ExecuteError> {
        debug!("handle_user_grant_role_request");
        let _user = self.get_user(&req.user)?;
        if req.role != ROOT_ROLE {
            let _role = self.get_role(&req.role)?;
        }
        Ok(AuthUserGrantRoleResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthUserRevokeRoleRequest`
    fn handle_user_revoke_role_request(
        &self,
        req: &AuthUserRevokeRoleRequest,
    ) -> Result<AuthUserRevokeRoleResponse, ExecuteError> {
        debug!("handle_user_revoke_role_request");
        if self.is_enabled() && (req.name == ROOT_USER) && (req.role == ROOT_ROLE) {
            return Err(ExecuteError::InvalidCommand(
                "root user cannot revoke root role".to_owned(),
            ));
        }
        let user = self.get_user(&req.name)?;
        if user.roles.binary_search(&req.role).is_err() {
            return Err(ExecuteError::InvalidCommand(
                "role is not granted to the user".to_owned(),
            ));
        }
        Ok(AuthUserRevokeRoleResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthRoleAddRequest`
    fn handle_role_add_request(
        &self,
        req: &AuthRoleAddRequest,
    ) -> Result<AuthRoleAddResponse, ExecuteError> {
        debug!("handle_role_add_request");
        if self.get_role(&req.name).is_ok() {
            return Err(ExecuteError::InvalidCommand(
                "role already exists".to_owned(),
            ));
        }
        Ok(AuthRoleAddResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthRoleGetRequest`
    fn handle_role_get_request(
        &self,
        req: &AuthRoleGetRequest,
    ) -> Result<AuthRoleGetResponse, ExecuteError> {
        debug!("handle_role_get_request");
        let role = self.get_role(&req.role)?;
        let perm = if role.name == ROOT_ROLE.as_bytes() {
            vec![Permission {
                #[allow(clippy::as_conversions)] // This cast is always valid
                perm_type: Type::Readwrite as i32,
                key: vec![],
                range_end: vec![0],
            }]
        } else {
            role.key_permission
        };
        Ok(AuthRoleGetResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            perm,
        })
    }

    /// Handle `AuthRoleListRequest`
    fn handle_role_list_request(&self, _req: &AuthRoleListRequest) -> AuthRoleListResponse {
        debug!("handle_role_list_request");
        let roles = self
            .get_all_roles()
            .into_iter()
            .map(|r| String::from_utf8_lossy(&r.name).to_string())
            .collect();
        AuthRoleListResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
            roles,
        }
    }

    /// Handle `UserRoleDeleteRequest`
    fn handle_role_delete_request(
        &self,
        req: &AuthRoleDeleteRequest,
    ) -> Result<AuthRoleDeleteResponse, ExecuteError> {
        debug!("handle_role_delete_request");
        if self.is_enabled() && req.role == ROOT_ROLE {
            return Err(ExecuteError::InvalidCommand(
                "root role cannot be deleted".to_owned(),
            ));
        }
        let _role = self.get_role(&req.role)?;
        Ok(AuthRoleDeleteResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthRoleGrantPermissionRequest`
    fn handle_role_grant_permission_request(
        &self,
        req: &AuthRoleGrantPermissionRequest,
    ) -> Result<AuthRoleGrantPermissionResponse, ExecuteError> {
        debug!("handle_role_grant_permission_request");
        let _role = self.get_role(&req.name)?;
        Ok(AuthRoleGrantPermissionResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Handle `AuthRoleRevokePermissionRequest`
    fn handle_role_revoke_permission_request(
        &self,
        req: &AuthRoleRevokePermissionRequest,
    ) -> Result<AuthRoleRevokePermissionResponse, ExecuteError> {
        debug!("handle_role_revoke_permission_request");
        let role = self.get_role(&req.role)?;
        if role
            .key_permission
            .binary_search_by(|p| match p.key.cmp(&req.key) {
                Ordering::Equal => p.range_end.cmp(&req.range_end),
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            })
            .is_err()
        {
            return Err(ExecuteError::InvalidCommand(
                "permission not granted to the role".to_owned(),
            ));
        }
        Ok(AuthRoleRevokePermissionResponse {
            header: Some(ResponseHeader {
                revision: -1,
                ..ResponseHeader::default()
            }),
        })
    }

    /// Sync a Command to storage and generate revision for Command.
    async fn sync_cmd(&self, sync_req: SyncRequest) {
        debug!("Receive SyncRequest {:?}", sync_req);
        let (propose_id, res_sender) = sync_req.unpack();
        let requests = self
            .sp_exec_pool
            .lock()
            .remove(&propose_id)
            .unwrap_or_else(|| {
                panic!(
                    "Failed to get speculative execution propose id {:?}",
                    propose_id
                );
            });
        let revision = self.sync_request(requests);
        assert!(
            res_sender.send(SyncResponse::new(revision)).is_ok(),
            "Failed to send response"
        );
    }

    /// Sync `RequestWrapper`
    fn sync_request(&self, wrapper: RequestWithToken) -> i64 {
        let revision = *self.revision.lock();
        let next_revision = revision.overflow_add(1);
        #[allow(clippy::wildcard_enum_match_arm)]
        let modify = match wrapper.request {
            RequestWrapper::AuthEnableRequest(req) => {
                debug!("Sync AuthEnableRequest {:?}", req);
                self.sync_auth_enable_request(&req, next_revision)
            }
            RequestWrapper::AuthDisableRequest(req) => {
                debug!("Sync AuthDisableRequest {:?}", req);
                self.sync_auth_disable_request(&req, next_revision)
            }
            RequestWrapper::AuthStatusRequest(req) => {
                debug!("Sync AuthStatusRequest {:?}", req);
                Self::sync_auth_status_request(&req)
            }
            RequestWrapper::AuthUserAddRequest(req) => {
                debug!("Sync AuthUserAddRequest {:?}", req);
                self.sync_user_add_request(req, next_revision)
            }
            RequestWrapper::AuthUserGetRequest(req) => {
                debug!("Sync AuthUserGetRequest {:?}", req);
                Self::sync_user_get_request(&req)
            }
            RequestWrapper::AuthUserListRequest(req) => {
                debug!("Sync AuthUserListRequest {:?}", req);
                Self::sync_user_list_request(&req)
            }
            RequestWrapper::AuthUserGrantRoleRequest(req) => {
                debug!("Sync AuthUserGrantRoleRequest {:?}", req);
                self.sync_user_grant_role_request(req, next_revision)
            }
            RequestWrapper::AuthUserRevokeRoleRequest(req) => {
                debug!("Sync AuthUserRevokeRoleRequest {:?}", req);
                self.sync_user_revoke_role_request(&req, next_revision)
            }
            RequestWrapper::AuthUserChangePasswordRequest(req) => {
                debug!("Sync AuthUserChangePasswordRequest {:?}", req);
                self.sync_user_change_password_request(req, next_revision)
            }
            RequestWrapper::AuthUserDeleteRequest(req) => {
                debug!("Sync AuthUserDeleteRequest {:?}", req);
                self.sync_user_delete_request(&req, next_revision)
            }

            RequestWrapper::AuthRoleAddRequest(req) => {
                debug!("Sync AuthRoleAddRequest {:?}", req);
                self.sync_role_add_request(req, next_revision)
            }
            RequestWrapper::AuthRoleGetRequest(req) => {
                debug!("Sync AuthRoleGetRequest {:?}", req);
                Self::sync_role_get_request(&req)
            }
            RequestWrapper::AuthRoleGrantPermissionRequest(req) => {
                debug!("Sync AuthRoleGrantPermissionRequest {:?}", req);
                self.sync_role_grant_permission_request(req, next_revision)
            }
            RequestWrapper::AuthRoleRevokePermissionRequest(req) => {
                debug!("Sync AuthRoleRevokePermissionRequest {:?}", req);
                self.sync_role_revoke_permission_request(&req, next_revision)
            }
            RequestWrapper::AuthRoleListRequest(req) => {
                debug!("Sync AuthRoleListRequest {:?}", req);
                Self::sync_role_list_request(&req)
            }
            RequestWrapper::AuthRoleDeleteRequest(req) => {
                debug!("Sync AuthRoleDeleteRequest {:?}", req);
                self.sync_role_delete_request(&req, next_revision)
            }
            RequestWrapper::AuthenticateRequest(req) => {
                debug!("Sync AuthenticateRequest {:?}", req);
                Self::sync_authenticate_request(&req)
            }
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        };
        if modify {
            *self.revision.lock() = next_revision;
            next_revision
        } else {
            revision
        }
    }

    /// Sync `AuthEnableRequest` and return whether authstore is changed.
    fn sync_auth_enable_request(&self, _req: &AuthEnableRequest, revision: i64) -> bool {
        if self.is_enabled() {
            return false;
        }
        let user = match self.get_user(ROOT_USER) {
            Ok(user) => user,
            Err(_) => return false,
        };
        if user.roles.binary_search(&ROOT_ROLE.to_owned()).is_err() {
            return false;
        }
        self.put(AUTH_ENABLE_KEY.to_vec(), vec![1], revision, 0);
        *self.enabled.lock() = true;
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthDisableRequest` and return whether authstore is changed.
    fn sync_auth_disable_request(&self, _req: &AuthDisableRequest, revision: i64) -> bool {
        if !self.is_enabled() {
            return false;
        }
        self.put(AUTH_ENABLE_KEY.to_vec(), vec![0], revision, 0);
        *self.enabled.lock() = false;
        true
    }

    /// Sync `AuthStatusRequest` and return whether authstore is changed.
    fn sync_auth_status_request(_req: &AuthStatusRequest) -> bool {
        false
    }

    /// Sync `AuthenticateRequest` and return whether authstore is changed.
    fn sync_authenticate_request(_req: &AuthenticateRequest) -> bool {
        false
    }

    /// Sync `AuthUserAddRequest` and return whether authstore is changed.
    fn sync_user_add_request(&self, req: AuthUserAddRequest, revision: i64) -> bool {
        if self.get_user(&req.name).is_ok() {
            return false;
        }
        let user = User {
            name: req.name.into_bytes(),
            password: req.hashed_password.into_bytes(),
            options: req.options,
            roles: Vec::new(),
        };
        self.put_user(&user, revision, 0);
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthUserGetRequest` and return whether authstore is changed.
    fn sync_user_get_request(_req: &AuthUserGetRequest) -> bool {
        false
    }

    /// Sync `AuthUserListRequest` and return whether authstore is changed.
    fn sync_user_list_request(_req: &AuthUserListRequest) -> bool {
        false
    }

    /// Sync `AuthUserDeleteRequest` and return whether authstore is changed.
    fn sync_user_delete_request(&self, req: &AuthUserDeleteRequest, next_revision: i64) -> bool {
        if self.is_enabled() && req.name == ROOT_USER {
            return false;
        }
        if self.get_user(&req.name).is_err() {
            return false;
        }
        self.delete_user(&req.name, next_revision, 0);
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthUserChangePasswordRequest` and return whether authstore is changed.
    fn sync_user_change_password_request(
        &self,
        req: AuthUserChangePasswordRequest,
        revision: i64,
    ) -> bool {
        let mut user = match self.get_user(&req.name) {
            Ok(user) => user,
            Err(_) => return false,
        };
        let need_password = user.options.as_ref().map_or(true, |o| !o.no_password);
        if need_password && req.hashed_password.is_empty() {
            return false;
        }
        user.password = req.hashed_password.into_bytes();
        self.put_user(&user, revision, 0);
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthUserGrantRoleRequest` and return whether authstore is changed.
    fn sync_user_grant_role_request(&self, req: AuthUserGrantRoleRequest, revision: i64) -> bool {
        let mut user = match self.get_user(&req.user) {
            Ok(user) => user,
            Err(_) => return false,
        };
        if (req.role != ROOT_ROLE) && self.get_role(&req.role).is_err() {
            return false;
        }
        // TODO get index from binary search and inset directly
        if user.roles.binary_search(&req.role).is_ok() {
            return false;
        }
        user.roles.push(req.role);
        user.roles.sort();
        self.put_user(&user, revision, 0);
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthUserRevokeRoleRequest` and return whether authstore is changed.
    fn sync_user_revoke_role_request(
        &self,
        req: &AuthUserRevokeRoleRequest,
        revision: i64,
    ) -> bool {
        if self.is_enabled() && (req.name == ROOT_USER) && (req.role == ROOT_ROLE) {
            return false;
        }
        let mut user = match self.get_user(&req.name) {
            Ok(user) => user,
            Err(_) => return false,
        };
        let idx = match user.roles.binary_search(&req.role) {
            Ok(idx) => idx,
            Err(_) => return false,
        };
        let _ignore = user.roles.remove(idx);
        self.put_user(&user, revision, 0);
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthRoleAddRequest` and return whether authstore is changed.
    fn sync_role_add_request(&self, req: AuthRoleAddRequest, revision: i64) -> bool {
        if self.get_role(&req.name).is_ok() {
            return false;
        }
        let role = Role {
            name: req.name.into_bytes(),
            key_permission: Vec::new(),
        };
        self.put_role(&role, revision, 0);
        true
    }

    /// Sync `AuthRoleGetRequest` and return whether authstore is changed.
    fn sync_role_get_request(_req: &AuthRoleGetRequest) -> bool {
        false
    }

    /// Sync `AuthRoleListRequest` and return whether authstore is changed.
    fn sync_role_list_request(_req: &AuthRoleListRequest) -> bool {
        false
    }

    /// Sync `AuthRoleDeleteRequest` and return whether authstore is changed.
    fn sync_role_delete_request(&self, req: &AuthRoleDeleteRequest, revision: i64) -> bool {
        if self.is_enabled() && req.role == ROOT_ROLE {
            return false;
        }
        if self.get_role(&req.role).is_err() {
            return false;
        }
        self.delete_role(&req.role, revision, 0);
        let users = self.get_all_users();
        let mut sub_revision = 1;
        for mut user in users {
            if let Ok(idx) = user.roles.binary_search(&req.role) {
                let _ignore = user.roles.remove(idx);
                self.put_user(&user, revision, sub_revision);
                sub_revision = sub_revision.wrapping_add(1);
            }
        }
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthRoleGrantPermissionRequest` and return whether authstore is changed.
    fn sync_role_grant_permission_request(
        &self,
        req: AuthRoleGrantPermissionRequest,
        revision: i64,
    ) -> bool {
        let mut role = match self.get_role(&req.name) {
            Ok(role) => role,
            Err(_) => return false,
        };
        let permission = match req.perm {
            Some(perm) => perm,
            None => return false,
        };

        #[allow(clippy::indexing_slicing)] // this index is always valid
        match role
            .key_permission
            .binary_search_by(|p| match p.key.cmp(&permission.key) {
                Ordering::Equal => p.range_end.cmp(&permission.range_end),
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            }) {
            Ok(idx) => {
                role.key_permission[idx].perm_type = permission.perm_type;
            }
            Err(idx) => {
                role.key_permission.insert(idx, permission);
            }
        };
        self.put_role(&role, revision, 0);
        self.refresh_permission_cache();
        true
    }

    /// Sync `AuthRoleRevokePermissionRequest` and return whether authstore is changed.
    fn sync_role_revoke_permission_request(
        &self,
        req: &AuthRoleRevokePermissionRequest,
        next_revision: i64,
    ) -> bool {
        let mut role = match self.get_role(&req.role) {
            Ok(role) => role,
            Err(_) => return false,
        };
        let idx = match role
            .key_permission
            .binary_search_by(|p| match p.key.cmp(&req.key) {
                Ordering::Equal => p.range_end.cmp(&req.range_end),
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            }) {
            Ok(idx) => idx,
            Err(_) => return false,
        };
        let _ignore = role.key_permission.remove(idx);
        self.put_role(&role, next_revision, 0);
        self.refresh_permission_cache();
        true
    }
}

/// Claims of Token
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TokenClaims {
    /// Username
    username: String,
    /// Revision
    revision: i64,
    /// Expiration
    exp: u64,
}

/// Operations of token manager
trait TokenOperate {
    /// Claims type
    type Claims;

    /// Error type
    type Error;

    /// Assign a token with claims.
    fn assign(&self, username: &str, revision: i64) -> Result<String, Self::Error>;

    /// Verify token and return claims.
    fn verify(&self, token: &str) -> Result<Self::Claims, Self::Error>;
}

/// `TokenManager` of Json Web Token.
struct JwtTokenManager {
    /// The key used to sign the token.
    encoding_key: EncodingKey,
    /// The key used to verify the token.
    decoding_key: DecodingKey,
}

impl JwtTokenManager {
    /// New `JwtTokenManager`
    fn new(encoding_key: EncodingKey, decoding_key: DecodingKey) -> Self {
        Self {
            encoding_key,
            decoding_key,
        }
    }
}

impl TokenOperate for JwtTokenManager {
    type Error = JwtError;

    type Claims = TokenClaims;

    fn assign(&self, username: &str, revision: i64) -> Result<String, Self::Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| panic!("SystemTime before UNIX EPOCH! {}", e))
            .as_secs();
        let claims = TokenClaims {
            username: username.to_owned(),
            revision,
            exp: now.wrapping_add(DEFAULT_TOKEN_TTL),
        };
        let token =
            jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key)?;
        Ok(token)
    }

    fn verify(&self, token: &str) -> Result<Self::Claims, Self::Error> {
        jsonwebtoken::decode::<TokenClaims>(
            token,
            &self.decoding_key,
            &Validation::new(Algorithm::RS256),
        )
        .map(|d| d.claims)
    }
}

/// Permissions if a user
#[derive(Debug, Clone)]
struct UserPermissions {
    /// `KeyRange` has read permission
    read: Vec<KeyRange>,
    /// `KeyRange` has write permission
    write: Vec<KeyRange>,
}

impl UserPermissions {
    /// New `UserPermissions`
    fn new() -> Self {
        Self {
            read: Vec::new(),
            write: Vec::new(),
        }
    }

    /// Check if user permissions is empty
    fn is_empty(&self) -> bool {
        self.read.is_empty() && self.write.is_empty()
    }
}
