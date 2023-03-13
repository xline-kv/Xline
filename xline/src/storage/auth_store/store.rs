use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc,
    },
};

use clippy_utilities::Cast;
use engine::WriteOperation;
use itertools::Itertools;
use jsonwebtoken::{DecodingKey, EncodingKey};
use log::debug;
use parking_lot::RwLock;
use pbkdf2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Pbkdf2,
};
use prost::Message;
use tokio::sync::mpsc;
use utils::parking_lot_lock::RwLockMap;

use super::{
    backend::{AUTH_ENABLE_KEY, AUTH_REVISION_KEY, AUTH_TABLE, ROOT_ROLE, ROOT_USER, USER_TABLE},
    perms::{JwtTokenManager, PermissionCache, TokenClaims, TokenOperate, UserPermissions},
    ROLE_TABLE,
};
use crate::{
    header_gen::HeaderGenerator,
    revision_number::RevisionNumber,
    rpc::{
        AuthDisableRequest, AuthDisableResponse, AuthEnableRequest, AuthEnableResponse,
        AuthRoleAddRequest, AuthRoleAddResponse, AuthRoleDeleteRequest, AuthRoleDeleteResponse,
        AuthRoleGetRequest, AuthRoleGetResponse, AuthRoleGrantPermissionRequest,
        AuthRoleGrantPermissionResponse, AuthRoleListRequest, AuthRoleListResponse,
        AuthRoleRevokePermissionRequest, AuthRoleRevokePermissionResponse, AuthStatusRequest,
        AuthStatusResponse, AuthUserAddRequest, AuthUserAddResponse, AuthUserChangePasswordRequest,
        AuthUserChangePasswordResponse, AuthUserDeleteRequest, AuthUserDeleteResponse,
        AuthUserGetRequest, AuthUserGetResponse, AuthUserGrantRoleRequest,
        AuthUserGrantRoleResponse, AuthUserListRequest, AuthUserListResponse,
        AuthUserRevokeRoleRequest, AuthUserRevokeRoleResponse, AuthenticateRequest,
        AuthenticateResponse, DeleteRangeRequest, LeaseRevokeRequest, Permission, PutRequest,
        RangeRequest, Request, RequestOp, RequestWithToken, RequestWrapper, Role, TxnRequest, Type,
        User,
    },
    server::command::{CommandResponse, KeyRange, SyncResponse},
    storage::{
        auth_store::backend::AuthStoreBackend,
        lease_store::{Lease, LeaseMessage},
        storage_api::StorageApi,
        ExecuteError,
    },
};

/// Auth store
#[derive(Debug)]
pub(crate) struct AuthStore<S>
where
    S: StorageApi,
{
    /// Auth store Backend
    backend: Arc<AuthStoreBackend<S>>,
    /// Enabled
    enabled: AtomicBool,
    /// Revision
    revision: RevisionNumber,
    /// Lease command sender
    lease_cmd_tx: mpsc::Sender<LeaseMessage>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
    /// Permission cache
    permission_cache: RwLock<PermissionCache>,
    /// The manager of token
    token_manager: Option<JwtTokenManager>,
}

impl<S> AuthStore<S>
where
    S: StorageApi,
{
    /// New `AuthStore`
    #[allow(clippy::integer_arithmetic)] // Introduced by tokio::select!
    pub(crate) fn new(
        lease_cmd_tx: mpsc::Sender<LeaseMessage>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
        header_gen: Arc<HeaderGenerator>,
        storage: Arc<S>,
    ) -> Self {
        let backend = Arc::new(AuthStoreBackend::new(storage));
        Self {
            backend,
            enabled: AtomicBool::new(false),
            revision: RevisionNumber::default(),
            lease_cmd_tx,
            header_gen,
            permission_cache: RwLock::new(PermissionCache::new()),
            token_manager: key_pair.map(|(encoding_key, decoding_key)| {
                JwtTokenManager::new(encoding_key, decoding_key)
            }),
        }
    }

    /// generate a next revision and create a write operation to persistent it
    fn op_inc_and_put_revision(&self) -> WriteOperation {
        let rev = self.revision.next();
        WriteOperation::new_put(
            AUTH_TABLE,
            AUTH_REVISION_KEY.to_vec(),
            rev.to_le_bytes().to_vec(),
        )
    }

    /// `WriteOperation` of put auth enable
    fn op_put_auth_enable(enable: bool) -> WriteOperation {
        WriteOperation::new_put(AUTH_TABLE, AUTH_ENABLE_KEY.to_vec(), vec![u8::from(enable)])
    }

    /// `WriteOperation` of put user
    fn op_put_user(user: &User) -> WriteOperation {
        WriteOperation::new_put(USER_TABLE, user.name.clone(), user.encode_to_vec())
    }

    /// `WriteOperation` of put role
    fn op_put_role(role: &Role) -> WriteOperation {
        WriteOperation::new_put(ROLE_TABLE, role.name.clone(), role.encode_to_vec())
    }

    /// `WriteOperation` of delete user
    fn op_delete_user(user: &str) -> WriteOperation {
        WriteOperation::new_delete(USER_TABLE, user.as_bytes().to_vec())
    }

    /// `WriteOperation` of delete role
    fn op_delete_role(role: &str) -> WriteOperation {
        WriteOperation::new_delete(ROLE_TABLE, role.as_bytes().to_vec())
    }

    /// Get Lease by lease id
    async fn get_lease(&self, lease_id: i64) -> Option<Lease> {
        let (detach, rx) = LeaseMessage::look_up(lease_id);
        assert!(
            self.lease_cmd_tx.send(detach).await.is_ok(),
            "lease_cmd_tx is closed"
        );
        rx.await.unwrap_or_else(|_e| panic!("res sender is closed"))
    }

    /// Get enabled of Auth store
    pub(super) fn is_enabled(&self) -> bool {
        self.enabled.load(AtomicOrdering::Relaxed)
    }

    /// Assign token
    pub(crate) fn assign(&self, username: &str) -> Result<String, ExecuteError> {
        match self.token_manager {
            Some(ref token_manager) => token_manager
                .assign(username, self.revision())
                .map_err(|_ignore| ExecuteError::invalid_auth_token()),
            None => Err(ExecuteError::token_manager_not_init()),
        }
    }

    /// verify token
    pub(crate) fn verify_token(&self, token: &str) -> Result<TokenClaims, ExecuteError> {
        match self.token_manager {
            Some(ref token_manager) => token_manager
                .verify(token)
                .map_err(|_ignore| ExecuteError::invalid_auth_token()),
            None => Err(ExecuteError::token_manager_not_init()),
        }
    }

    /// create permission cache
    fn create_permission_cache(&self) -> Result<(), ExecuteError> {
        let mut permission_cache = PermissionCache::new();
        for user in self.backend.get_all_users()? {
            let user_permission = self.get_user_permissions(&user, None);
            let username = String::from_utf8_lossy(&user.name).to_string();
            let _ignore = permission_cache
                .user_permissions
                .insert(username.clone(), user_permission);
            for role in user.roles {
                permission_cache
                    .role_to_users_map
                    .entry(role)
                    .or_insert_with(Vec::new)
                    .push(username.clone());
            }
        }
        self.permission_cache
            .map_write(|mut cache| *cache = permission_cache);
        Ok(())
    }

    /// get user permissions
    pub(crate) fn get_user_permissions(
        &self,
        user: &User,
        skip_role: Option<&str>,
    ) -> UserPermissions {
        let mut user_permission = UserPermissions::new();
        for role_name in &user.roles {
            if skip_role.map_or(false, |r| r == role_name) {
                continue;
            }
            let Ok(role) = self.backend.get_role(role_name) else {
                continue;
            };
            for permission in role.key_permission {
                let key_range = KeyRange {
                    start: permission.key,
                    end: permission.range_end,
                };
                #[allow(clippy::unwrap_used)] // safe unwrap
                match Type::from_i32(permission.perm_type).unwrap() {
                    Type::Readwrite => {
                        user_permission.read.push(key_range.clone());
                        user_permission.write.push(key_range.clone());
                    }
                    Type::Write => {
                        user_permission.write.push(key_range.clone());
                    }
                    Type::Read => {
                        user_permission.read.push(key_range.clone());
                    }
                }
            }
        }
        user_permission
    }

    /// execute a auth request
    pub(crate) fn execute(
        &self,
        request: &RequestWithToken,
    ) -> Result<CommandResponse, ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        let res = match request.request {
            RequestWrapper::AuthEnableRequest(ref req) => {
                self.handle_auth_enable_request(req).map(Into::into)
            }
            RequestWrapper::AuthDisableRequest(ref req) => {
                Ok(self.handle_auth_disable_request(req).into())
            }
            RequestWrapper::AuthStatusRequest(ref req) => {
                Ok(self.handle_auth_status_request(req).into())
            }
            RequestWrapper::AuthUserAddRequest(ref req) => {
                self.handle_user_add_request(req).map(Into::into)
            }
            RequestWrapper::AuthUserGetRequest(ref req) => {
                self.handle_user_get_request(req).map(Into::into)
            }
            RequestWrapper::AuthUserListRequest(ref req) => {
                self.handle_user_list_request(req).map(Into::into)
            }
            RequestWrapper::AuthUserGrantRoleRequest(ref req) => {
                self.handle_user_grant_role_request(req).map(Into::into)
            }
            RequestWrapper::AuthUserRevokeRoleRequest(ref req) => {
                self.handle_user_revoke_role_request(req).map(Into::into)
            }
            RequestWrapper::AuthUserChangePasswordRequest(ref req) => self
                .handle_user_change_password_request(req)
                .map(Into::into),
            RequestWrapper::AuthUserDeleteRequest(ref req) => {
                self.handle_user_delete_request(req).map(Into::into)
            }
            RequestWrapper::AuthRoleAddRequest(ref req) => {
                self.handle_role_add_request(req).map(Into::into)
            }
            RequestWrapper::AuthRoleGetRequest(ref req) => {
                self.handle_role_get_request(req).map(Into::into)
            }
            RequestWrapper::AuthRoleGrantPermissionRequest(ref req) => self
                .handle_role_grant_permission_request(req)
                .map(Into::into),
            RequestWrapper::AuthRoleRevokePermissionRequest(ref req) => self
                .handle_role_revoke_permission_request(req)
                .map(Into::into),
            RequestWrapper::AuthRoleDeleteRequest(ref req) => {
                self.handle_role_delete_request(req).map(Into::into)
            }
            RequestWrapper::AuthRoleListRequest(ref req) => {
                self.handle_role_list_request(req).map(Into::into)
            }
            RequestWrapper::AuthenticateRequest(ref req) => {
                self.handle_authenticate_request(req).map(Into::into)
            }
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        };
        res.map(CommandResponse::new)
    }

    /// Handle `AuthEnableRequest`
    fn handle_auth_enable_request(
        &self,
        _req: &AuthEnableRequest,
    ) -> Result<AuthEnableResponse, ExecuteError> {
        debug!("handle_auth_enable");
        let res = Ok(AuthEnableResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        });
        if self.is_enabled() {
            debug!("auth is already enabled");
            return res;
        }
        let user = self.backend.get_user(ROOT_USER)?;
        if user.roles.binary_search(&ROOT_ROLE.to_owned()).is_err() {
            return Err(ExecuteError::root_role_not_exist());
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
            header: Some(self.header_gen.gen_header_without_revision()),
        }
    }

    /// Handle `AuthStatusRequest`
    fn handle_auth_status_request(&self, _req: &AuthStatusRequest) -> AuthStatusResponse {
        debug!("handle_auth_status");
        AuthStatusResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
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
            return Err(ExecuteError::auth_not_enabled());
        }
        let token = self.assign(&req.name)?;
        Ok(AuthenticateResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            token,
        })
    }

    /// Handle `AuthUserAddRequest`
    fn handle_user_add_request(
        &self,
        req: &AuthUserAddRequest,
    ) -> Result<AuthUserAddResponse, ExecuteError> {
        debug!("handle_user_add_request");
        if self.backend.get_user(&req.name).is_ok() {
            return Err(ExecuteError::user_already_exists(&req.name));
        }
        Ok(AuthUserAddResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthUserGetRequest`
    fn handle_user_get_request(
        &self,
        req: &AuthUserGetRequest,
    ) -> Result<AuthUserGetResponse, ExecuteError> {
        debug!("handle_user_add_request");
        let user = self.backend.get_user(&req.name)?;
        Ok(AuthUserGetResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            roles: user.roles,
        })
    }

    /// Handle `AuthUserListRequest`
    fn handle_user_list_request(
        &self,
        _req: &AuthUserListRequest,
    ) -> Result<AuthUserListResponse, ExecuteError> {
        debug!("handle_user_list_request");
        let users = self
            .backend
            .get_all_users()?
            .into_iter()
            .map(|u| String::from_utf8_lossy(&u.name).to_string())
            .collect();
        Ok(AuthUserListResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            users,
        })
    }

    /// Handle `AuthUserDeleteRequest`
    fn handle_user_delete_request(
        &self,
        req: &AuthUserDeleteRequest,
    ) -> Result<AuthUserDeleteResponse, ExecuteError> {
        debug!("handle_user_delete_request");
        if self.is_enabled() && (req.name == ROOT_USER) {
            return Err(ExecuteError::invalid_auth_management());
        }
        let _user = self.backend.get_user(&req.name)?;
        Ok(AuthUserDeleteResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthUserChangePasswordRequest`
    fn handle_user_change_password_request(
        &self,
        req: &AuthUserChangePasswordRequest,
    ) -> Result<AuthUserChangePasswordResponse, ExecuteError> {
        debug!("handle_user_change_password_request");
        let user = self.backend.get_user(&req.name)?;
        let need_password = user.options.as_ref().map_or(true, |o| !o.no_password);
        if need_password && req.hashed_password.is_empty() {
            return Err(ExecuteError::no_password_user());
        }
        Ok(AuthUserChangePasswordResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthUserGrantRoleRequest`
    fn handle_user_grant_role_request(
        &self,
        req: &AuthUserGrantRoleRequest,
    ) -> Result<AuthUserGrantRoleResponse, ExecuteError> {
        debug!("handle_user_grant_role_request");
        let _user = self.backend.get_user(&req.user)?;
        if req.role != ROOT_ROLE {
            let _role = self.backend.get_role(&req.role)?;
        }
        Ok(AuthUserGrantRoleResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthUserRevokeRoleRequest`
    fn handle_user_revoke_role_request(
        &self,
        req: &AuthUserRevokeRoleRequest,
    ) -> Result<AuthUserRevokeRoleResponse, ExecuteError> {
        debug!("handle_user_revoke_role_request");
        if self.is_enabled() && (req.name == ROOT_USER) && (req.role == ROOT_ROLE) {
            return Err(ExecuteError::invalid_auth_management());
        }
        let user = self.backend.get_user(&req.name)?;
        if user.roles.binary_search(&req.role).is_err() {
            return Err(ExecuteError::role_not_granted(&req.role));
        }
        Ok(AuthUserRevokeRoleResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthRoleAddRequest`
    fn handle_role_add_request(
        &self,
        req: &AuthRoleAddRequest,
    ) -> Result<AuthRoleAddResponse, ExecuteError> {
        debug!("handle_role_add_request");
        if self.backend.get_role(&req.name).is_ok() {
            return Err(ExecuteError::role_already_exists(&req.name));
        }
        Ok(AuthRoleAddResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthRoleGetRequest`
    fn handle_role_get_request(
        &self,
        req: &AuthRoleGetRequest,
    ) -> Result<AuthRoleGetResponse, ExecuteError> {
        debug!("handle_role_get_request");
        let role = self.backend.get_role(&req.role)?;
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
            header: Some(self.header_gen.gen_header_without_revision()),
            perm,
        })
    }

    /// Handle `AuthRoleListRequest`
    fn handle_role_list_request(
        &self,
        _req: &AuthRoleListRequest,
    ) -> Result<AuthRoleListResponse, ExecuteError> {
        debug!("handle_role_list_request");
        let roles = self
            .backend
            .get_all_roles()?
            .into_iter()
            .map(|r| String::from_utf8_lossy(&r.name).to_string())
            .collect();
        Ok(AuthRoleListResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
            roles,
        })
    }

    /// Handle `UserRoleDeleteRequest`
    fn handle_role_delete_request(
        &self,
        req: &AuthRoleDeleteRequest,
    ) -> Result<AuthRoleDeleteResponse, ExecuteError> {
        debug!("handle_role_delete_request");
        if self.is_enabled() && req.role == ROOT_ROLE {
            return Err(ExecuteError::invalid_auth_management());
        }
        let _role = self.backend.get_role(&req.role)?;
        Ok(AuthRoleDeleteResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthRoleGrantPermissionRequest`
    fn handle_role_grant_permission_request(
        &self,
        req: &AuthRoleGrantPermissionRequest,
    ) -> Result<AuthRoleGrantPermissionResponse, ExecuteError> {
        debug!("handle_role_grant_permission_request");
        let _role = self.backend.get_role(&req.name)?;
        Ok(AuthRoleGrantPermissionResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Handle `AuthRoleRevokePermissionRequest`
    fn handle_role_revoke_permission_request(
        &self,
        req: &AuthRoleRevokePermissionRequest,
    ) -> Result<AuthRoleRevokePermissionResponse, ExecuteError> {
        debug!("handle_role_revoke_permission_request");
        let role = self.backend.get_role(&req.role)?;
        if role
            .key_permission
            .binary_search_by(|p| match p.key.cmp(&req.key) {
                Ordering::Equal => p.range_end.cmp(&req.range_end),
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            })
            .is_err()
        {
            return Err(ExecuteError::permission_not_granted());
        }
        Ok(AuthRoleRevokePermissionResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// sync a auth request
    pub(crate) fn after_sync(
        &self,
        request: &RequestWithToken,
    ) -> Result<(SyncResponse, Vec<WriteOperation>), ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        let ops = match request.request {
            RequestWrapper::AuthEnableRequest(ref req) => {
                debug!("Sync AuthEnableRequest {:?}", req);
                self.sync_auth_enable_request(req)?
            }
            RequestWrapper::AuthDisableRequest(ref req) => {
                debug!("Sync AuthDisableRequest {:?}", req);
                self.sync_auth_disable_request(req)
            }
            RequestWrapper::AuthStatusRequest(ref req) => {
                debug!("Sync AuthStatusRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthUserAddRequest(ref req) => {
                debug!("Sync AuthUserAddRequest {:?}", req);
                self.sync_user_add_request(req)
            }
            RequestWrapper::AuthUserGetRequest(ref req) => {
                debug!("Sync AuthUserGetRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthUserListRequest(ref req) => {
                debug!("Sync AuthUserListRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthUserGrantRoleRequest(ref req) => {
                debug!("Sync AuthUserGrantRoleRequest {:?}", req);
                self.sync_user_grant_role_request(req)?
            }
            RequestWrapper::AuthUserRevokeRoleRequest(ref req) => {
                debug!("Sync AuthUserRevokeRoleRequest {:?}", req);
                self.sync_user_revoke_role_request(req)?
            }
            RequestWrapper::AuthUserChangePasswordRequest(ref req) => {
                debug!("Sync AuthUserChangePasswordRequest {:?}", req);
                self.sync_user_change_password_request(req)?
            }
            RequestWrapper::AuthUserDeleteRequest(ref req) => {
                debug!("Sync AuthUserDeleteRequest {:?}", req);
                self.sync_user_delete_request(req)
            }
            RequestWrapper::AuthRoleAddRequest(ref req) => {
                debug!("Sync AuthRoleAddRequest {:?}", req);
                self.sync_role_add_request(req)
            }
            RequestWrapper::AuthRoleGetRequest(ref req) => {
                debug!("Sync AuthRoleGetRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthRoleGrantPermissionRequest(ref req) => {
                debug!("Sync AuthRoleGrantPermissionRequest {:?}", req);
                self.sync_role_grant_permission_request(req)?
            }
            RequestWrapper::AuthRoleRevokePermissionRequest(ref req) => {
                debug!("Sync AuthRoleRevokePermissionRequest {:?}", req);
                self.sync_role_revoke_permission_request(req)?
            }
            RequestWrapper::AuthRoleListRequest(ref req) => {
                debug!("Sync AuthRoleListRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthRoleDeleteRequest(ref req) => {
                debug!("Sync AuthRoleDeleteRequest {:?}", req);
                self.sync_role_delete_request(req)?
            }
            RequestWrapper::AuthenticateRequest(ref req) => {
                debug!("Sync AuthenticateRequest {:?}", req);
                Vec::new()
            }
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        };
        Ok((SyncResponse::new(self.header_gen.revision()), ops))
    }

    /// Sync `AuthEnableRequest` and return whether authstore is changed.
    fn sync_auth_enable_request(
        &self,
        _req: &AuthEnableRequest,
    ) -> Result<Vec<WriteOperation>, ExecuteError> {
        if self.is_enabled() {
            return Ok(Vec::new());
        }
        let ops = vec![Self::op_put_auth_enable(true)];
        self.enabled.store(true, AtomicOrdering::Relaxed);
        self.create_permission_cache()?;
        let rev = self.backend.get_revision()?;
        self.revision.set(rev);
        Ok(ops)
    }

    /// Sync `AuthDisableRequest` and return whether authstore is changed.
    fn sync_auth_disable_request(&self, _req: &AuthDisableRequest) -> Vec<WriteOperation> {
        if !self.is_enabled() {
            return Vec::new();
        }
        let mut ops = Vec::new();
        self.enabled.store(false, AtomicOrdering::Relaxed);
        ops.push(Self::op_put_auth_enable(false));
        ops.push(self.op_inc_and_put_revision());
        ops
    }

    /// Sync `AuthUserAddRequest` and return whether authstore is changed.
    fn sync_user_add_request(&self, req: &AuthUserAddRequest) -> Vec<WriteOperation> {
        let mut ops = Vec::new();
        let user = User {
            name: req.name.as_str().into(),
            password: req.hashed_password.as_str().into(),
            options: req.options.clone(),
            roles: Vec::new(),
        };
        ops.push(Self::op_put_user(&user));
        ops.push(self.op_inc_and_put_revision());
        ops
    }

    /// Sync `AuthUserDeleteRequest` and return whether authstore is changed.
    fn sync_user_delete_request(&self, req: &AuthUserDeleteRequest) -> Vec<WriteOperation> {
        let mut ops = Vec::new();
        self.permission_cache.map_write(|mut cache| {
            let _ignore = cache.user_permissions.remove(&req.name);
            cache.role_to_users_map.iter_mut().for_each(|(_, users)| {
                if let Some((idx, _)) = users.iter().find_position(|uname| uname == &&req.name) {
                    let _old = users.swap_remove(idx);
                };
            });
        });
        ops.push(Self::op_delete_user(&req.name));
        ops.push(self.op_inc_and_put_revision());
        ops
    }

    /// Sync `AuthUserChangePasswordRequest` and return whether authstore is changed.
    fn sync_user_change_password_request(
        &self,
        req: &AuthUserChangePasswordRequest,
    ) -> Result<Vec<WriteOperation>, ExecuteError> {
        let mut ops = Vec::new();
        let mut user = self.backend.get_user(&req.name)?;
        user.password = req.hashed_password.as_str().into();
        ops.push(Self::op_put_user(&user));
        ops.push(self.op_inc_and_put_revision());
        Ok(ops)
    }

    /// Sync `AuthUserGrantRoleRequest` and return whether authstore is changed.
    fn sync_user_grant_role_request(
        &self,
        req: &AuthUserGrantRoleRequest,
    ) -> Result<Vec<WriteOperation>, ExecuteError> {
        let mut ops = Vec::new();
        let mut user = self.backend.get_user(&req.user)?;
        let role = self.backend.get_role(&req.role);
        if (req.role != ROOT_ROLE) && role.is_err() {
            return Err(ExecuteError::role_not_found(&req.role));
        }
        let Err(idx) =  user.roles.binary_search(&req.role) else {
            return Err(ExecuteError::user_already_has_role(&req.user, &req.role));
        };
        user.roles.insert(idx, req.role.clone());
        ops.push(Self::op_put_user(&user));
        ops.push(self.op_inc_and_put_revision());
        if let Ok(role) = role {
            let perms = role.key_permission;
            self.permission_cache.map_write(|mut cache| {
                let entry = cache
                    .user_permissions
                    .entry(req.user.clone())
                    .or_insert_with(UserPermissions::new);
                for perm in perms {
                    let key_range = KeyRange::new(perm.key, perm.range_end);
                    #[allow(clippy::unwrap_used)] // safe unwrap
                    match Type::from_i32(perm.perm_type).unwrap() {
                        Type::Readwrite => {
                            entry.read.push(key_range.clone());
                            entry.write.push(key_range);
                        }
                        Type::Write => {
                            entry.write.push(key_range);
                        }
                        Type::Read => {
                            entry.read.push(key_range);
                        }
                    }
                }
                cache
                    .role_to_users_map
                    .entry(req.role.clone())
                    .or_insert_with(Vec::new)
                    .push(req.user.clone());
            });
        }
        Ok(ops)
    }

    /// Sync `AuthUserRevokeRoleRequest` and return whether authstore is changed.
    fn sync_user_revoke_role_request(
        &self,
        req: &AuthUserRevokeRoleRequest,
    ) -> Result<Vec<WriteOperation>, ExecuteError> {
        let mut ops = Vec::new();
        let mut user = self.backend.get_user(&req.name)?;
        let idx = user
            .roles
            .binary_search(&req.role)
            .map_err(|_ignore| ExecuteError::role_not_granted(&req.role))?;
        let _ignore = user.roles.remove(idx);
        ops.push(Self::op_put_user(&user));
        ops.push(self.op_inc_and_put_revision());
        self.permission_cache.map_write(|mut cache| {
            let user_permissions = self.get_user_permissions(&user, None);
            let _entry = cache
                .role_to_users_map
                .entry(req.role.clone())
                .and_modify(|users| {
                    if let Some((i, _)) = users.iter().find_position(|uname| uname == &&req.name) {
                        let _old = users.swap_remove(i);
                    };
                });
            let _old = cache
                .user_permissions
                .insert(req.name.clone(), user_permissions);
        });
        Ok(ops)
    }

    /// Sync `AuthRoleAddRequest` and return whether authstore is changed.
    fn sync_role_add_request(&self, req: &AuthRoleAddRequest) -> Vec<WriteOperation> {
        let mut ops = Vec::new();
        let role = Role {
            name: req.name.as_str().into(),
            key_permission: Vec::new(),
        };
        ops.push(Self::op_put_role(&role));
        ops.push(self.op_inc_and_put_revision());
        ops
    }

    /// Sync `AuthRoleDeleteRequest` and return whether authstore is changed.
    fn sync_role_delete_request(
        &self,
        req: &AuthRoleDeleteRequest,
    ) -> Result<Vec<WriteOperation>, ExecuteError> {
        let mut ops = vec![Self::op_delete_role(&req.role)];
        let users = self.backend.get_all_users()?;
        let mut new_perms = HashMap::new();
        for mut user in users {
            if let Ok(idx) = user.roles.binary_search(&req.role) {
                let _ignore = user.roles.remove(idx);
                let perms = self.get_user_permissions(&user, None);
                let _old = new_perms.insert(String::from_utf8_lossy(&user.name).to_string(), perms);
                ops.push(Self::op_put_user(&user));
            }
        }
        ops.push(self.op_inc_and_put_revision());
        self.permission_cache.map_write(|mut cache| {
            cache.user_permissions.extend(new_perms.into_iter());
            let _ignore = cache.role_to_users_map.remove(&req.role);
        });
        Ok(ops)
    }

    /// Sync `AuthRoleGrantPermissionRequest` and return whether authstore is changed.
    fn sync_role_grant_permission_request(
        &self,
        req: &AuthRoleGrantPermissionRequest,
    ) -> Result<Vec<WriteOperation>, ExecuteError> {
        let mut ops = Vec::new();
        let mut role = self.backend.get_role(&req.name)?;
        let permission = req
            .perm
            .clone()
            .ok_or_else(ExecuteError::permission_not_given)?;

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
                role.key_permission.insert(idx, permission.clone());
            }
        };
        ops.push(Self::op_put_role(&role));
        ops.push(self.op_inc_and_put_revision());
        self.permission_cache.map_write(move |mut cache| {
            let users = cache
                .role_to_users_map
                .get(&req.name)
                .cloned()
                .unwrap_or_default();
            let key_range = KeyRange::new(permission.key, permission.range_end);
            for user in users {
                let entry = cache
                    .user_permissions
                    .entry(user)
                    .or_insert_with(UserPermissions::new);
                #[allow(clippy::unwrap_used)] // safe unwrap
                match Type::from_i32(permission.perm_type).unwrap() {
                    Type::Readwrite => {
                        entry.read.push(key_range.clone());
                        entry.write.push(key_range.clone());
                    }
                    Type::Write => {
                        entry.write.push(key_range.clone());
                    }
                    Type::Read => {
                        entry.read.push(key_range.clone());
                    }
                }
            }
        });
        Ok(ops)
    }

    /// Sync `AuthRoleRevokePermissionRequest` and return whether authstore is changed.
    fn sync_role_revoke_permission_request(
        &self,
        req: &AuthRoleRevokePermissionRequest,
    ) -> Result<Vec<WriteOperation>, ExecuteError> {
        let mut ops = Vec::new();
        let mut role = self.backend.get_role(&req.role)?;
        let idx = role
            .key_permission
            .binary_search_by(|p| match p.key.cmp(&req.key) {
                Ordering::Equal => p.range_end.cmp(&req.range_end),
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            })
            .map_err(|_ignore| ExecuteError::permission_not_granted())?;
        let _ignore = role.key_permission.remove(idx);
        ops.push(Self::op_put_role(&role));
        ops.push(self.op_inc_and_put_revision());
        self.permission_cache.map_write(|mut cache| {
            let users = cache
                .role_to_users_map
                .get(&req.role)
                .map_or_else(Vec::new, |users| {
                    users
                        .iter()
                        .filter_map(|user| self.backend.get_user(user).ok())
                        .collect::<Vec<_>>()
                });
            for user in users {
                let perms = self.get_user_permissions(&user, Some(&req.role));
                let _old = cache
                    .user_permissions
                    .insert(String::from_utf8_lossy(&user.name).to_string(), perms);
            }
        });
        Ok(ops)
    }

    /// Auth revision
    pub(crate) fn revision(&self) -> i64 {
        self.revision.get()
    }

    /// Check password
    pub(crate) fn check_password(
        &self,
        username: &str,
        password: &str,
    ) -> Result<i64, ExecuteError> {
        if !self.is_enabled() {
            return Err(ExecuteError::auth_not_enabled());
        }
        let user = self.backend.get_user(username)?;
        let need_password = user.options.as_ref().map_or(true, |o| !o.no_password);
        if !need_password {
            return Err(ExecuteError::no_password_user());
        }

        let hash = String::from_utf8_lossy(&user.password);
        let hash = PasswordHash::new(&hash)
            .unwrap_or_else(|e| panic!("Failed to parse password hash, error: {e}"));
        Pbkdf2
            .verify_password(password.as_bytes(), &hash)
            .map_err(|_ignore| ExecuteError::auth_failed())?;

        Ok(self.revision())
    }

    /// Check if the request need admin permission
    fn need_admin_permission(wrapper: &RequestWithToken) -> bool {
        matches!(
            wrapper.request,
            RequestWrapper::AuthEnableRequest(_)
                | RequestWrapper::AuthDisableRequest(_)
                | RequestWrapper::AuthStatusRequest(_)
                | RequestWrapper::AuthUserAddRequest(_)
                | RequestWrapper::AuthUserDeleteRequest(_)
                | RequestWrapper::AuthUserChangePasswordRequest(_)
                | RequestWrapper::AuthUserGrantRoleRequest(_)
                | RequestWrapper::AuthUserRevokeRoleRequest(_)
                | RequestWrapper::AuthRoleAddRequest(_)
                | RequestWrapper::AuthRoleGrantPermissionRequest(_)
                | RequestWrapper::AuthRoleRevokePermissionRequest(_)
                | RequestWrapper::AuthRoleDeleteRequest(_)
                | RequestWrapper::AuthUserListRequest(_)
                | RequestWrapper::AuthRoleListRequest(_)
        )
    }

    #[cfg(test)]
    pub(super) fn permission_cache(&self) -> PermissionCache {
        self.permission_cache.map_read(|cache| cache.clone())
    }

    /// check if the request is permitted
    pub(crate) async fn check_permission(
        &self,
        wrapper: &RequestWithToken,
    ) -> Result<(), ExecuteError> {
        if !self.is_enabled() {
            return Ok(());
        }
        if let RequestWrapper::AuthenticateRequest(_) = wrapper.request {
            return Ok(());
        }
        let claims = match wrapper.token {
            Some(ref token) => self.verify_token(token)?,
            None => {
                // TODO: some requests are allowed without token when auth is enabled
                return Err(ExecuteError::token_not_provided());
            }
        };
        if claims.revision < self.revision() {
            return Err(ExecuteError::token_old_revision());
        }
        let username = claims.username;
        if Self::need_admin_permission(wrapper) {
            self.check_admin_permission(&username)?;
        } else {
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
                            let user = self.backend.get_user(&username)?;
                            if user.has_role(&role_get_req.role) {
                                Ok(())
                            } else {
                                Err(e)
                            }
                        },
                        |_| Ok(()),
                    )?;
                }
                _ => {}
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
        let mut check_queue = VecDeque::new();
        let req = RequestOp {
            request: Some(Request::RequestTxn(req.clone())),
        };
        check_queue.push_back(&req);
        while let Some(req_op) = check_queue.pop_front() {
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
                        check_queue.push_back(op);
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
        let lease = self.get_lease(lease_id).await;
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
        if !self.is_enabled() {
            return Ok(());
        }
        let user = self.backend.get_user(username)?;
        if user.has_role(ROOT_ROLE) {
            return Ok(());
        }
        Err(ExecuteError::PermissionDenied)
    }

    /// check permission for a kv operation
    fn check_op_permission(
        &self,
        username: &str,
        key: &[u8],
        range_end: &[u8],
        perm_type: Type,
    ) -> Result<(), ExecuteError> {
        let user = self.backend.get_user(username)?;
        if user.has_role(ROOT_ROLE) {
            return Ok(());
        }
        let key_range = KeyRange::new(key, range_end);
        if let Some(permissions) = self.permission_cache.read().user_permissions.get(username) {
            match perm_type {
                Type::Read => {
                    if permissions
                        .read
                        .iter()
                        .any(|kr| kr.contains_range(&key_range))
                    {
                        return Ok(());
                    }
                }
                Type::Write => {
                    if permissions
                        .write
                        .iter()
                        .any(|kr| kr.contains_range(&key_range))
                    {
                        return Ok(());
                    }
                }
                Type::Readwrite => {
                    unreachable!("Readwrite is unreachable");
                }
            }
        }
        Err(ExecuteError::PermissionDenied)
    }

    /// Assign root token
    pub(crate) fn root_token(&self) -> Result<String, ExecuteError> {
        self.assign(ROOT_USER)
    }

    /// Recover data from persistent storage
    pub(crate) fn recover(&self) -> Result<(), ExecuteError> {
        let enabled = self.backend.get_enable()?;
        if enabled {
            self.enabled.store(true, AtomicOrdering::Relaxed);
        }
        let revision = self.backend.get_revision()?;
        self.revision.set(revision);
        self.create_permission_cache()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use utils::config::StorageConfig;

    use super::*;
    use crate::{
        rpc::{
            AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGrantPermissionRequest,
            AuthRoleRevokePermissionRequest, AuthUserAddRequest, AuthUserDeleteRequest,
            AuthUserGrantRoleRequest, Permission,
        },
        storage::{
            auth_store::perms::{PermissionCache, UserPermissions},
            db::DBProxy,
        },
    };

    #[test]
    fn test_role_grant_permission() -> Result<(), ExecuteError> {
        let db = DBProxy::open(&StorageConfig::Memory)?;
        let store = init_auth_store(db);
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
        assert!(exe_and_sync(&store, &req).is_ok());
        assert_eq!(
            store.permission_cache(),
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
        Ok(())
    }

    #[test]
    fn test_role_revoke_permission() -> Result<(), ExecuteError> {
        let db = DBProxy::open(&StorageConfig::Memory)?;
        let store = init_auth_store(db);
        let req = RequestWithToken::new(
            AuthRoleRevokePermissionRequest {
                role: "r".to_owned(),
                key: "foo".into(),
                range_end: "".into(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req).is_ok());
        assert_eq!(
            store.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([("u".to_owned(), UserPermissions::new())]),
                role_to_users_map: HashMap::from([("r".to_owned(), vec!["u".to_owned()])]),
            },
        );
        Ok(())
    }

    #[test]
    fn test_role_delete() -> Result<(), ExecuteError> {
        let db = DBProxy::open(&StorageConfig::Memory)?;
        let store = init_auth_store(db);
        let req = RequestWithToken::new(
            AuthRoleDeleteRequest {
                role: "r".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req).is_ok());
        assert_eq!(
            store.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([("u".to_owned(), UserPermissions::new(),)]),
                role_to_users_map: HashMap::new(),
            },
        );
        Ok(())
    }

    #[test]
    fn test_user_delete() -> Result<(), ExecuteError> {
        let db = DBProxy::open(&StorageConfig::Memory)?;
        let store = init_auth_store(db);
        let req = RequestWithToken::new(
            AuthUserDeleteRequest {
                name: "u".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req).is_ok());
        assert_eq!(
            store.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::new(),
                role_to_users_map: HashMap::from([("r".to_owned(), vec![])]),
            },
        );
        Ok(())
    }

    #[test]
    fn test_auth_enable_and_disable() {
        let db = DBProxy::open(&StorageConfig::Memory).unwrap();
        let store = init_auth_store(db);
        let revision = store.revision();
        assert!(!store.is_enabled());

        let enable_req = RequestWithToken::new(AuthEnableRequest {}.into());
        assert!(exe_and_sync(&store, &enable_req).is_err());
        let req_1 = RequestWithToken::new(
            AuthUserAddRequest {
                name: "root".to_owned(),
                password: String::new(),
                hashed_password: "123".to_owned(),
                options: None,
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req_1).is_ok());

        let req_2 = RequestWithToken::new(
            AuthRoleAddRequest {
                name: "root".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req_2).is_ok());

        let req_3 = RequestWithToken::new(
            AuthUserGrantRoleRequest {
                user: "root".to_owned(),
                role: "root".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req_3).is_ok());
        assert_eq!(store.revision(), revision + 3);

        // AuthEnableRequest won't increase the auth revision, but AuthDisableRequest will
        assert!(exe_and_sync(&store, &enable_req).is_ok());
        assert_eq!(store.revision(), 8);
        assert!(store.is_enabled());

        let disable_req = RequestWithToken::new(AuthDisableRequest {}.into());

        assert!(exe_and_sync(&store, &disable_req).is_ok());
        assert_eq!(store.revision(), revision + 4);
        assert!(!store.is_enabled());
    }

    #[test]
    fn test_recover() -> Result<(), ExecuteError> {
        let db = DBProxy::open(&StorageConfig::Memory).unwrap();
        let store = init_auth_store(Arc::clone(&db));

        let new_store = init_empty_store(db);
        assert_eq!(new_store.permission_cache(), PermissionCache::new());
        new_store.recover()?;
        assert_eq!(store.permission_cache(), new_store.permission_cache());
        assert_eq!(store.revision(), new_store.revision());

        Ok(())
    }

    fn init_auth_store(db: Arc<DBProxy>) -> AuthStore<DBProxy> {
        let store = init_empty_store(db);
        let req1 = RequestWithToken::new(
            AuthRoleAddRequest {
                name: "r".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req1).is_ok());
        let req2 = RequestWithToken::new(
            AuthUserAddRequest {
                name: "u".to_owned(),
                password: String::new(),
                hashed_password: "123".to_owned(),
                options: None,
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req2).is_ok());
        let req3 = RequestWithToken::new(
            AuthUserGrantRoleRequest {
                user: "u".to_owned(),
                role: "r".to_owned(),
            }
            .into(),
        );
        assert!(exe_and_sync(&store, &req3).is_ok());
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
        assert!(exe_and_sync(&store, &req4).is_ok());
        assert_eq!(
            store.permission_cache(),
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

    fn init_empty_store(db: Arc<DBProxy>) -> AuthStore<DBProxy> {
        let key_pair = test_key_pair();
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let (lease_cmd_tx, _) = mpsc::channel(1);
        AuthStore::new(lease_cmd_tx, key_pair, header_gen, db)
    }

    fn exe_and_sync(
        store: &AuthStore<DBProxy>,
        req: &RequestWithToken,
    ) -> Result<(CommandResponse, SyncResponse), ExecuteError> {
        let cmd_res = store.execute(req)?;
        let (sync_res, ops) = store.after_sync(req)?;
        store.backend.write_batch(ops, false)?;
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
