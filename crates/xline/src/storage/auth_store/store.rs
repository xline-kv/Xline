use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc,
    },
};

use clippy_utilities::NumericCast;
use itertools::Itertools;
use jsonwebtoken::{DecodingKey, EncodingKey};
use log::debug;
use parking_lot::RwLock;
use pbkdf2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Pbkdf2,
};
use utils::parking_lot_lock::RwLockMap;
use xlineapi::{
    command::{CommandResponse, KeyRange, SyncResponse},
    execute_error::ExecuteError,
    AuthInfo,
};

use super::{
    backend::{ROOT_ROLE, ROOT_USER},
    perms::{JwtTokenManager, PermissionCache, TokenOperate, UserPermissions},
};
use crate::{
    header_gen::HeaderGenerator,
    revision_number::RevisionNumberGenerator,
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
        RangeRequest, Request, RequestOp, RequestWrapper, Role, TxnRequest, Type, User,
    },
    server::get_token,
    storage::{
        auth_store::backend::AuthStoreBackend,
        db::WriteOp,
        lease_store::{Lease, LeaseCollection},
        storage_api::StorageApi,
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
    revision: Arc<RevisionNumberGenerator>,
    /// Lease collection
    lease_collection: Arc<LeaseCollection>,
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
    #[allow(clippy::arithmetic_side_effects)] // Introduced by tokio::select!
    pub(crate) fn new(
        lease_collection: Arc<LeaseCollection>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
        header_gen: Arc<HeaderGenerator>,
        storage: Arc<S>,
    ) -> Self {
        let backend = Arc::new(AuthStoreBackend::new(storage));
        Self {
            backend,
            enabled: AtomicBool::new(false),
            revision: header_gen.auth_revision_arc(),
            lease_collection,
            header_gen,
            permission_cache: RwLock::new(PermissionCache::new()),
            token_manager: key_pair.map(|(encoding_key, decoding_key)| {
                JwtTokenManager::new(encoding_key, decoding_key)
            }),
        }
    }

    /// Get Lease by lease id
    fn look_up(&self, lease_id: i64) -> Option<Lease> {
        self.lease_collection.look_up(lease_id)
    }

    /// Get enabled of Auth store
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(AtomicOrdering::Relaxed)
    }

    /// Assign token
    pub(crate) fn assign(&self, username: &str) -> Result<String, ExecuteError> {
        match self.token_manager {
            Some(ref token_manager) => token_manager
                .assign(username, self.revision())
                .map_err(|_ignore| ExecuteError::InvalidAuthToken),
            None => Err(ExecuteError::TokenManagerNotInit),
        }
    }

    /// verify token
    pub(crate) fn verify(&self, token: &str) -> Result<AuthInfo, ExecuteError> {
        match self.token_manager {
            Some(ref token_manager) => token_manager
                .verify(token)
                .map(Into::into)
                .map_err(|_ignore| ExecuteError::InvalidAuthToken),
            None => Err(ExecuteError::TokenManagerNotInit),
        }
    }

    /// Try get auth info from tonic request
    pub(crate) fn try_get_auth_info_from_request<T>(
        &self,
        request: &tonic::Request<T>,
    ) -> Result<Option<AuthInfo>, tonic::Status> {
        if !self.is_enabled() {
            return Ok(None);
        }
        if let Some(token) = get_token(request.metadata()) {
            let auth_info = self.verify(&token)?;
            return Ok(Some(auth_info));
        }
        if let Some(cn) = get_cn(request) {
            let auth_info = AuthInfo {
                username: cn,
                auth_revision: self.revision(),
            };
            return Ok(Some(auth_info));
        }
        Ok(None)
    }

    /// create permission cache
    fn create_permission_cache(&self) -> Result<(), ExecuteError> {
        let mut permission_cache = PermissionCache::new();
        for user in self.backend.get_all_users()? {
            let user_permission = self.get_user_permissions(&user, None);
            let username = String::from_utf8_lossy(&user.name).to_string();
            for role in user.roles {
                permission_cache
                    .role_to_users_map
                    .entry(role)
                    .or_insert_with(Vec::new)
                    .push(username.clone());
            }
            let _ignore = permission_cache
                .user_permissions
                .insert(username, user_permission);
        }
        self.permission_cache
            .map_write(|mut cache| *cache = permission_cache);
        Ok(())
    }

    /// get user permissions
    fn get_user_permissions(&self, user: &User, skip_role: Option<&str>) -> UserPermissions {
        let mut user_permission = UserPermissions::new();
        for role_name in &user.roles {
            if skip_role.map_or(false, |r| r == role_name) {
                continue;
            }
            let Ok(role) = self.backend.get_role(role_name) else {
                continue;
            };
            for permission in role.key_permission {
                user_permission.insert(permission);
            }
        }
        user_permission
    }

    /// execute a auth request
    pub(crate) fn execute(
        &self,
        request: &RequestWrapper,
    ) -> Result<CommandResponse, ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        let res = match *request {
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
            header: Some(self.header_gen.gen_auth_header()),
        });
        if self.is_enabled() {
            debug!("auth is already enabled");
            return res;
        }
        let user = self.backend.get_user(ROOT_USER)?;
        if user.roles.binary_search(&ROOT_ROLE.to_owned()).is_err() {
            return Err(ExecuteError::RootRoleNotExist);
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
            header: Some(self.header_gen.gen_auth_header()),
        }
    }

    /// Handle `AuthStatusRequest`
    fn handle_auth_status_request(&self, _req: &AuthStatusRequest) -> AuthStatusResponse {
        debug!("handle_auth_status");
        AuthStatusResponse {
            header: Some(self.header_gen.gen_auth_header()),
            auth_revision: self.revision().numeric_cast(),
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
            return Err(ExecuteError::AuthNotEnabled);
        }
        self.check_password(&req.name, &req.password)?;
        let token = self.assign(&req.name)?;
        Ok(AuthenticateResponse {
            header: Some(self.header_gen.gen_auth_header()),
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
            return Err(ExecuteError::UserAlreadyExists(req.name.clone()));
        }
        Ok(AuthUserAddResponse {
            header: Some(self.header_gen.gen_auth_header()),
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
            header: Some(self.header_gen.gen_auth_header()),
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
            header: Some(self.header_gen.gen_auth_header()),
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
            return Err(ExecuteError::InvalidAuthManagement);
        }
        let _user = self.backend.get_user(&req.name)?;
        Ok(AuthUserDeleteResponse {
            header: Some(self.header_gen.gen_auth_header()),
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
            return Err(ExecuteError::NoPasswordUser);
        }
        Ok(AuthUserChangePasswordResponse {
            header: Some(self.header_gen.gen_auth_header()),
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
            header: Some(self.header_gen.gen_auth_header()),
        })
    }

    /// Handle `AuthUserRevokeRoleRequest`
    fn handle_user_revoke_role_request(
        &self,
        req: &AuthUserRevokeRoleRequest,
    ) -> Result<AuthUserRevokeRoleResponse, ExecuteError> {
        debug!("handle_user_revoke_role_request");
        if self.is_enabled() && (req.name == ROOT_USER) && (req.role == ROOT_ROLE) {
            return Err(ExecuteError::InvalidAuthManagement);
        }
        let user = self.backend.get_user(&req.name)?;
        if user.roles.binary_search(&req.role).is_err() {
            return Err(ExecuteError::RoleNotGranted(req.role.clone()));
        }
        Ok(AuthUserRevokeRoleResponse {
            header: Some(self.header_gen.gen_auth_header()),
        })
    }

    /// Handle `AuthRoleAddRequest`
    fn handle_role_add_request(
        &self,
        req: &AuthRoleAddRequest,
    ) -> Result<AuthRoleAddResponse, ExecuteError> {
        debug!("handle_role_add_request");
        if self.backend.get_role(&req.name).is_ok() {
            return Err(ExecuteError::RoleAlreadyExists(req.name.clone()));
        }
        Ok(AuthRoleAddResponse {
            header: Some(self.header_gen.gen_auth_header()),
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
            header: Some(self.header_gen.gen_auth_header()),
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
            header: Some(self.header_gen.gen_auth_header()),
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
            return Err(ExecuteError::InvalidAuthManagement);
        }
        let _role = self.backend.get_role(&req.role)?;
        Ok(AuthRoleDeleteResponse {
            header: Some(self.header_gen.gen_auth_header()),
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
            header: Some(self.header_gen.gen_auth_header()),
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
            return Err(ExecuteError::PermissionNotGranted);
        }
        Ok(AuthRoleRevokePermissionResponse {
            header: Some(self.header_gen.gen_auth_header()),
        })
    }

    /// sync a auth request
    pub(crate) fn after_sync<'a>(
        &self,
        request: &'a RequestWrapper,
        revision: i64,
    ) -> Result<(SyncResponse, Vec<WriteOp<'a>>), ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        let ops = match *request {
            RequestWrapper::AuthEnableRequest(ref req) => {
                debug!("Sync AuthEnableRequest {:?}", req);
                self.sync_auth_enable_request(req)?
            }
            RequestWrapper::AuthDisableRequest(ref req) => {
                debug!("Sync AuthDisableRequest {:?}", req);
                self.sync_auth_disable_request(req, revision)
            }
            RequestWrapper::AuthStatusRequest(ref req) => {
                debug!("Sync AuthStatusRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthUserAddRequest(ref req) => {
                debug!("Sync AuthUserAddRequest {:?}", req);
                Self::sync_user_add_request(req, revision)
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
                self.sync_user_grant_role_request(req, revision)?
            }
            RequestWrapper::AuthUserRevokeRoleRequest(ref req) => {
                debug!("Sync AuthUserRevokeRoleRequest {:?}", req);
                self.sync_user_revoke_role_request(req, revision)?
            }
            RequestWrapper::AuthUserChangePasswordRequest(ref req) => {
                debug!("Sync AuthUserChangePasswordRequest {:?}", req);
                self.sync_user_change_password_request(req, revision)?
            }
            RequestWrapper::AuthUserDeleteRequest(ref req) => {
                debug!("Sync AuthUserDeleteRequest {:?}", req);
                self.sync_user_delete_request(req, revision)
            }
            RequestWrapper::AuthRoleAddRequest(ref req) => {
                debug!("Sync AuthRoleAddRequest {:?}", req);
                Self::sync_role_add_request(req, revision)
            }
            RequestWrapper::AuthRoleGetRequest(ref req) => {
                debug!("Sync AuthRoleGetRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthRoleGrantPermissionRequest(ref req) => {
                debug!("Sync AuthRoleGrantPermissionRequest {:?}", req);
                self.sync_role_grant_permission_request(req, revision)?
            }
            RequestWrapper::AuthRoleRevokePermissionRequest(ref req) => {
                debug!("Sync AuthRoleRevokePermissionRequest {:?}", req);
                self.sync_role_revoke_permission_request(req, revision)?
            }
            RequestWrapper::AuthRoleListRequest(ref req) => {
                debug!("Sync AuthRoleListRequest {:?}", req);
                Vec::new()
            }
            RequestWrapper::AuthRoleDeleteRequest(ref req) => {
                debug!("Sync AuthRoleDeleteRequest {:?}", req);
                self.sync_role_delete_request(req, revision)?
            }
            RequestWrapper::AuthenticateRequest(ref req) => {
                debug!("Sync AuthenticateRequest {:?}", req);
                Vec::new()
            }
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        };
        Ok((SyncResponse::new(revision), ops))
    }

    /// Sync `AuthEnableRequest` and return whether authstore is changed.
    fn sync_auth_enable_request<'a>(
        &self,
        _req: &'a AuthEnableRequest,
    ) -> Result<Vec<WriteOp<'a>>, ExecuteError> {
        if self.is_enabled() {
            return Ok(Vec::new());
        }
        self.enabled.store(true, AtomicOrdering::Relaxed);
        self.create_permission_cache()?;
        Ok(vec![WriteOp::PutAuthEnable(true)])
    }

    /// Sync `AuthDisableRequest` and return whether authstore is changed.
    fn sync_auth_disable_request<'a>(
        &self,
        _req: &'a AuthDisableRequest,
        revision: i64,
    ) -> Vec<WriteOp<'a>> {
        let mut ops = Vec::new();
        if !self.is_enabled() {
            return Vec::new();
        }
        self.enabled.store(false, AtomicOrdering::Relaxed);
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutAuthEnable(false));
        ops
    }

    /// Sync `AuthUserAddRequest` and return whether authstore is changed.
    fn sync_user_add_request(req: &AuthUserAddRequest, revision: i64) -> Vec<WriteOp> {
        let mut ops = Vec::new();
        let user = User {
            name: req.name.as_str().into(),
            password: req.hashed_password.as_str().into(),
            options: req.options.clone(),
            roles: Vec::new(),
        };
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutUser(user));
        ops
    }

    /// Sync `AuthUserDeleteRequest` and return whether authstore is changed.
    fn sync_user_delete_request<'a>(
        &self,
        req: &'a AuthUserDeleteRequest,
        revision: i64,
    ) -> Vec<WriteOp<'a>> {
        let mut ops = Vec::new();
        self.permission_cache.map_write(|mut cache| {
            let _ignore = cache.user_permissions.remove(&req.name);
            cache.role_to_users_map.iter_mut().for_each(|(_, users)| {
                if let Some((idx, _)) = users.iter().find_position(|uname| uname == &&req.name) {
                    let _old = users.swap_remove(idx);
                };
            });
        });
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::DeleteUser(req.name.as_str()));
        ops
    }

    /// Sync `AuthUserChangePasswordRequest` and return whether authstore is changed.
    fn sync_user_change_password_request<'a>(
        &self,
        req: &'a AuthUserChangePasswordRequest,
        revision: i64,
    ) -> Result<Vec<WriteOp<'a>>, ExecuteError> {
        let mut ops = Vec::new();
        let mut user = self.backend.get_user(&req.name)?;
        user.password = req.hashed_password.as_str().into();
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutUser(user));
        Ok(ops)
    }

    /// Sync `AuthUserGrantRoleRequest` and return whether authstore is changed.
    fn sync_user_grant_role_request<'a>(
        &self,
        req: &'a AuthUserGrantRoleRequest,
        revision: i64,
    ) -> Result<Vec<WriteOp<'a>>, ExecuteError> {
        let mut ops = Vec::new();
        let mut user = self.backend.get_user(&req.user)?;
        let role = self.backend.get_role(&req.role);
        if (req.role != ROOT_ROLE) && role.is_err() {
            return Err(ExecuteError::RoleNotFound(req.role.clone()));
        }
        let Err(idx) =  user.roles.binary_search(&req.role) else {
            return Err(ExecuteError::UserAlreadyHasRole(req.user.clone(), req.role.clone()));
        };
        user.roles.insert(idx, req.role.clone());
        if let Ok(role) = role {
            let perms = role.key_permission;
            self.permission_cache.map_write(|mut cache| {
                let entry = cache
                    .user_permissions
                    .entry(req.user.clone())
                    .or_insert_with(UserPermissions::new);
                for perm in perms {
                    entry.insert(perm);
                }
                cache
                    .role_to_users_map
                    .entry(req.role.clone())
                    .or_insert_with(Vec::new)
                    .push(req.user.clone());
            });
        }
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutUser(user));
        Ok(ops)
    }

    /// Sync `AuthUserRevokeRoleRequest` and return whether authstore is changed.
    fn sync_user_revoke_role_request<'a>(
        &self,
        req: &'a AuthUserRevokeRoleRequest,
        revision: i64,
    ) -> Result<Vec<WriteOp<'a>>, ExecuteError> {
        let mut ops = Vec::new();
        let mut user = self.backend.get_user(&req.name)?;
        let idx = user
            .roles
            .binary_search(&req.role)
            .map_err(|_ignore| ExecuteError::RoleNotGranted(req.role.clone()))?;
        let _ignore = user.roles.remove(idx);
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
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutUser(user));
        Ok(ops)
    }

    /// Sync `AuthRoleAddRequest` and return whether authstore is changed.
    fn sync_role_add_request(req: &AuthRoleAddRequest, revision: i64) -> Vec<WriteOp> {
        let mut ops = Vec::new();
        let role = Role {
            name: req.name.as_str().into(),
            key_permission: Vec::new(),
        };
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutRole(role));
        ops
    }

    /// Sync `AuthRoleDeleteRequest` and return whether authstore is changed.
    fn sync_role_delete_request<'a>(
        &self,
        req: &'a AuthRoleDeleteRequest,
        revision: i64,
    ) -> Result<Vec<WriteOp<'a>>, ExecuteError> {
        let mut ops = Vec::new();
        let users = self.backend.get_all_users()?;
        let mut new_perms = HashMap::new();
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::DeleteRole(req.role.as_str()));
        for mut user in users {
            if let Ok(idx) = user.roles.binary_search(&req.role) {
                let _ignore = user.roles.remove(idx);
                let perms = self.get_user_permissions(&user, None);
                let _old = new_perms.insert(String::from_utf8_lossy(&user.name).to_string(), perms);
                ops.push(WriteOp::PutUser(user));
            }
        }
        self.permission_cache.map_write(|mut cache| {
            cache.user_permissions.extend(new_perms.into_iter());
            let _ignore = cache.role_to_users_map.remove(&req.role);
        });
        Ok(ops)
    }

    /// Sync `AuthRoleGrantPermissionRequest` and return whether authstore is changed.
    fn sync_role_grant_permission_request<'a>(
        &self,
        req: &'a AuthRoleGrantPermissionRequest,
        revision: i64,
    ) -> Result<Vec<WriteOp<'a>>, ExecuteError> {
        let mut ops = Vec::new();
        let mut role = self.backend.get_role(&req.name)?;
        let permission = req.perm.clone().ok_or(ExecuteError::PermissionNotGiven)?;

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
        self.permission_cache.map_write(move |mut cache| {
            let users = cache
                .role_to_users_map
                .get(&req.name)
                .cloned()
                .unwrap_or_default();
            for user in users {
                let entry = cache
                    .user_permissions
                    .entry(user)
                    .or_insert_with(UserPermissions::new);
                entry.insert(permission.clone());
            }
        });
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutRole(role));
        Ok(ops)
    }

    /// Sync `AuthRoleRevokePermissionRequest` and return whether authstore is changed.
    fn sync_role_revoke_permission_request<'a>(
        &self,
        req: &'a AuthRoleRevokePermissionRequest,
        revision: i64,
    ) -> Result<Vec<WriteOp<'a>>, ExecuteError> {
        let mut ops = Vec::new();
        let mut role = self.backend.get_role(&req.role)?;
        let idx = role
            .key_permission
            .binary_search_by(|p| match p.key.cmp(&req.key) {
                Ordering::Equal => p.range_end.cmp(&req.range_end),
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            })
            .map_err(|_ignore| ExecuteError::PermissionNotGranted)?;
        let _ignore = role.key_permission.remove(idx);
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
        ops.push(WriteOp::PutAuthRevision(revision));
        ops.push(WriteOp::PutRole(role));
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
    ) -> Result<(), ExecuteError> {
        if !self.is_enabled() {
            return Err(ExecuteError::AuthNotEnabled);
        }
        let user = self.backend.get_user(username)?;
        let need_password = user.options.as_ref().map_or(true, |o| !o.no_password);
        if !need_password {
            return Err(ExecuteError::NoPasswordUser);
        }

        let hash = String::from_utf8_lossy(&user.password);
        let hash = PasswordHash::new(&hash)
            .unwrap_or_else(|e| panic!("Failed to parse password hash, error: {e}"));
        Pbkdf2
            .verify_password(password.as_bytes(), &hash)
            .map_err(|_ignore| ExecuteError::AuthFailed)?;

        Ok(())
    }

    /// Check if the request need admin permission
    fn need_admin_permission(wrapper: &RequestWrapper) -> bool {
        matches!(
            *wrapper,
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
    pub(crate) fn check_permission(
        &self,
        wrapper: &RequestWrapper,
        auth_info: Option<&AuthInfo>,
    ) -> Result<(), ExecuteError> {
        if !self.is_enabled() {
            return Ok(());
        }
        if let RequestWrapper::AuthenticateRequest(_) = *wrapper {
            return Ok(());
        }
        let Some(auth_info) = auth_info else {
            // TODO: some requests are allowed without token when auth is enabled
            return Err(ExecuteError::TokenNotProvided);
        };
        let cur_rev = self.revision();
        if auth_info.auth_revision < cur_rev {
            return Err(ExecuteError::TokenOldRevision(
                auth_info.auth_revision,
                cur_rev,
            ));
        }
        let username = &auth_info.username;
        if Self::need_admin_permission(wrapper) {
            self.check_admin_permission(username)?;
        } else {
            #[allow(clippy::wildcard_enum_match_arm)]
            match *wrapper {
                RequestWrapper::RangeRequest(ref range_req) => {
                    self.check_range_permission(username, range_req)?;
                }
                RequestWrapper::PutRequest(ref put_req) => {
                    self.check_put_permission(username, put_req)?;
                }
                RequestWrapper::DeleteRangeRequest(ref del_range_req) => {
                    self.check_delete_permission(username, del_range_req)?;
                }
                RequestWrapper::TxnRequest(ref txn_req) => {
                    self.check_txn_permission(username, txn_req)?;
                }
                RequestWrapper::LeaseRevokeRequest(ref lease_revoke_req) => {
                    self.check_lease_revoke_permission(username, lease_revoke_req)?;
                }
                RequestWrapper::AuthUserGetRequest(ref user_get_req) => {
                    self.check_admin_permission(username).map_or_else(
                        |e| {
                            if user_get_req.name == *username {
                                Ok(())
                            } else {
                                Err(e)
                            }
                        },
                        |_| Ok(()),
                    )?;
                }
                RequestWrapper::AuthRoleGetRequest(ref role_get_req) => {
                    self.check_admin_permission(username).map_or_else(
                        |e| {
                            let user = self.backend.get_user(username)?;
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
    fn check_put_permission(&self, username: &str, req: &PutRequest) -> Result<(), ExecuteError> {
        if req.prev_kv {
            self.check_op_permission(username, &req.key, &[], Type::Read)?;
        }
        self.check_lease(username, req.lease)?;
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
                    self.check_put_permission(username, put_req)?;
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
    fn check_lease_revoke_permission(
        &self,
        username: &str,
        req: &LeaseRevokeRequest,
    ) -> Result<(), ExecuteError> {
        self.check_lease(username, req.id)
    }

    /// check if user can revoke lease
    fn check_lease(&self, username: &str, lease_id: i64) -> Result<(), ExecuteError> {
        let lease = self.look_up(lease_id);
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
                    if permissions.read.contains_range(&key_range) {
                        return Ok(());
                    }
                }
                Type::Write => {
                    if permissions.write.contains_range(&key_range) {
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

/// Get common name from tonic request
fn get_cn<T>(request: &tonic::Request<T>) -> Option<String> {
    let chain = request.peer_certs()?;
    let cert_der = chain.first()?;
    let cert = x509_certificate::X509Certificate::from_der(cert_der.as_ref()).ok()?;
    cert.subject_common_name()
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use merged_range::MergedRange;
    use utils::config::EngineConfig;

    use super::*;
    use crate::{
        rpc::{
            AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGrantPermissionRequest,
            AuthRoleRevokePermissionRequest, AuthUserAddRequest, AuthUserDeleteRequest,
            AuthUserGrantRoleRequest, Permission,
        },
        storage::{
            auth_store::perms::{PermissionCache, UserPermissions},
            db::DB,
        },
    };

    #[test]
    fn test_token_assign_and_verify() {
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let store = init_auth_store(db);
        let current_revision = store.revision();
        let token = store.assign("xline").unwrap();
        let auth_info = store.verify(token.as_str()).unwrap();
        assert_eq!(auth_info.auth_revision, current_revision);
        assert_eq!(auth_info.username, "xline");
    }

    #[test]
    fn test_role_grant_permission() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let store = init_auth_store(db);
        let req = RequestWrapper::from(AuthRoleGrantPermissionRequest {
            name: "r".to_owned(),
            perm: Some(Permission {
                    #[allow(clippy::as_conversions)] // This cast is always valid
                    perm_type: Type::Write as i32,
                    key: "fop".into(),
                    range_end: "foz".into(),
                }),
        });
        assert!(exe_and_sync(&store, &req, 6).is_ok());
        assert_eq!(
            store.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([(
                    "u".to_owned(),
                    UserPermissions {
                        read: MergedRange::from_iter(vec![KeyRange::new("foo", "")]),
                        write: MergedRange::from_iter(vec![
                            KeyRange::new("foo", ""),
                            KeyRange::new("fop", "foz")
                        ]),
                    },
                )]),
                role_to_users_map: HashMap::from([("r".to_owned(), vec!["u".to_owned()])]),
            },
        );
        Ok(())
    }

    #[test]
    fn test_role_revoke_permission() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory)?;
        let store = init_auth_store(db);
        let req = RequestWrapper::from(AuthRoleRevokePermissionRequest {
            role: "r".to_owned(),
            key: "foo".into(),
            range_end: "".into(),
        });
        assert!(exe_and_sync(&store, &req, 6).is_ok());
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
        let db = DB::open(&EngineConfig::Memory)?;
        let store = init_auth_store(db);
        let req = RequestWrapper::from(AuthRoleDeleteRequest {
            role: "r".to_owned(),
        });
        assert!(exe_and_sync(&store, &req, 6).is_ok());
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
        let db = DB::open(&EngineConfig::Memory)?;
        let store = init_auth_store(db);
        let req = RequestWrapper::from(AuthUserDeleteRequest {
            name: "u".to_owned(),
        });
        assert!(exe_and_sync(&store, &req, 6).is_ok());
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
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let store = init_auth_store(db);
        let revision = store.revision();
        let rev_gen = Arc::clone(&store.revision);
        assert!(!store.is_enabled());
        let enable_req = RequestWrapper::from(AuthEnableRequest {});

        // AuthEnableRequest won't increase the auth revision, but AuthDisableRequest will
        assert!(exe_and_sync(&store, &enable_req, store.revision()).is_err());
        let req_1 = RequestWrapper::from(AuthUserAddRequest {
            name: "root".to_owned(),
            password: String::new(),
            hashed_password: "123".to_owned(),
            options: None,
        });
        assert!(exe_and_sync(&store, &req_1, rev_gen.next()).is_ok());

        let req_2 = RequestWrapper::from(AuthRoleAddRequest {
            name: "root".to_owned(),
        });
        assert!(exe_and_sync(&store, &req_2, rev_gen.next()).is_ok());

        let req_3 = RequestWrapper::from(AuthUserGrantRoleRequest {
            user: "root".to_owned(),
            role: "root".to_owned(),
        });
        assert!(exe_and_sync(&store, &req_3, rev_gen.next()).is_ok());
        assert_eq!(store.revision(), revision + 3);

        assert!(exe_and_sync(&store, &enable_req, -1).is_ok());
        assert_eq!(store.revision(), 8);
        assert!(store.is_enabled());

        let disable_req = RequestWrapper::from(AuthDisableRequest {});

        assert!(exe_and_sync(&store, &disable_req, rev_gen.next()).is_ok());
        assert_eq!(store.revision(), revision + 4);
        assert!(!store.is_enabled());
    }

    #[test]
    fn test_recover() -> Result<(), ExecuteError> {
        let db = DB::open(&EngineConfig::Memory).unwrap();
        let store = init_auth_store(Arc::clone(&db));

        let new_store = init_empty_store(db);
        assert_eq!(new_store.permission_cache(), PermissionCache::new());
        new_store.recover()?;
        assert_eq!(store.permission_cache(), new_store.permission_cache());
        assert_eq!(store.revision(), new_store.revision());

        Ok(())
    }

    fn init_auth_store(db: Arc<DB>) -> AuthStore<DB> {
        let store = init_empty_store(db);
        let rev = Arc::clone(&store.revision);
        let req1 = RequestWrapper::from(AuthRoleAddRequest {
            name: "r".to_owned(),
        });
        assert!(exe_and_sync(&store, &req1, rev.next()).is_ok());
        let req2 = RequestWrapper::from(AuthUserAddRequest {
            name: "u".to_owned(),
            password: String::new(),
            hashed_password: "123".to_owned(),
            options: None,
        });
        assert!(exe_and_sync(&store, &req2, rev.next()).is_ok());
        let req3 = RequestWrapper::from(AuthUserGrantRoleRequest {
            user: "u".to_owned(),
            role: "r".to_owned(),
        });
        assert!(exe_and_sync(&store, &req3, rev.next()).is_ok());
        let req4 = RequestWrapper::from(AuthRoleGrantPermissionRequest {
            name: "r".to_owned(),
            perm: Some(Permission {
                    #[allow(clippy::as_conversions)] // This cast is always valid
                    perm_type: Type::Readwrite as i32,
                    key: b"foo".to_vec(),
                    range_end: vec![],
                }),
        });
        assert!(exe_and_sync(&store, &req4, rev.next()).is_ok());
        assert_eq!(
            store.permission_cache(),
            PermissionCache {
                user_permissions: HashMap::from([(
                    "u".to_owned(),
                    UserPermissions {
                        read: MergedRange::from_iter(vec![KeyRange::new("foo", "")]),
                        write: MergedRange::from_iter(vec![KeyRange::new("foo", "")]),
                    },
                )]),
                role_to_users_map: HashMap::from([("r".to_owned(), vec!["u".to_owned()])]),
            },
        );
        store
    }

    fn init_empty_store(db: Arc<DB>) -> AuthStore<DB> {
        let key_pair = test_key_pair();
        let header_gen = Arc::new(HeaderGenerator::new(0, 0));
        let lease_collection = Arc::new(LeaseCollection::new(0));
        AuthStore::new(lease_collection, key_pair, header_gen, db)
    }

    fn exe_and_sync(
        store: &AuthStore<DB>,
        req: &RequestWrapper,
        revision: i64,
    ) -> Result<(CommandResponse, SyncResponse), ExecuteError> {
        let cmd_res = store.execute(req)?;
        let (sync_res, ops) = store.after_sync(req, revision)?;
        store.backend.flush_ops(ops)?;
        Ok((cmd_res, sync_res))
    }

    fn test_key_pair() -> Option<(EncodingKey, DecodingKey)> {
        let private_key = include_bytes!("../../../../../fixtures/private.pem");
        let public_key = include_bytes!("../../../../../fixtures/public.pem");
        let encoding_key = EncodingKey::from_rsa_pem(private_key).ok()?;
        let decoding_key = DecodingKey::from_rsa_pem(public_key).ok()?;
        Some((encoding_key, decoding_key))
    }
}
