use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc,
    },
};

use anyhow::Result;
use clippy_utilities::Cast;
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

use super::perms::{JwtTokenManager, PermissionCache, TokenClaims, TokenOperate, UserPermissions};
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
        AuthenticateResponse, Permission, RequestWrapper, ResponseWrapper, Role, Type, User,
    },
    server::command::KeyRange,
    storage::{
        lease_store::{Lease, LeaseMessage},
        storage_api::StorageApi,
        ExecuteError
    },
};

/// User table
pub(crate) const USER_TABLE: &str = "user";
/// Role table
pub(crate) const ROLE_TABLE: &str = "role";
/// Auth table
pub(crate) const AUTH_TABLE: &str = "auth";
/// Key of `AuthEnable`
pub(crate) const AUTH_ENABLE_KEY: &[u8] = b"auth_enable";
/// Root user
pub(crate) const ROOT_USER: &str = "root";
/// Root role
pub(crate) const ROOT_ROLE: &str = "root";

/// Auth store inner
pub(crate) struct AuthStoreBackend<DB>
where
    DB: StorageApi,
{
    /// DB to store key value
    db: Arc<DB>,
    /// Revision
    revision: RevisionNumber,
    /// Enabled
    enabled: AtomicBool,
    /// Permission cache
    permission_cache: RwLock<PermissionCache>,
    /// The manager of token
    token_manager: Option<JwtTokenManager>,
    /// Lease command sender
    lease_cmd_tx: mpsc::Sender<LeaseMessage>,
    /// Header generator
    header_gen: Arc<HeaderGenerator>,
}

impl<DB> fmt::Debug for AuthStoreBackend<DB>
where
    DB: StorageApi,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthStoreBackend")
            .field("db", &self.db)
            .field("revision", &self.revision)
            .field("enabled", &self.enabled)
            .field("permission_cache", &self.permission_cache)
            .field("lease_cmd_tx", &self.lease_cmd_tx)
            .field("header_gen", &self.header_gen)
            .finish()
    }
}

impl<DB> AuthStoreBackend<DB>
where
    DB: StorageApi,
{
    /// New `AuthStoreBackend`
    pub(super) fn new(
        lease_cmd_tx: mpsc::Sender<LeaseMessage>,
        key_pair: Option<(EncodingKey, DecodingKey)>,
        header_gen: Arc<HeaderGenerator>,
        db: Arc<DB>,
    ) -> Self {
        Self {
            db,
            revision: RevisionNumber::new(),
            enabled: AtomicBool::new(false),
            token_manager: key_pair.map(|(encoding_key, decoding_key)| {
                JwtTokenManager::new(encoding_key, decoding_key)
            }),
            permission_cache: RwLock::new(PermissionCache::new()),
            lease_cmd_tx,
            header_gen,
        }
    }

    /// Get Lease by lease id
    pub(super) async fn get_lease(&self, lease_id: i64) -> Option<Lease> {
        let (detach, rx) = LeaseMessage::look_up(lease_id);
        assert!(
            self.lease_cmd_tx.send(detach).await.is_ok(),
            "lease_cmd_tx is closed"
        );
        rx.await.unwrap_or_else(|_e| panic!("res sender is closed"))
    }

    /// Get revision of Auth store
    pub(crate) fn revision(&self) -> i64 {
        self.revision.get()
    }

    /// Get enabled of Auth store
    pub(super) fn is_enabled(&self) -> bool {
        self.enabled.load(AtomicOrdering::Acquire)
    }

    /// Check password
    pub(super) fn check_password(
        &self,
        username: &str,
        password: &str,
    ) -> Result<i64, ExecuteError> {
        if !self.is_enabled() {
            return Err(ExecuteError::auth_not_enabled());
        }
        let user = self.get_user(username)?;
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

    /// Assign token
    pub(super) fn assign(&self, username: &str) -> Result<String, ExecuteError> {
        match self.token_manager {
            Some(ref token_manager) => token_manager
                .assign(username, self.revision())
                .map_err(|_ignore| ExecuteError::invalid_auth_token()),
            None => Err(ExecuteError::token_manager_not_init()),
        }
    }

    /// verify token
    pub(super) fn verify_token(&self, token: &str) -> Result<TokenClaims, ExecuteError> {
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
        for user in self.get_all_users()? {
            let user_permission = self.get_user_permissions(&user);
            let username = String::from_utf8_lossy(&user.name).to_string();
            let _ignore = permission_cache
                .user_permissions
                .insert(username, user_permission);
        }
        self.permission_cache
            .map_write(|mut cache| *cache = permission_cache);
        Ok(())
    }

    /// get user permissions
    fn get_user_permissions(&self, user: &User) -> UserPermissions {
        let mut user_permission = UserPermissions::new();
        for role_name in &user.roles {
            let Ok(role) = self.get_role(role_name) else {
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

    /// Check permission by username and key range
    pub(super) fn check_permission(
        &self,
        username: &str,
        key_range: &KeyRange,
        permission_type: Type,
    ) -> Result<(), ExecuteError> {
        if let Some(permissions) = self.permission_cache.read().user_permissions.get(username) {
            match permission_type {
                Type::Read => {
                    if permissions
                        .read
                        .iter()
                        .any(|kr| kr.contains_range(key_range))
                    {
                        return Ok(());
                    }
                }
                Type::Write => {
                    if permissions
                        .write
                        .iter()
                        .any(|kr| kr.contains_range(key_range))
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

    /// get user by username
    pub(super) fn get_user(&self, username: &str) -> Result<User, ExecuteError> {
        match self.db.get_value(USER_TABLE, username)? {
            Some(value) => Ok(User::decode(value.as_slice()).unwrap_or_else(|e| {
                panic!("Failed to decode user from value, error: {e:?}, value: {value:?}");
            })),
            None => Err(ExecuteError::user_not_found(username)),
        }
    }

    /// get role by rolename
    fn get_role(&self, rolename: &str) -> Result<Role, ExecuteError> {
        match self.db.get_value(ROLE_TABLE, rolename)? {
            Some(value) => Ok(Role::decode(value.as_slice()).unwrap_or_else(|e| {
                panic!("Failed to decode role from value, error: {e:?}, value: {value:?}");
            })),
            None => Err(ExecuteError::role_not_found(rolename)),
        }
    }

    /// put user to `AuthStore`
    fn put_user(&self, user: &User) -> Result<(), ExecuteError> {
        let key = user.name.clone();
        let value = user.encode_to_vec();
        self.db.insert(USER_TABLE, key, value, false)?;
        let rev = self.revision.next();
        self.db
            .insert(AUTH_TABLE, "revision", rev.to_le_bytes(), false)
    }

    /// put role to `AuthStore`
    fn put_role(&self, role: &Role) -> Result<(), ExecuteError> {
        let key = role.name.clone();
        let value = role.encode_to_vec();
        self.db.insert(ROLE_TABLE, key, value, false)?;
        let rev = self.revision.next();
        self.db
            .insert(AUTH_TABLE, "revision", rev.to_le_bytes(), false)
    }

    /// Get all users in the `AuthStore`
    fn get_all_users(&self) -> Result<Vec<User>, ExecuteError> {
        let users = self
            .db
            .get_all(USER_TABLE)?
            .into_iter()
            .map(|(_, user)| {
                User::decode(user.as_slice()).unwrap_or_else(|e| {
                    panic!("Failed to decode user from value, error: {e:?}, user: {user:?}");
                })
            })
            .collect();
        Ok(users)
    }

    /// Get all roles in the `AuthStore`
    fn get_all_roles(&self) -> Result<Vec<Role>, ExecuteError> {
        let roles = self
            .db
            .get_all(ROLE_TABLE)?
            .into_iter()
            .map(|(_, value)| {
                Role::decode(value.as_slice()).unwrap_or_else(|e| {
                    panic!("Failed to decode role from value, error: {e:?}, value: {value:?}");
                })
            })
            .collect();
        Ok(roles)
    }

    /// Handle `InternalRequest`
    pub(super) fn handle_auth_req(
        &self,
        wrapper: &RequestWrapper,
    ) -> Result<ResponseWrapper, ExecuteError> {
        // routed when call execute, other request will be routed to other backend
        #[allow(clippy::wildcard_enum_match_arm)]
        match *wrapper {
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
        }
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
        let user = self.get_user(ROOT_USER)?;
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
        if self.get_user(&req.name).is_ok() {
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
        let user = self.get_user(&req.name)?;
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
        let _user = self.get_user(&req.name)?;
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
        let user = self.get_user(&req.name)?;
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
        let _user = self.get_user(&req.user)?;
        if req.role != ROOT_ROLE {
            let _role = self.get_role(&req.role)?;
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
        let user = self.get_user(&req.name)?;
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
        if self.get_role(&req.name).is_ok() {
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
        let _role = self.get_role(&req.role)?;
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
        let _role = self.get_role(&req.name)?;
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
            return Err(ExecuteError::permission_not_granted());
        }
        Ok(AuthRoleRevokePermissionResponse {
            header: Some(self.header_gen.gen_header_without_revision()),
        })
    }

    /// Sync `RequestWrapper`
    pub(super) fn sync_request(&self, wrapper: &RequestWrapper) -> Result<i64, ExecuteError> {
        #[allow(clippy::wildcard_enum_match_arm)]
        match *wrapper {
            RequestWrapper::AuthEnableRequest(ref req) => {
                debug!("Sync AuthEnableRequest {:?}", req);
                self.sync_auth_enable_request(req)?;
            }
            RequestWrapper::AuthDisableRequest(ref req) => {
                debug!("Sync AuthDisableRequest {:?}", req);
                self.sync_auth_disable_request(req)?;
            }
            RequestWrapper::AuthStatusRequest(ref req) => {
                debug!("Sync AuthStatusRequest {:?}", req);
            }
            RequestWrapper::AuthUserAddRequest(ref req) => {
                debug!("Sync AuthUserAddRequest {:?}", req);
                self.sync_user_add_request(req)?;
            }
            RequestWrapper::AuthUserGetRequest(ref req) => {
                debug!("Sync AuthUserGetRequest {:?}", req);
            }
            RequestWrapper::AuthUserListRequest(ref req) => {
                debug!("Sync AuthUserListRequest {:?}", req);
            }
            RequestWrapper::AuthUserGrantRoleRequest(ref req) => {
                debug!("Sync AuthUserGrantRoleRequest {:?}", req);
                self.sync_user_grant_role_request(req)?;
            }
            RequestWrapper::AuthUserRevokeRoleRequest(ref req) => {
                debug!("Sync AuthUserRevokeRoleRequest {:?}", req);
                self.sync_user_revoke_role_request(req)?;
            }
            RequestWrapper::AuthUserChangePasswordRequest(ref req) => {
                debug!("Sync AuthUserChangePasswordRequest {:?}", req);
                self.sync_user_change_password_request(req)?;
            }
            RequestWrapper::AuthUserDeleteRequest(ref req) => {
                debug!("Sync AuthUserDeleteRequest {:?}", req);
                self.sync_user_delete_request(req)?;
            }
            RequestWrapper::AuthRoleAddRequest(ref req) => {
                debug!("Sync AuthRoleAddRequest {:?}", req);
                self.sync_role_add_request(req)?;
            }
            RequestWrapper::AuthRoleGetRequest(ref req) => {
                debug!("Sync AuthRoleGetRequest {:?}", req);
            }
            RequestWrapper::AuthRoleGrantPermissionRequest(ref req) => {
                debug!("Sync AuthRoleGrantPermissionRequest {:?}", req);
                self.sync_role_grant_permission_request(req)?;
            }
            RequestWrapper::AuthRoleRevokePermissionRequest(ref req) => {
                debug!("Sync AuthRoleRevokePermissionRequest {:?}", req);
                self.sync_role_revoke_permission_request(req)?;
            }
            RequestWrapper::AuthRoleListRequest(ref req) => {
                debug!("Sync AuthRoleListRequest {:?}", req);
            }
            RequestWrapper::AuthRoleDeleteRequest(ref req) => {
                debug!("Sync AuthRoleDeleteRequest {:?}", req);
                self.sync_role_delete_request(req)?;
            }
            RequestWrapper::AuthenticateRequest(ref req) => {
                debug!("Sync AuthenticateRequest {:?}", req);
            }
            _ => {
                unreachable!("Other request should not be sent to this store");
            }
        }
        Ok(self.header_gen.revision())
    }

    /// Sync `AuthEnableRequest` and return whether authstore is changed.
    fn sync_auth_enable_request(&self, _req: &AuthEnableRequest) -> Result<(), ExecuteError> {
        if self.is_enabled() {
            return Ok(());
        }
        self.db
            .insert(AUTH_TABLE, AUTH_ENABLE_KEY, vec![1], false)?;
        self.enabled.store(true, AtomicOrdering::Relaxed);
        let rev = self.revision.next();
        self.db
            .insert(AUTH_TABLE, "revision", rev.to_le_bytes(), false)?;
        self.create_permission_cache()
    }

    /// Sync `AuthDisableRequest` and return whether authstore is changed.
    fn sync_auth_disable_request(&self, _req: &AuthDisableRequest) -> Result<(), ExecuteError> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.db
            .insert(AUTH_TABLE, AUTH_ENABLE_KEY, vec![0], false)?;
        self.enabled.store(false, AtomicOrdering::Relaxed);
        let rev = self.revision.next();
        self.db
            .insert(AUTH_TABLE, "revision", rev.to_le_bytes(), false)
    }

    /// Sync `AuthUserAddRequest` and return whether authstore is changed.
    fn sync_user_add_request(&self, req: &AuthUserAddRequest) -> Result<(), ExecuteError> {
        let user = User {
            name: req.name.as_str().into(),
            password: req.hashed_password.as_str().into(),
            options: req.options.clone(),
            roles: Vec::new(),
        };
        self.put_user(&user)
    }

    /// Sync `AuthUserDeleteRequest` and return whether authstore is changed.
    fn sync_user_delete_request(&self, req: &AuthUserDeleteRequest) -> Result<(), ExecuteError> {
        self.db.delete(USER_TABLE, &req.name, false)?;
        self.permission_cache.map_write(|mut cache| {
            let _ignore = cache.user_permissions.remove(&req.name);
            cache.role_to_users_map.iter_mut().for_each(|(_, users)| {
                if let Some((idx, _)) = users.iter().find_position(|uname| uname == &&req.name) {
                    let _old = users.swap_remove(idx);
                };
            });
        });
        Ok(())
    }

    /// Sync `AuthUserChangePasswordRequest` and return whether authstore is changed.
    fn sync_user_change_password_request(
        &self,
        req: &AuthUserChangePasswordRequest,
    ) -> Result<(), ExecuteError> {
        let mut user = self.get_user(&req.name)?;
        user.password = req.hashed_password.as_str().into();
        self.put_user(&user)
    }

    /// Sync `AuthUserGrantRoleRequest` and return whether authstore is changed.
    fn sync_user_grant_role_request(
        &self,
        req: &AuthUserGrantRoleRequest,
    ) -> Result<(), ExecuteError> {
        let mut user = self.get_user(&req.user)?;
        let role = self.get_role(&req.role);
        if (req.role != ROOT_ROLE) && role.is_err() {
            return Err(ExecuteError::role_not_found(&req.role));
        }
        let Err(idx) =  user.roles.binary_search(&req.role) else {
            return Err(ExecuteError::user_already_has_role(&req.user, &req.role));
        };
        user.roles.insert(idx, req.role.clone());
        self.put_user(&user)?;
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
        Ok(())
    }

    /// Sync `AuthUserRevokeRoleRequest` and return whether authstore is changed.
    fn sync_user_revoke_role_request(
        &self,
        req: &AuthUserRevokeRoleRequest,
    ) -> Result<(), ExecuteError> {
        let mut user = self.get_user(&req.name)?;
        let idx = user
            .roles
            .binary_search(&req.role)
            .map_err(|_ignore| ExecuteError::role_not_granted(&req.role))?;
        let _ignore = user.roles.remove(idx);
        self.permission_cache.map_write(|mut cache| {
            let user_permissions = self.get_user_permissions(&user);
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
        Ok(())
    }

    /// Sync `AuthRoleAddRequest` and return whether authstore is changed.
    fn sync_role_add_request(&self, req: &AuthRoleAddRequest) -> Result<(), ExecuteError> {
        let role = Role {
            name: req.name.as_str().into(),
            key_permission: Vec::new(),
        };
        self.put_role(&role)
    }

    /// Sync `AuthRoleDeleteRequest` and return whether authstore is changed.
    fn sync_role_delete_request(&self, req: &AuthRoleDeleteRequest) -> Result<(), ExecuteError> {
        self.db.delete(ROLE_TABLE, &req.role, false)?;
        let users = self.get_all_users()?;
        let mut new_perms = HashMap::new();
        for mut user in users {
            if let Ok(idx) = user.roles.binary_search(&req.role) {
                let _ignore = user.roles.remove(idx);
                let perms = self.get_user_permissions(&user);
                let _old = new_perms.insert(String::from_utf8_lossy(&user.name).to_string(), perms);
                self.put_user(&user)?;
            }
        }
        self.permission_cache.map_write(|mut cache| {
            cache.user_permissions.extend(new_perms.into_iter());
            let _ignore = cache.role_to_users_map.remove(&req.role);
        });
        Ok(())
    }

    /// Sync `AuthRoleGrantPermissionRequest` and return whether authstore is changed.
    fn sync_role_grant_permission_request(
        &self,
        req: &AuthRoleGrantPermissionRequest,
    ) -> Result<(), ExecuteError> {
        let mut role = self.get_role(&req.name)?;
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
        self.put_role(&role)?;
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
        Ok(())
    }

    /// Sync `AuthRoleRevokePermissionRequest` and return whether authstore is changed.
    fn sync_role_revoke_permission_request(
        &self,
        req: &AuthRoleRevokePermissionRequest,
    ) -> Result<(), ExecuteError> {
        let mut role = self.get_role(&req.role)?;
        let idx = role
            .key_permission
            .binary_search_by(|p| match p.key.cmp(&req.key) {
                Ordering::Equal => p.range_end.cmp(&req.range_end),
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            })
            .map_err(|_ignore| ExecuteError::permission_not_granted())?;
        let _ignore = role.key_permission.remove(idx);
        self.put_role(&role)?;
        self.permission_cache.map_write(|mut cache| {
            let users = cache
                .role_to_users_map
                .get(&req.role)
                .map_or_else(Vec::new, |users| {
                    users
                        .iter()
                        .filter_map(|user| self.get_user(user).ok())
                        .collect::<Vec<_>>()
                });
            for user in users {
                let perms = self.get_user_permissions(&user);
                let _old = cache
                    .user_permissions
                    .insert(String::from_utf8_lossy(&user.name).to_string(), perms);
            }
        });
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn permission_cache(&self) -> PermissionCache {
        self.permission_cache.map_read(|cache| cache.clone())
    }
}
