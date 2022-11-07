use std::sync::Arc;

use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use log::debug;
use pbkdf2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Pbkdf2,
};
use uuid::Uuid;

use crate::{
    rpc::{
        Auth, AuthDisableRequest, AuthDisableResponse, AuthEnableRequest, AuthEnableResponse,
        AuthRoleAddRequest, AuthRoleAddResponse, AuthRoleDeleteRequest, AuthRoleDeleteResponse,
        AuthRoleGetRequest, AuthRoleGetResponse, AuthRoleGrantPermissionRequest,
        AuthRoleGrantPermissionResponse, AuthRoleListRequest, AuthRoleListResponse,
        AuthRoleRevokePermissionRequest, AuthRoleRevokePermissionResponse, AuthStatusRequest,
        AuthStatusResponse, AuthUserAddRequest, AuthUserAddResponse, AuthUserChangePasswordRequest,
        AuthUserChangePasswordResponse, AuthUserDeleteRequest, AuthUserDeleteResponse,
        AuthUserGetRequest, AuthUserGetResponse, AuthUserGrantRoleRequest,
        AuthUserGrantRoleResponse, AuthUserListRequest, AuthUserListResponse,
        AuthUserRevokeRoleRequest, AuthUserRevokeRoleResponse, AuthenticateRequest,
        AuthenticateResponse, RequestWrapper,
    },
    storage::AuthStore,
};

use super::command::{Command, CommandResponse, SyncResponse};

/// Auth Server
#[derive(Debug)]
#[allow(unused)]
pub(crate) struct AuthServer {
    /// Auth storage
    storage: Arc<AuthStore>,
    /// Consensus client
    client: Arc<Client<Command>>,
    /// Server name
    name: String,
}

impl AuthServer {
    /// New `AuthServer`
    pub(crate) fn new(storage: Arc<AuthStore>, client: Arc<Client<Command>>, name: String) -> Self {
        Self {
            storage,
            client,
            name,
        }
    }

    /// Generate propose id
    fn generate_propose_id(&self) -> ProposeId {
        ProposeId::new(format!("{}-{}", self.name, Uuid::new_v4()))
    }

    /// Generate `Command` proposal from `Request`
    fn command_from_request_wrapper(propose_id: ProposeId, wrapper: &RequestWrapper) -> Command {
        let bin_req = bincode::serialize(&wrapper)
            .unwrap_or_else(|e| panic!("Failed to serialize RequestWrapper, error: {e}"));
        Command::new(vec![], bin_req, propose_id)
    }

    /// Propose request and get result with slow path
    async fn propose_slow_path(
        &self,
        propose_id: ProposeId,
        wrapper: RequestWrapper,
    ) -> Result<(CommandResponse, SyncResponse), tonic::Status> {
        let cmd = Self::command_from_request_wrapper(propose_id, &wrapper);
        self.client.propose_indexed(cmd).await.map_err(|err| {
            if let ProposeError::ExecutionError(e) = err {
                tonic::Status::invalid_argument(e)
            } else {
                panic!("propose err {err:?}")
            }
        })
    }

    /// Hash password
    fn hash_password(password: &[u8]) -> String {
        let salt = SaltString::generate(&mut OsRng);
        let hashed_password = Pbkdf2
            .hash_password(password, salt.as_ref())
            .unwrap_or_else(|e| panic!("Failed to hash password: {}", e));
        hashed_password.to_string()
    }
}

#[tonic::async_trait]
impl Auth for AuthServer {
    async fn auth_enable(
        &self,
        request: tonic::Request<AuthEnableRequest>,
    ) -> Result<tonic::Response<AuthEnableResponse>, tonic::Status> {
        debug!("Receive AuthEnableRequest {:?}", request);
        let auth_enable_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, auth_enable_req.into())
            .await?;

        let mut res: AuthEnableResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthEnableResponse", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn auth_disable(
        &self,
        request: tonic::Request<AuthDisableRequest>,
    ) -> Result<tonic::Response<AuthDisableResponse>, tonic::Status> {
        debug!("Receive AuthDisableRequest {:?}", request);
        let auth_disable_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, auth_disable_req.into())
            .await?;

        let mut res: AuthDisableResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthDisableResponse", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn auth_status(
        &self,
        request: tonic::Request<AuthStatusRequest>,
    ) -> Result<tonic::Response<AuthStatusResponse>, tonic::Status> {
        debug!("Receive AuthStatusRequest {:?}", request);
        let auth_status_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, auth_status_req.into())
            .await?;

        let mut res: AuthStatusResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthStatusResponse", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn authenticate(
        &self,
        request: tonic::Request<AuthenticateRequest>,
    ) -> Result<tonic::Response<AuthenticateResponse>, tonic::Status> {
        debug!("Receive AuthenticateRequest {:?}", request);
        // TODO: add implementation
        Err(tonic::Status::unimplemented("Not implemented"))
    }

    async fn user_add(
        &self,
        request: tonic::Request<AuthUserAddRequest>,
    ) -> Result<tonic::Response<AuthUserAddResponse>, tonic::Status> {
        debug!("Receive AuthUserAddRequest {:?}", request);
        let mut user_add_req = request.into_inner();
        if user_add_req.name.is_empty() {
            return Err(tonic::Status::invalid_argument("user name is empty"));
        }
        let need_password = user_add_req
            .options
            .as_ref()
            .map_or(true, |o| !o.no_password);
        if need_password && user_add_req.password.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "password is required but not provided",
            ));
        }
        let hashed_password = Self::hash_password(user_add_req.password.as_bytes());
        user_add_req.hashed_password = hashed_password;
        user_add_req.password = "".to_owned();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, user_add_req.into())
            .await?;

        let mut res: AuthUserAddResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthUserAddRequest", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn user_get(
        &self,
        request: tonic::Request<AuthUserGetRequest>,
    ) -> Result<tonic::Response<AuthUserGetResponse>, tonic::Status> {
        debug!("Receive AuthUserGetRequest {:?}", request);
        let user_get_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, user_get_req.into())
            .await?;

        let mut res: AuthUserGetResponse = res.decode().into();
        if let Some(mut header) = res.header.as_mut() {
            header.revision = sync_res.revision();
        }
        Ok(tonic::Response::new(res))
    }

    async fn user_list(
        &self,
        request: tonic::Request<AuthUserListRequest>,
    ) -> Result<tonic::Response<AuthUserListResponse>, tonic::Status> {
        debug!("Receive AuthUserListRequest {:?}", request);
        let user_list_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, user_list_req.into())
            .await?;

        let mut res: AuthUserListResponse = res.decode().into();
        if let Some(mut header) = res.header.as_mut() {
            header.revision = sync_res.revision();
        }
        Ok(tonic::Response::new(res))
    }

    async fn user_delete(
        &self,
        request: tonic::Request<AuthUserDeleteRequest>,
    ) -> Result<tonic::Response<AuthUserDeleteResponse>, tonic::Status> {
        debug!("Receive AuthUserDeleteRequest {:?}", request);
        let user_delete_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, user_delete_req.into())
            .await?;

        let mut res: AuthUserDeleteResponse = res.decode().into();
        if let Some(mut header) = res.header.as_mut() {
            header.revision = sync_res.revision();
        }
        Ok(tonic::Response::new(res))
    }

    async fn user_change_password(
        &self,
        request: tonic::Request<AuthUserChangePasswordRequest>,
    ) -> Result<tonic::Response<AuthUserChangePasswordResponse>, tonic::Status> {
        debug!("Receive AuthUserChangePasswordRequest {:?}", request);
        let mut user_change_password_req = request.into_inner();
        let hashed_password = Self::hash_password(user_change_password_req.password.as_bytes());
        user_change_password_req.hashed_password = hashed_password;
        user_change_password_req.password = "".to_owned();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, user_change_password_req.into())
            .await?;

        let mut res: AuthUserChangePasswordResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!(
            "Get revision {:?} for AuthUserChangePasswordRequest",
            revision
        );
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn user_grant_role(
        &self,
        request: tonic::Request<AuthUserGrantRoleRequest>,
    ) -> Result<tonic::Response<AuthUserGrantRoleResponse>, tonic::Status> {
        debug!("Receive AuthUserGrantRoleRequest {:?}", request);
        let user_grant_role_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, user_grant_role_req.into())
            .await?;

        let mut res: AuthUserGrantRoleResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthUserGrantRoleRequest", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn user_revoke_role(
        &self,
        request: tonic::Request<AuthUserRevokeRoleRequest>,
    ) -> Result<tonic::Response<AuthUserRevokeRoleResponse>, tonic::Status> {
        debug!("Receive AuthUserRevokeRoleRequest {:?}", request);
        let user_revoke_role_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, user_revoke_role_req.into())
            .await?;

        let mut res: AuthUserRevokeRoleResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthUserRevokeRoleRequest", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn role_add(
        &self,
        request: tonic::Request<AuthRoleAddRequest>,
    ) -> Result<tonic::Response<AuthRoleAddResponse>, tonic::Status> {
        debug!("Receive AuthRoleAddRequest {:?}", request);
        let role_add_req = request.into_inner();
        if role_add_req.name.is_empty() {
            return Err(tonic::Status::invalid_argument("Role name is empty"));
        }
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, role_add_req.into())
            .await?;

        let mut res: AuthRoleAddResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthRoleAddResponse", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn role_get(
        &self,
        request: tonic::Request<AuthRoleGetRequest>,
    ) -> Result<tonic::Response<AuthRoleGetResponse>, tonic::Status> {
        debug!("Receive AuthRoleGetRequest {:?}", request);
        let role_get_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, role_get_req.into())
            .await?;

        let mut res: AuthRoleGetResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthRoleGetResponse", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn role_list(
        &self,
        request: tonic::Request<AuthRoleListRequest>,
    ) -> Result<tonic::Response<AuthRoleListResponse>, tonic::Status> {
        debug!("Receive AuthRoleListRequest {:?}", request);
        let role_list_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, role_list_req.into())
            .await?;

        let mut res: AuthRoleListResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthRoleListResponse", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn role_delete(
        &self,
        request: tonic::Request<AuthRoleDeleteRequest>,
    ) -> Result<tonic::Response<AuthRoleDeleteResponse>, tonic::Status> {
        debug!("Receive AuthRoleDeleteRequest {:?}", request);
        let role_delete_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, role_delete_req.into())
            .await?;

        let mut res: AuthRoleDeleteResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!("Get revision {:?} for AuthRoleDeleteResponse", revision);
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn role_grant_permission(
        &self,
        request: tonic::Request<AuthRoleGrantPermissionRequest>,
    ) -> Result<tonic::Response<AuthRoleGrantPermissionResponse>, tonic::Status> {
        debug!("Receive AuthRoleGrantPermissionRequest {:?}", request);
        let role_grant_permission_req = request.into_inner();
        if role_grant_permission_req.perm.is_none() {
            return Err(tonic::Status::invalid_argument("Permission not given"));
        }
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, role_grant_permission_req.into())
            .await?;

        let mut res: AuthRoleGrantPermissionResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!(
            "Get revision {:?} for AuthRoleGrantPermissionResponse",
            revision
        );
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<AuthRoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<AuthRoleRevokePermissionResponse>, tonic::Status> {
        debug!("Receive AuthRoleRevokePermissionRequest {:?}", request);
        let role_revoke_permission_req = request.into_inner();
        let propose_id = self.generate_propose_id();

        let (res, sync_res) = self
            .propose_slow_path(propose_id, role_revoke_permission_req.into())
            .await?;

        let mut res: AuthRoleRevokePermissionResponse = res.decode().into();
        let revision = sync_res.revision();
        debug!(
            "Get revision {:?} for AuthRoleRevokePermissionResponse",
            revision
        );
        if let Some(mut header) = res.header.as_mut() {
            header.revision = revision;
        }
        Ok(tonic::Response::new(res))
    }
}
