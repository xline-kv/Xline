use std::sync::Arc;

use curp::{client::Client, cmd::ProposeId, error::ProposeError};
use log::debug;
use pbkdf2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Pbkdf2,
};
use tonic::metadata::MetadataMap;
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
        AuthenticateResponse, RequestWithToken, RequestWrapper,
    },
    storage::AuthStore,
    update_revision,
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

/// Get token from metadata
pub(crate) fn get_token(metadata: &MetadataMap) -> Option<String> {
    metadata
        .get("token")
        .or_else(|| metadata.get("authorization"))
        .and_then(|v| v.to_str().map(String::from).ok())
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
    fn command_from_request_wrapper(propose_id: ProposeId, wrapper: RequestWithToken) -> Command {
        Command::new(vec![], wrapper, propose_id)
    }

    /// Propose request and get result with fast/slow path
    async fn propose<T>(
        &self,
        request: tonic::Request<T>,
        use_fast_path: bool,
    ) -> Result<(CommandResponse, Option<SyncResponse>), tonic::Status>
    where
        T: Into<RequestWrapper>,
    {
        let wrapper = match get_token(request.metadata()) {
            Some(token) => RequestWithToken::new_with_token(request.into_inner().into(), token),
            None => RequestWithToken::new(request.into_inner().into()),
        };
        let propose_id = self.generate_propose_id();
        let cmd = Self::command_from_request_wrapper(propose_id, wrapper);
        if use_fast_path {
            let cmd_res = self.client.propose(cmd).await.map_err(|err| {
                if let ProposeError::ExecutionError(e) = err {
                    tonic::Status::invalid_argument(e)
                } else {
                    panic!("propose err {err:?}")
                }
            })?;
            Ok((cmd_res, None))
        } else {
            let (cmd_res, sync_res) = self.client.propose_indexed(cmd).await.map_err(|err| {
                if let ProposeError::ExecutionError(e) = err {
                    tonic::Status::invalid_argument(e)
                } else {
                    panic!("propose err {err:?}")
                }
            })?;
            Ok((cmd_res, Some(sync_res)))
        }
    }

    /// Hash password
    fn hash_password(password: &[u8]) -> String {
        let salt = SaltString::generate(&mut OsRng);
        let hashed_password = Pbkdf2
            .hash_password(password, salt.as_ref())
            .unwrap_or_else(|e| panic!("Failed to hash password: {}", e));
        hashed_password.to_string()
    }

    /// Check password in storage
    pub(crate) fn check_password(
        &self,
        username: &str,
        password: &str,
    ) -> Result<i64, tonic::Status> {
        self.storage
            .check_password(username, password)
            .map_err(|e| tonic::Status::invalid_argument(format!("Auth failed, error: {e}")))
    }
}

#[tonic::async_trait]
impl Auth for AuthServer {
    async fn auth_enable(
        &self,
        request: tonic::Request<AuthEnableRequest>,
    ) -> Result<tonic::Response<AuthEnableResponse>, tonic::Status> {
        debug!("Receive AuthEnableRequest {:?}", request);

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthEnableResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn auth_disable(
        &self,
        request: tonic::Request<AuthDisableRequest>,
    ) -> Result<tonic::Response<AuthDisableResponse>, tonic::Status> {
        debug!("Receive AuthDisableRequest {:?}", request);

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthDisableResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn auth_status(
        &self,
        request: tonic::Request<AuthStatusRequest>,
    ) -> Result<tonic::Response<AuthStatusResponse>, tonic::Status> {
        debug!("Receive AuthStatusRequest {:?}", request);

        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: AuthStatusResponse = cmd_res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn authenticate(
        &self,
        request: tonic::Request<AuthenticateRequest>,
    ) -> Result<tonic::Response<AuthenticateResponse>, tonic::Status> {
        debug!("Receive AuthenticateRequest {:?}", request);
        loop {
            let checked_revision =
                self.check_password(&request.get_ref().name, &request.get_ref().password)?;
            let mut authenticate_req = request.get_ref().clone();
            authenticate_req.password = "".to_owned();

            let (res, sync_res) = self
                .propose(tonic::Request::new(authenticate_req), false)
                .await?;

            if let Some(sync_res) = sync_res {
                let revision = sync_res.revision();
                debug!("Get revision {:?} for AuthDisableResponse", revision);
                if revision == checked_revision {
                    let mut res: AuthenticateResponse = res.decode().into();
                    if let Some(mut header) = res.header.as_mut() {
                        header.revision = revision;
                    }
                    return Ok(tonic::Response::new(res));
                }
            }
        }
    }

    async fn user_add(
        &self,
        mut request: tonic::Request<AuthUserAddRequest>,
    ) -> Result<tonic::Response<AuthUserAddResponse>, tonic::Status> {
        debug!("Receive AuthUserAddRequest {:?}", request);
        let user_add_req = request.get_mut();
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

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthUserAddResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn user_get(
        &self,
        request: tonic::Request<AuthUserGetRequest>,
    ) -> Result<tonic::Response<AuthUserGetResponse>, tonic::Status> {
        debug!("Receive AuthUserGetRequest {:?}", request);

        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: AuthUserGetResponse = cmd_res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn user_list(
        &self,
        request: tonic::Request<AuthUserListRequest>,
    ) -> Result<tonic::Response<AuthUserListResponse>, tonic::Status> {
        debug!("Receive AuthUserListRequest {:?}", request);

        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: AuthUserListResponse = cmd_res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn user_delete(
        &self,
        request: tonic::Request<AuthUserDeleteRequest>,
    ) -> Result<tonic::Response<AuthUserDeleteResponse>, tonic::Status> {
        debug!("Receive AuthUserDeleteRequest {:?}", request);

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthUserDeleteResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn user_change_password(
        &self,
        mut request: tonic::Request<AuthUserChangePasswordRequest>,
    ) -> Result<tonic::Response<AuthUserChangePasswordResponse>, tonic::Status> {
        debug!("Receive AuthUserChangePasswordRequest {:?}", request);
        let mut user_change_password_req = request.get_mut();
        let hashed_password = Self::hash_password(user_change_password_req.password.as_bytes());
        user_change_password_req.hashed_password = hashed_password;
        user_change_password_req.password = "".to_owned();

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthUserChangePasswordResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn user_grant_role(
        &self,
        request: tonic::Request<AuthUserGrantRoleRequest>,
    ) -> Result<tonic::Response<AuthUserGrantRoleResponse>, tonic::Status> {
        debug!("Receive AuthUserGrantRoleRequest {:?}", request);

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthUserGrantRoleResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn user_revoke_role(
        &self,
        request: tonic::Request<AuthUserRevokeRoleRequest>,
    ) -> Result<tonic::Response<AuthUserRevokeRoleResponse>, tonic::Status> {
        debug!("Receive AuthUserRevokeRoleRequest {:?}", request);

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthUserRevokeRoleResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn role_add(
        &self,
        request: tonic::Request<AuthRoleAddRequest>,
    ) -> Result<tonic::Response<AuthRoleAddResponse>, tonic::Status> {
        debug!("Receive AuthRoleAddRequest {:?}", request);
        if request.get_ref().name.is_empty() {
            return Err(tonic::Status::invalid_argument("Role name is empty"));
        }

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthRoleAddResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn role_get(
        &self,
        request: tonic::Request<AuthRoleGetRequest>,
    ) -> Result<tonic::Response<AuthRoleGetResponse>, tonic::Status> {
        debug!("Receive AuthRoleGetRequest {:?}", request);

        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: AuthRoleGetResponse = cmd_res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn role_list(
        &self,
        request: tonic::Request<AuthRoleListRequest>,
    ) -> Result<tonic::Response<AuthRoleListResponse>, tonic::Status> {
        debug!("Receive AuthRoleListRequest {:?}", request);

        let is_fast_path = true;
        let (cmd_res, sync_res) = self.propose(request, is_fast_path).await?;

        let mut res: AuthRoleListResponse = cmd_res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn role_delete(
        &self,
        request: tonic::Request<AuthRoleDeleteRequest>,
    ) -> Result<tonic::Response<AuthRoleDeleteResponse>, tonic::Status> {
        debug!("Receive AuthRoleDeleteRequest {:?}", request);

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthRoleDeleteResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn role_grant_permission(
        &self,
        request: tonic::Request<AuthRoleGrantPermissionRequest>,
    ) -> Result<tonic::Response<AuthRoleGrantPermissionResponse>, tonic::Status> {
        debug!("Receive AuthRoleGrantPermissionRequest {:?}", request);
        if request.get_ref().perm.is_none() {
            return Err(tonic::Status::invalid_argument("Permission not given"));
        }

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthRoleGrantPermissionResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<AuthRoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<AuthRoleRevokePermissionResponse>, tonic::Status> {
        debug!("Receive AuthRoleRevokePermissionRequest {:?}", request);

        let (res, sync_res) = self.propose(request, false).await?;

        let mut res: AuthRoleRevokePermissionResponse = res.decode().into();
        update_revision!(res, sync_res);
        Ok(tonic::Response::new(res))
    }
}
