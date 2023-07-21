use std::sync::Arc;

use curp::{client::Client, cmd::ProposeId};
use pbkdf2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Pbkdf2,
};
use tonic::metadata::MetadataMap;
use tracing::debug;
use uuid::Uuid;

use super::{
    command::{Command, CommandResponse, SyncResponse},
    common::{propose, propose_indexed},
};
use crate::{
    request_validation::RequestValidator,
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
        AuthenticateResponse, RequestWithToken, RequestWrapper, ResponseWrapper,
    },
    storage::{storage_api::StorageApi, AuthStore},
};

/// Auth Server
#[derive(Debug)]
pub(crate) struct AuthServer<S>
where
    S: StorageApi,
{
    /// Auth storage
    storage: Arc<AuthStore<S>>,
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

impl<S> AuthServer<S>
where
    S: StorageApi,
{
    /// New `AuthServer`
    pub(crate) fn new(
        storage: Arc<AuthStore<S>>,
        client: Arc<Client<Command>>,
        name: String,
    ) -> Self {
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
        let token = get_token(request.metadata());
        let wrapper = RequestWithToken::new_with_token(request.into_inner().into(), token);
        let propose_id = self.generate_propose_id();
        let cmd = Self::command_from_request_wrapper(propose_id, wrapper);
        #[allow(clippy::wildcard_enum_match_arm)]
        if use_fast_path {
            let cmd_res = propose(&self.client, cmd).await?;
            Ok((cmd_res, None))
        } else {
            let (cmd_res, sync_res) = propose_indexed(&self.client, cmd).await?;
            Ok((cmd_res, Some(sync_res)))
        }
    }

    /// Hash password
    fn hash_password(password: &[u8]) -> String {
        let salt = SaltString::generate(&mut OsRng);
        let hashed_password = Pbkdf2
            .hash_password(password, salt.as_ref())
            .unwrap_or_else(|e| panic!("Failed to hash password: {e}"));
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

    /// Propose request and make a response
    async fn handle_req<Req, Res>(
        &self,
        request: tonic::Request<Req>,
        use_fast_path: bool,
    ) -> Result<tonic::Response<Res>, tonic::Status>
    where
        Req: Into<RequestWrapper>,
        Res: From<ResponseWrapper>,
    {
        let (cmd_res, sync_res) = self.propose(request, use_fast_path).await?;
        let mut res_wrapper = cmd_res.decode();
        if let Some(sync_res) = sync_res {
            res_wrapper.update_revision(sync_res.revision());
        }
        Ok(tonic::Response::new(res_wrapper.into()))
    }
}

#[tonic::async_trait]
impl<S> Auth for AuthServer<S>
where
    S: StorageApi,
{
    async fn auth_enable(
        &self,
        request: tonic::Request<AuthEnableRequest>,
    ) -> Result<tonic::Response<AuthEnableResponse>, tonic::Status> {
        debug!("Receive AuthEnableRequest {:?}", request);
        self.handle_req(request, false).await
    }

    async fn auth_disable(
        &self,
        request: tonic::Request<AuthDisableRequest>,
    ) -> Result<tonic::Response<AuthDisableResponse>, tonic::Status> {
        debug!("Receive AuthDisableRequest {:?}", request);
        self.handle_req(request, false).await
    }

    async fn auth_status(
        &self,
        request: tonic::Request<AuthStatusRequest>,
    ) -> Result<tonic::Response<AuthStatusResponse>, tonic::Status> {
        debug!("Receive AuthStatusRequest {:?}", request);
        let is_fast_path = true;
        self.handle_req(request, is_fast_path).await
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
            authenticate_req.password = String::new();

            let (res, sync_res) = self
                .propose(tonic::Request::new(authenticate_req), false)
                .await?;

            if checked_revision == self.storage.revision() {
                if let Some(sync_res) = sync_res {
                    let revision = sync_res.revision();
                    debug!("Get revision {:?} for AuthDisableResponse", revision);
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
        user_add_req.validation(())?;
        let hashed_password = Self::hash_password(user_add_req.password.as_bytes());
        user_add_req.hashed_password = hashed_password;
        user_add_req.password = String::new();
        self.handle_req(request, false).await
    }

    async fn user_get(
        &self,
        request: tonic::Request<AuthUserGetRequest>,
    ) -> Result<tonic::Response<AuthUserGetResponse>, tonic::Status> {
        debug!("Receive AuthUserGetRequest {:?}", request);
        let is_fast_path = true;
        self.handle_req(request, is_fast_path).await
    }

    async fn user_list(
        &self,
        request: tonic::Request<AuthUserListRequest>,
    ) -> Result<tonic::Response<AuthUserListResponse>, tonic::Status> {
        debug!("Receive AuthUserListRequest {:?}", request);
        let is_fast_path = true;
        self.handle_req(request, is_fast_path).await
    }

    async fn user_delete(
        &self,
        request: tonic::Request<AuthUserDeleteRequest>,
    ) -> Result<tonic::Response<AuthUserDeleteResponse>, tonic::Status> {
        debug!("Receive AuthUserDeleteRequest {:?}", request);
        self.handle_req(request, false).await
    }

    async fn user_change_password(
        &self,
        mut request: tonic::Request<AuthUserChangePasswordRequest>,
    ) -> Result<tonic::Response<AuthUserChangePasswordResponse>, tonic::Status> {
        debug!("Receive AuthUserChangePasswordRequest {:?}", request);
        let mut user_change_password_req = request.get_mut();
        let hashed_password = Self::hash_password(user_change_password_req.password.as_bytes());
        user_change_password_req.hashed_password = hashed_password;
        user_change_password_req.password = String::new();
        self.handle_req(request, false).await
    }

    async fn user_grant_role(
        &self,
        request: tonic::Request<AuthUserGrantRoleRequest>,
    ) -> Result<tonic::Response<AuthUserGrantRoleResponse>, tonic::Status> {
        debug!("Receive AuthUserGrantRoleRequest {:?}", request);
        self.handle_req(request, false).await
    }

    async fn user_revoke_role(
        &self,
        request: tonic::Request<AuthUserRevokeRoleRequest>,
    ) -> Result<tonic::Response<AuthUserRevokeRoleResponse>, tonic::Status> {
        debug!("Receive AuthUserRevokeRoleRequest {:?}", request);
        self.handle_req(request, false).await
    }

    async fn role_add(
        &self,
        request: tonic::Request<AuthRoleAddRequest>,
    ) -> Result<tonic::Response<AuthRoleAddResponse>, tonic::Status> {
        debug!("Receive AuthRoleAddRequest {:?}", request);
        request.get_ref().validation(())?;
        self.handle_req(request, false).await
    }

    async fn role_get(
        &self,
        request: tonic::Request<AuthRoleGetRequest>,
    ) -> Result<tonic::Response<AuthRoleGetResponse>, tonic::Status> {
        debug!("Receive AuthRoleGetRequest {:?}", request);
        let is_fast_path = true;
        self.handle_req(request, is_fast_path).await
    }

    async fn role_list(
        &self,
        request: tonic::Request<AuthRoleListRequest>,
    ) -> Result<tonic::Response<AuthRoleListResponse>, tonic::Status> {
        debug!("Receive AuthRoleListRequest {:?}", request);
        let is_fast_path = true;
        self.handle_req(request, is_fast_path).await
    }

    async fn role_delete(
        &self,
        request: tonic::Request<AuthRoleDeleteRequest>,
    ) -> Result<tonic::Response<AuthRoleDeleteResponse>, tonic::Status> {
        debug!("Receive AuthRoleDeleteRequest {:?}", request);
        self.handle_req(request, false).await
    }

    async fn role_grant_permission(
        &self,
        request: tonic::Request<AuthRoleGrantPermissionRequest>,
    ) -> Result<tonic::Response<AuthRoleGrantPermissionResponse>, tonic::Status> {
        debug!("Receive AuthRoleGrantPermissionRequest {:?}", request);
        request.get_ref().validation(())?;
        self.handle_req(request, false).await
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<AuthRoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<AuthRoleRevokePermissionResponse>, tonic::Status> {
        debug!("Receive AuthRoleRevokePermissionRequest {:?}", request);
        self.handle_req(request, false).await
    }
}
