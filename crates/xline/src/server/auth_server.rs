use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tracing::debug;
use utils::hash_password;
use xlineapi::{
    command::{Command, CommandResponse, CurpClient, SyncResponse},
    request_validation::RequestValidator,
};

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
        AuthenticateResponse, RequestWrapper, ResponseWrapper,
    },
    storage::{storage_api::StorageApi, AuthStore},
};

/// Auth Server
pub(crate) struct AuthServer<S>
where
    S: StorageApi,
{
    /// Consensus client
    client: Arc<CurpClient>,
    /// Auth Store
    auth_store: Arc<AuthStore<S>>,
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
    pub(crate) fn new(client: Arc<CurpClient>, auth_store: Arc<AuthStore<S>>) -> Self {
        Self { client, auth_store }
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
        let auth_info = self.auth_store.try_get_auth_info_from_request(&request)?;
        let request = request.into_inner().into();
        let cmd = Command::new_with_auth_info(request.keys(), request, auth_info);
        let res = self.client.propose(&cmd, None, use_fast_path).await??;
        Ok(res)
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
        let mut res_wrapper = cmd_res.into_inner();
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
        self.handle_req(request, false).await
    }

    async fn user_add(
        &self,
        mut request: tonic::Request<AuthUserAddRequest>,
    ) -> Result<tonic::Response<AuthUserAddResponse>, tonic::Status> {
        let user_add_req = request.get_mut();
        debug!("Receive AuthUserAddRequest {}", user_add_req);
        user_add_req.validation()?;
        let hashed_password = hash_password(user_add_req.password.as_bytes())
            .map_err(|err| tonic::Status::internal(format!("Failed to hash password: {err}")))?;
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
        let user_change_password_req = request.get_mut();
        let hashed_password = hash_password(user_change_password_req.password.as_bytes())
            .map_err(|err| tonic::Status::internal(format!("Failed to hash password: {err}")))?;
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
        request.get_ref().validation()?;
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
        debug!(
            "Receive AuthRoleGrantPermissionRequest {}",
            request.get_ref()
        );
        request.get_ref().validation()?;
        self.handle_req(request, false).await
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<AuthRoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<AuthRoleRevokePermissionResponse>, tonic::Status> {
        debug!(
            "Receive AuthRoleRevokePermissionRequest {}",
            request.get_ref()
        );
        self.handle_req(request, false).await
    }
}
