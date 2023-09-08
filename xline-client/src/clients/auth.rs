use std::sync::Arc;

use curp::{client::Client as CurpClient, cmd::generate_propose_id};
use pbkdf2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Pbkdf2,
};
use tonic::transport::Channel;
use xline::server::Command;
use xlineapi::{
    AuthDisableResponse, AuthEnableResponse, AuthRoleAddResponse, AuthRoleDeleteResponse,
    AuthRoleGetResponse, AuthRoleGrantPermissionResponse, AuthRoleListResponse,
    AuthRoleRevokePermissionResponse, AuthStatusResponse, AuthUserAddResponse,
    AuthUserChangePasswordResponse, AuthUserDeleteResponse, AuthUserGetResponse,
    AuthUserGrantRoleResponse, AuthUserListResponse, AuthUserRevokeRoleResponse,
    AuthenticateResponse, RequestWithToken, RequestWrapper, ResponseWrapper,
};

use crate::{
    error::{ClientError, Result},
    types::auth::{
        AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGetRequest,
        AuthRoleGrantPermissionRequest, AuthRoleRevokePermissionRequest, AuthUserAddRequest,
        AuthUserChangePasswordRequest, AuthUserDeleteRequest, AuthUserGetRequest,
        AuthUserGrantRoleRequest, AuthUserRevokeRoleRequest, AuthenticateRequest,
    },
    AuthService,
};

/// Client for Auth operations.
#[derive(Clone, Debug)]
pub struct AuthClient {
    /// Name of the AuthClient, which will be used in CURP propose id generation
    name: String,
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient<Command>>,
    /// The auth RPC client, only communicate with one server at a time
    auth_client: xlineapi::AuthClient<AuthService<Channel>>,
    /// The auth token
    token: Option<String>,
}

impl AuthClient {
    /// Creates a new `AuthClient`
    #[inline]
    pub fn new(
        name: String,
        curp_client: Arc<CurpClient<Command>>,
        channel: Channel,
        token: Option<String>,
    ) -> Self {
        Self {
            name,
            curp_client,
            auth_client: xlineapi::AuthClient::new(AuthService::new(
                channel,
                token.as_ref().and_then(|t| t.parse().ok().map(Arc::new)),
            )),
            token,
        }
    }

    /// Enables authentication.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let _resp = client.auth_enable().await?;
    ///
    ///     // auth with some user
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn auth_enable(&self) -> Result<AuthEnableResponse> {
        self.handle_req(xlineapi::AuthEnableRequest {}, false).await
    }

    /// Disables authentication.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let _resp = client.auth_enable().await?;
    ///
    ///     // auth with some user
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn auth_disable(&self) -> Result<AuthDisableResponse> {
        self.handle_req(xlineapi::AuthDisableRequest {}, false)
            .await
    }

    /// Gets authentication status.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.auth_status().await?;
    ///     println!("auth status:");
    ///     println!(
    ///         "enabled: {}, revision: {}",
    ///         resp.enabled, resp.auth_revision
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn auth_status(&self) -> Result<AuthStatusResponse> {
        self.handle_req(xlineapi::AuthStatusRequest {}, true).await
    }

    /// Process an authentication request, and return the auth token
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner RPC client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, types::auth::AuthenticateRequest, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let mut client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client
    ///         .authenticate(AuthenticateRequest::new("root", "rootpw"))
    ///         .await?;
    ///
    ///     println!("auth token: {}", resp.token);
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn authenticate(
        &mut self,
        request: AuthenticateRequest,
    ) -> Result<AuthenticateResponse> {
        Ok(self
            .auth_client
            .authenticate(xlineapi::AuthenticateRequest::from(request))
            .await?
            .into_inner())
    }

    /// Add an user.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, types::auth::AuthUserAddRequest, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     client.user_add(AuthUserAddRequest::new("user1")).await?;
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_add(&self, mut request: AuthUserAddRequest) -> Result<AuthUserAddResponse> {
        if request.inner.name.is_empty() {
            return Err(ClientError::InvalidArgs(String::from("user name is empty")));
        }
        let need_password = request
            .inner
            .options
            .as_ref()
            .map_or(true, |o| !o.no_password);
        if need_password && request.inner.password.is_empty() {
            return Err(ClientError::InvalidArgs(String::from(
                "password is required but not provided",
            )));
        }
        let hashed_password = Self::hash_password(request.inner.password.as_bytes());
        request.inner.hashed_password = hashed_password;
        request.inner.password = String::new();
        self.handle_req(request.inner, false).await
    }

    /// Gets the user info by the user name.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, types::auth::AuthUserGetRequest, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.user_get(AuthUserGetRequest::new("user")).await?;
    ///
    ///     for role in resp.roles {
    ///         print!("{} ", role);
    ///     }
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_get(&self, request: AuthUserGetRequest) -> Result<AuthUserGetResponse> {
        self.handle_req(request.inner, true).await
    }

    /// Lists all users.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.user_list().await?;
    ///
    ///     for user in resp.users {
    ///         println!("user: {}", user);
    ///     }
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_list(&self) -> Result<AuthUserListResponse> {
        self.handle_req(xlineapi::AuthUserListRequest {}, true)
            .await
    }

    /// Deletes the given key from the key-value store.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     // add the user
    ///
    ///     let resp = client.user_list().await?;
    ///
    ///     for user in resp.users {
    ///         println!("user: {}", user);
    ///     }
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_delete(
        &self,
        request: AuthUserDeleteRequest,
    ) -> Result<AuthUserDeleteResponse> {
        self.handle_req(request.inner, false).await
    }

    /// Change password for an user.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{
    ///     error::Result, types::auth::AuthUserChangePasswordRequest, Client, ClientOptions,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     // add the user
    ///
    ///     client
    ///         .user_change_password(AuthUserChangePasswordRequest::new("user", "123"))
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_change_password(
        &self,
        mut request: AuthUserChangePasswordRequest,
    ) -> Result<AuthUserChangePasswordResponse> {
        if request.inner.password.is_empty() {
            return Err(ClientError::InvalidArgs(String::from("role name is empty")));
        }
        let hashed_password = Self::hash_password(request.inner.password.as_bytes());
        request.inner.hashed_password = hashed_password;
        request.inner.password = String::new();
        self.handle_req(request.inner, false).await
    }

    /// Grant role for an user.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, types::auth::AuthUserGrantRoleRequest, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     // add user and role
    ///
    ///     client
    ///         .user_grant_role(AuthUserGrantRoleRequest::new("user", "role"))
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_grant_role(
        &self,
        request: AuthUserGrantRoleRequest,
    ) -> Result<AuthUserGrantRoleResponse> {
        self.handle_req(request.inner, false).await
    }

    /// Revoke role for an user.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, types::auth::AuthUserRevokeRoleRequest, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     // grant role
    ///
    ///     client
    ///         .user_revoke_role(AuthUserRevokeRoleRequest::new("user", "role"))
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_revoke_role(
        &self,
        request: AuthUserRevokeRoleRequest,
    ) -> Result<AuthUserRevokeRoleResponse> {
        self.handle_req(request.inner, false).await
    }

    /// Adds role.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::types::auth::AuthRoleAddRequest;
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     client.role_add(AuthRoleAddRequest::new("role")).await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_add(&self, request: AuthRoleAddRequest) -> Result<AuthRoleAddResponse> {
        if request.inner.name.is_empty() {
            return Err(ClientError::InvalidArgs(String::from("role name is empty")));
        }
        self.handle_req(request.inner, false).await
    }

    /// Gets role.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::types::auth::AuthRoleGetRequest;
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.role_get(AuthRoleGetRequest::new("role")).await?;
    ///
    ///     println!("permmisions:");
    ///     for perm in resp.perm {
    ///         println!("{} {}", perm.perm_type, String::from_utf8_lossy(&perm.key));
    ///     }
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_get(&self, request: AuthRoleGetRequest) -> Result<AuthRoleGetResponse> {
        self.handle_req(request.inner, true).await
    }

    /// Lists role.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.role_list().await?;
    ///
    ///     println!("roles:");
    ///     for role in resp.roles {
    ///         println!("{}", role);
    ///     }
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_list(&self) -> Result<AuthRoleListResponse> {
        self.handle_req(xlineapi::AuthRoleListRequest {}, true)
            .await
    }

    /// Deletes role.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{error::Result, types::auth::AuthRoleDeleteRequest, Client, ClientOptions};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     // add the role
    ///
    ///     client
    ///         .role_delete(AuthRoleDeleteRequest::new("role"))
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_delete(
        &self,
        request: AuthRoleDeleteRequest,
    ) -> Result<AuthRoleDeleteResponse> {
        self.handle_req(request.inner, false).await
    }

    /// Grants role permission.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{
    ///     error::Result,
    ///     types::auth::{AuthRoleGrantPermissionRequest, Permission, PermissionType},
    ///     Client, ClientOptions,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     // add the role and key
    ///
    ///     client
    ///         .role_grant_permission(AuthRoleGrantPermissionRequest::new(
    ///             "role",
    ///             Permission::new(PermissionType::Read, "key"),
    ///         ))
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_grant_permission(
        &self,
        request: AuthRoleGrantPermissionRequest,
    ) -> Result<AuthRoleGrantPermissionResponse> {
        if request.inner.perm.is_none() {
            return Err(ClientError::InvalidArgs(String::from(
                "Permission not given",
            )));
        }
        self.handle_req(request.inner, false).await
    }

    /// Revokes role permission.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{
    ///     error::Result, types::auth::AuthRoleRevokePermissionRequest, Client, ClientOptions,
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     // grant the role
    ///
    ///     client
    ///         .role_revoke_permission(AuthRoleRevokePermissionRequest::new("role", "key"))
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_revoke_permission(
        &self,
        request: AuthRoleRevokePermissionRequest,
    ) -> Result<AuthRoleRevokePermissionResponse> {
        self.handle_req(request.inner, false).await
    }

    /// Send request using fast path
    async fn handle_req<Req: Into<RequestWrapper>, Res: From<ResponseWrapper>>(
        &self,
        request: Req,
        use_fast_path: bool,
    ) -> Result<Res> {
        let propose_id = generate_propose_id(&self.name);
        let request = RequestWithToken::new_with_token(request.into(), self.token.clone());
        let cmd = Command::new(vec![], request, propose_id);

        let res_wrapper = if use_fast_path {
            let (cmd_res, _sync_error) = self.curp_client.propose(cmd, true).await?;
            cmd_res.into_inner()
        } else {
            let (cmd_res, Some(sync_res)) = self.curp_client.propose(cmd, false).await? else {
                unreachable!("sync_res is always Some when use_fast_path is false");
            };
            let mut res_wrapper = cmd_res.into_inner();
            res_wrapper.update_revision(sync_res.revision());
            res_wrapper
        };

        Ok(res_wrapper.into())
    }

    /// Generate hash of the password
    fn hash_password(password: &[u8]) -> String {
        let salt = SaltString::generate(&mut OsRng);
        #[allow(clippy::panic)] // This doesn't seems to be fallible
        let hashed_password = Pbkdf2
            .hash_password(password, salt.as_ref())
            .unwrap_or_else(|e| panic!("Failed to hash password: {e}"));
        hashed_password.to_string()
    }
}
