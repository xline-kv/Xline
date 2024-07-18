use std::{fmt::Debug, sync::Arc};

use tonic::transport::Channel;
use utils::hash_password;
use xlineapi::{
    command::Command, AuthDisableResponse, AuthEnableResponse, AuthRoleAddResponse,
    AuthRoleDeleteResponse, AuthRoleGetResponse, AuthRoleGrantPermissionResponse,
    AuthRoleListResponse, AuthRoleRevokePermissionResponse, AuthStatusResponse,
    AuthUserAddResponse, AuthUserChangePasswordResponse, AuthUserDeleteResponse,
    AuthUserGetResponse, AuthUserGrantRoleResponse, AuthUserListResponse,
    AuthUserRevokeRoleResponse, AuthenticateResponse, RequestWrapper, ResponseWrapper,
    Type as PermissionType,
};

use crate::{
    error::{Result, XlineClientError},
    types::{auth::Permission, range_end::RangeOption},
    AuthService, CurpClient,
};

/// Client for Auth operations.
#[derive(Clone)]
pub struct AuthClient {
    /// The client running the CURP protocol, communicate with all servers.
    curp_client: Arc<CurpClient>,
    /// The auth RPC client, only communicate with one server at a time
    #[cfg(not(madsim))]
    auth_client: xlineapi::AuthClient<AuthService<Channel>>,
    /// The auth RPC client, only communicate with one server at a time
    #[cfg(madsim)]
    auth_client: xlineapi::AuthClient<Channel>,
    /// The auth token
    token: Option<String>,
}

impl Debug for AuthClient {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthClient")
            .field("auth_client", &self.auth_client)
            .field("token", &self.token)
            .finish()
    }
}

impl AuthClient {
    /// Creates a new `AuthClient`
    #[inline]
    pub fn new(curp_client: Arc<CurpClient>, channel: Channel, token: Option<String>) -> Self {
        Self {
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    ///         .authenticate("root", "root pass word")
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
        name: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<AuthenticateResponse> {
        Ok(self
            .auth_client
            .authenticate(xlineapi::AuthenticateRequest {
                name: name.into(),
                password: password.into(),
            })
            .await?
            .into_inner())
    }

    /// Add an user.
    /// Set password to empty String if you want to create a user without password.
    ///
    /// # Errors
    ///
    /// This function will return an error if the inner CURP client encountered a propose failure;
    ///
    /// Returns `XlineClientError::InvalidArgs` if the user name is empty,
    /// or the password is empty when `allow_no_password` is false.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     client.user_add("user1", "", true).await?;
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_add(
        &self,
        name: impl Into<String>,
        password: impl AsRef<str>,
        allow_no_password: bool,
    ) -> Result<AuthUserAddResponse> {
        let name = name.into();
        let password: &str = password.as_ref();
        if name.is_empty() {
            return Err(XlineClientError::InvalidArgs(String::from(
                "user name is empty",
            )));
        }
        if !allow_no_password && password.is_empty() {
            return Err(XlineClientError::InvalidArgs(String::from(
                "password is required but not provided",
            )));
        }
        let hashed_password = hash_password(password.as_bytes()).map_err(|err| {
            XlineClientError::InternalError(format!("Failed to hash password: {err}"))
        })?;
        let options = allow_no_password.then_some(xlineapi::UserAddOptions { no_password: true });
        self.handle_req(
            xlineapi::AuthUserAddRequest {
                name,
                password: String::new(),
                hashed_password,
                options,
            },
            false,
        )
        .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.user_get("user").await?;
    ///
    ///     for role in resp.roles {
    ///         print!("{} ", role);
    ///     }
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_get(&self, name: impl Into<String>) -> Result<AuthUserGetResponse> {
        self.handle_req(xlineapi::AuthUserGetRequest { name: name.into() }, true)
            .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.user_delete("user").await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_delete(&self, name: impl Into<String>) -> Result<AuthUserDeleteResponse> {
        self.handle_req(xlineapi::AuthUserDeleteRequest { name: name.into() }, false)
            .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    ///         .user_change_password("user", "123")
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_change_password(
        &self,
        name: impl Into<String>,
        password: impl AsRef<str>,
    ) -> Result<AuthUserChangePasswordResponse> {
        let password: &str = password.as_ref();
        if password.is_empty() {
            return Err(XlineClientError::InvalidArgs(String::from(
                "role name is empty",
            )));
        }
        let hashed_password = hash_password(password.as_bytes()).map_err(|err| {
            XlineClientError::InternalError(format!("Failed to hash password: {err}"))
        })?;
        self.handle_req(
            xlineapi::AuthUserChangePasswordRequest {
                name: name.into(),
                hashed_password,
                password: String::new(),
            },
            false,
        )
        .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    ///     client.user_grant_role("user", "role").await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_grant_role(
        &self,
        name: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<AuthUserGrantRoleResponse> {
        self.handle_req(
            xlineapi::AuthUserGrantRoleRequest {
                user: name.into(),
                role: role.into(),
            },
            false,
        )
        .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    ///     client.user_revoke_role("user", "role").await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn user_revoke_role(
        &self,
        name: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<AuthUserRevokeRoleResponse> {
        self.handle_req(
            xlineapi::AuthUserRevokeRoleRequest {
                name: name.into(),
                role: role.into(),
            },
            false,
        )
        .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     client.role_add("role").await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_add(&self, name: impl Into<String>) -> Result<AuthRoleAddResponse> {
        let name = name.into();
        if name.is_empty() {
            return Err(XlineClientError::InvalidArgs(String::from(
                "role name is empty",
            )));
        }
        self.handle_req(xlineapi::AuthRoleAddRequest { name }, false)
            .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];
    ///
    ///     let client = Client::connect(curp_members, ClientOptions::default())
    ///         .await?
    ///         .auth_client();
    ///
    ///     let resp = client.role_get("role").await?;
    ///
    ///     println!("permissions:");
    ///     for perm in resp.perm {
    ///         println!("{} {}", perm.perm_type, String::from_utf8_lossy(&perm.key));
    ///     }
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_get(&self, name: impl Into<String>) -> Result<AuthRoleGetResponse> {
        self.handle_req(xlineapi::AuthRoleGetRequest { role: name.into() }, true)
            .await
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    /// use xline_client::{Client, ClientOptions};
    /// use anyhow::Result;
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
    ///         .role_delete("role")
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_delete(&self, name: impl Into<String>) -> Result<AuthRoleDeleteResponse> {
        self.handle_req(xlineapi::AuthRoleDeleteRequest { role: name.into() }, false)
            .await
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
    ///     types::auth::{Permission, PermissionType},
    ///     Client, ClientOptions,
    /// };
    /// use anyhow::Result;
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
    ///         .role_grant_permission(
    ///             "role",
    ///             PermissionType::Read,
    ///             "key",
    ///             None
    ///         )
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_grant_permission(
        &self,
        name: impl Into<String>,
        perm_type: PermissionType,
        perm_key: impl Into<Vec<u8>>,
        range_option: Option<RangeOption>,
    ) -> Result<AuthRoleGrantPermissionResponse> {
        self.handle_req(
            xlineapi::AuthRoleGrantPermissionRequest {
                name: name.into(),
                perm: Some(Permission::new(perm_type, perm_key.into(), range_option).into()),
            },
            false,
        )
        .await
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
    /// use xline_client::{Client, ClientOptions, types::range_end::RangeOption};
    /// use anyhow::Result;
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
    ///     client.role_revoke_permission("role", "key", None).await?;
    ///     client
    ///         .role_revoke_permission(
    ///             "role2",
    ///             "hi",
    ///             Some(RangeOption::RangeEnd("hjj".into())),
    ///         )
    ///         .await?;
    ///
    ///     Ok(())
    /// }
    ///```
    #[inline]
    pub async fn role_revoke_permission(
        &self,
        name: impl Into<String>,
        key: impl Into<Vec<u8>>,
        range_option: Option<RangeOption>,
    ) -> Result<AuthRoleRevokePermissionResponse> {
        let mut key = key.into();
        let range_end = range_option.unwrap_or_default().get_range_end(&mut key);
        self.handle_req(
            xlineapi::AuthRoleRevokePermissionRequest {
                role: name.into(),
                key,
                range_end,
            },
            false,
        )
        .await
    }

    /// Send request using fast path
    async fn handle_req<Req: Into<RequestWrapper>, Res: From<ResponseWrapper>>(
        &self,
        request: Req,
        use_fast_path: bool,
    ) -> Result<Res> {
        let request = request.into();
        let cmd = Command::new(request);

        let res_wrapper = if use_fast_path {
            let (cmd_res, _sync_error) = self
                .curp_client
                .propose(&cmd, self.token.as_ref(), true)
                .await??;
            cmd_res.into_inner()
        } else {
            let (cmd_res, Some(sync_res)) = self
                .curp_client
                .propose(&cmd, self.token.as_ref(), false)
                .await??
            else {
                unreachable!("sync_res is always Some when use_fast_path is false");
            };
            let mut res_wrapper = cmd_res.into_inner();
            res_wrapper.update_revision(sync_res.revision());
            res_wrapper
        };

        Ok(res_wrapper.into())
    }
}
