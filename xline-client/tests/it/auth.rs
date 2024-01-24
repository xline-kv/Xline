//! The following tests are originally from `etcd-client`
use xline_client::{
    error::Result,
    types::auth::{
        AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGetRequest,
        AuthRoleGrantPermissionRequest, AuthRoleRevokePermissionRequest, AuthUserAddRequest,
        AuthUserChangePasswordRequest, AuthUserDeleteRequest, AuthUserGetRequest,
        AuthUserGrantRoleRequest, AuthUserRevokeRoleRequest, Permission, PermissionType,
    },
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
async fn role_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();
    let role1 = "role1";
    let role2 = "role2";

    client.role_add(AuthRoleAddRequest::new(role1)).await?;
    client.role_add(AuthRoleAddRequest::new(role2)).await?;

    client.role_get(AuthRoleGetRequest::new(role1)).await?;
    client.role_get(AuthRoleGetRequest::new(role2)).await?;

    let role_list_resp = client.role_list().await?;
    assert_eq!(
        role_list_resp.roles,
        vec![role1.to_owned(), role2.to_owned()]
    );

    client
        .role_delete(AuthRoleDeleteRequest::new(role1))
        .await?;
    client
        .role_delete(AuthRoleDeleteRequest::new(role2))
        .await?;

    client
        .role_get(AuthRoleGetRequest::new(role1))
        .await
        .unwrap_err();
    client
        .role_get(AuthRoleGetRequest::new(role2))
        .await
        .unwrap_err();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn permission_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();

    let role1 = "role1";
    let perm1 = Permission::new(PermissionType::Read, "123");
    let perm2 = Permission::new(PermissionType::Write, "abc").with_from_key();
    let perm3 = Permission::new(PermissionType::Readwrite, "hi").with_range_end("hjj");
    let perm4 = Permission::new(PermissionType::Write, "pp").with_prefix();
    let perm5 = Permission::new(PermissionType::Read, vec![0]).with_from_key();

    client.role_add(AuthRoleAddRequest::new(role1)).await?;

    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role1, perm1.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role1, perm2.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role1, perm3.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role1, perm4.clone()))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(role1, perm5.clone()))
        .await?;

    {
        let resp = client.role_get(AuthRoleGetRequest::new(role1)).await?;
        let permissions = resp.perm;
        assert!(permissions.contains(&perm1.into()));
        assert!(permissions.contains(&perm2.into()));
        assert!(permissions.contains(&perm3.into()));
        assert!(permissions.contains(&perm4.into()));
        assert!(permissions.contains(&perm5.into()));
    }

    // revoke all permission
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new(role1, "123"))
        .await?;
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new(role1, "abc").with_from_key())
        .await?;
    client
        .role_revoke_permission(
            AuthRoleRevokePermissionRequest::new(role1, "hi").with_range_end("hjj"),
        )
        .await?;
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new(role1, "pp").with_prefix())
        .await?;
    client
        .role_revoke_permission(
            AuthRoleRevokePermissionRequest::new(role1, vec![0]).with_from_key(),
        )
        .await?;

    let role_get_resp = client.role_get(AuthRoleGetRequest::new(role1)).await?;
    assert!(role_get_resp.perm.is_empty());

    client
        .role_delete(AuthRoleDeleteRequest::new(role1))
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn user_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();

    let name1 = "usr1";
    let password1 = "pwd1";
    let password2 = "pwd2";

    client
        .user_add(AuthUserAddRequest::new(name1).with_pwd(password1))
        .await?;
    client.user_get(AuthUserGetRequest::new(name1)).await?;

    let user_list_resp = client.user_list().await?;
    assert!(user_list_resp.users.contains(&name1.to_string()));

    client
        .user_change_password(AuthUserChangePasswordRequest::new(name1, password2))
        .await?;

    client
        .user_delete(AuthUserDeleteRequest::new(name1))
        .await?;
    client
        .user_get(AuthUserGetRequest::new(name1))
        .await
        .unwrap_err();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn user_role_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();

    let name1 = "usr1";
    let role1 = "role1";
    let role2 = "role2";

    client.user_add(AuthUserAddRequest::new(name1)).await?;
    client.role_add(AuthRoleAddRequest::new(role1)).await?;
    client.role_add(AuthRoleAddRequest::new(role2)).await?;

    client
        .user_grant_role(AuthUserGrantRoleRequest::new(name1, role1))
        .await?;
    client
        .user_grant_role(AuthUserGrantRoleRequest::new(name1, role2))
        .await?;

    let user_get_resp = client.user_get(AuthUserGetRequest::new(name1)).await?;
    assert_eq!(
        user_get_resp.roles,
        vec![role1.to_owned(), role2.to_owned()]
    );

    client
        .user_revoke_role(AuthUserRevokeRoleRequest::new(name1, role1))
        .await?;
    client
        .user_revoke_role(AuthUserRevokeRoleRequest::new(name1, role2))
        .await?;

    Ok(())
}
