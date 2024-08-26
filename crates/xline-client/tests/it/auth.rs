//! The following tests are originally from `etcd-client`
use xline_client::{
    error::Result,
    types::{
        auth::{Permission, PermissionType},
        range_end::RangeOption,
    },
};

use super::common::get_cluster_client;

#[tokio::test(flavor = "multi_thread")]
async fn role_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();
    let role1 = "role1";
    let role2 = "role2";

    client.role_add(role1).await?;
    client.role_add(role2).await?;

    client.role_get(role1).await?;
    client.role_get(role2).await?;

    let role_list_resp = client.role_list().await?;
    assert_eq!(
        role_list_resp.roles,
        vec![role1.to_owned(), role2.to_owned()]
    );

    client.role_delete(role1).await?;
    client.role_delete(role2).await?;

    client.role_get(role1).await.unwrap_err();
    client.role_get(role2).await.unwrap_err();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn permission_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();

    let role1 = "role1";
    let perm1 = (PermissionType::Read, "123", None);
    let perm2 = (PermissionType::Write, "abc", Some(RangeOption::FromKey));
    let perm3 = (
        PermissionType::Readwrite,
        "hi",
        Some(RangeOption::RangeEnd("hjj".into())),
    );
    let perm4 = (PermissionType::Write, "pp", Some(RangeOption::Prefix));
    let perm5 = (PermissionType::Read, vec![0], Some(RangeOption::FromKey));

    client.role_add(role1).await?;

    let (p1, p2, p3) = perm1.clone();
    client.role_grant_permission(role1, p1, p2, p3).await?;
    let (p1, p2, p3) = perm2.clone();
    client.role_grant_permission(role1, p1, p2, p3).await?;
    let (p1, p2, p3) = perm3.clone();
    client.role_grant_permission(role1, p1, p2, p3).await?;
    let (p1, p2, p3) = perm4.clone();
    client.role_grant_permission(role1, p1, p2, p3).await?;
    let (p1, p2, p3) = perm5.clone();
    client.role_grant_permission(role1, p1, p2, p3).await?;

    {
        // get permissions for role1, and validate the result
        let resp = client.role_get(role1).await?;
        let permissions = resp.perm;

        assert!(permissions.contains(&Permission::from(perm1).into()));
        assert!(permissions.contains(&Permission::from(perm2).into()));
        assert!(permissions.contains(&Permission::from(perm3).into()));
        assert!(permissions.contains(&Permission::from(perm4).into()));
        assert!(permissions.contains(&Permission::from(perm5).into()));
    }

    // revoke all permission
    client.role_revoke_permission(role1, "123", None).await?;
    client
        .role_revoke_permission(role1, "abc", Some(RangeOption::FromKey))
        .await?;
    client
        .role_revoke_permission(role1, "hi", Some(RangeOption::RangeEnd("hjj".into())))
        .await?;
    client
        .role_revoke_permission(role1, "pp", Some(RangeOption::Prefix))
        .await?;
    client
        .role_revoke_permission(role1, vec![0], Some(RangeOption::FromKey))
        .await?;

    let role_get_resp = client.role_get(role1).await?;
    assert!(role_get_resp.perm.is_empty());

    client.role_delete(role1).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn user_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();

    let name1 = "usr1";
    let password1 = "pwd1";
    let password2 = "pwd2";

    client.user_add(name1, password1, false).await?;
    client.user_get(name1).await?;

    let user_list_resp = client.user_list().await?;
    assert!(user_list_resp.users.contains(&name1.to_string()));

    client.user_change_password(name1, password2).await?;

    client.user_delete(name1).await?;
    client.user_get(name1).await.unwrap_err();

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn user_role_operations_should_success_in_normal_path() -> Result<()> {
    let (_cluster, client) = get_cluster_client().await.unwrap();
    let client = client.auth_client();

    let name1 = "usr1";
    let role1 = "role1";
    let role2 = "role2";

    client.user_add(name1, "", true).await?;
    client.role_add(role1).await?;
    client.role_add(role2).await?;

    client.user_grant_role(name1, role1).await?;
    client.user_grant_role(name1, role2).await?;

    let user_get_resp = client.user_get(name1).await?;
    assert_eq!(
        user_get_resp.roles,
        vec![role1.to_owned(), role2.to_owned()]
    );

    client.user_revoke_role(name1, role1).await?;
    client.user_revoke_role(name1, role2).await?;

    Ok(())
}
