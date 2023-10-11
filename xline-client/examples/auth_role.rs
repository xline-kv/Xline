use anyhow::Result;
use xline_client::{
    types::auth::{
        AuthRoleAddRequest, AuthRoleDeleteRequest, AuthRoleGetRequest,
        AuthRoleGrantPermissionRequest, AuthRoleRevokePermissionRequest, Permission,
        PermissionType,
    },
    Client, ClientOptions,
};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .auth_client();

    // add roles
    client.role_add(AuthRoleAddRequest::new("role1")).await?;
    client.role_add(AuthRoleAddRequest::new("role2")).await?;

    // grant permissions to roles
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(
            "role1",
            Permission::new(PermissionType::Read, "key1"),
        ))
        .await?;
    client
        .role_grant_permission(AuthRoleGrantPermissionRequest::new(
            "role2",
            Permission::new(PermissionType::Readwrite, "key2"),
        ))
        .await?;

    // list all roles and their permissions
    let resp = client.role_list().await?;
    println!("roles:");
    for role in resp.roles {
        println!("{}", role);
        let get_resp = client.role_get(AuthRoleGetRequest::new(role)).await?;
        println!("permmisions:");
        for perm in get_resp.perm {
            println!("{} {}", perm.perm_type, String::from_utf8_lossy(&perm.key));
        }
    }

    // revoke permissions from roles
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new("role1", "key1"))
        .await?;
    client
        .role_revoke_permission(AuthRoleRevokePermissionRequest::new("role2", "key2"))
        .await?;

    // delete roles
    client
        .role_delete(AuthRoleDeleteRequest::new("role1"))
        .await?;
    client
        .role_delete(AuthRoleDeleteRequest::new("role2"))
        .await?;

    Ok(())
}
