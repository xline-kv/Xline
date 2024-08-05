use anyhow::Result;
use xline_client::{Client, ClientOptions};

#[tokio::main]
async fn main() -> Result<()> {
    // the name and address of all curp members
    let curp_members = ["10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"];

    let client = Client::connect(curp_members, ClientOptions::default())
        .await?
        .auth_client();

    // add user
    client.user_add("user1", "", true).await?;
    client.user_add("user2", "", true).await?;

    // change user1's password to "123"
    client.user_change_password("user1", "123").await?;

    // grant roles
    client.user_grant_role("user1", "role1").await?;
    client.user_grant_role("user2", "role2").await?;

    // list all users and their roles
    let resp = client.user_list().await?;
    for user in resp.users {
        println!("user: {}", user);
        let get_resp = client.user_get(user).await?;
        println!("roles:");
        for role in get_resp.roles.iter() {
            print!("{} ", role);
        }
        println!();
    }

    // revoke role from user
    client.user_revoke_role("user1", "role1").await?;
    client.user_revoke_role("user2", "role2").await?;

    // delete users
    client.user_delete("user1").await?;
    client.user_delete("user2").await?;

    Ok(())
}
