mod common;

use std::error::Error;

use etcd_client::{AuthClient, ConnectOptions, GetOptions, Permission, PermissionType};
use xline::client::kv_types::{PutRequest, RangeRequest};

use crate::common::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_auth_empty_user_get() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    enable_auth(&mut auth_client).await?;
    let res = client.range(RangeRequest::new("foo")).await;
    assert!(res.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_auth_empty_user_put() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    enable_auth(&mut auth_client).await?;
    let res = client.put(PutRequest::new("foo", "bar")).await;
    assert!(res.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_auth_token_with_disable() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    enable_auth(&mut auth_client).await?;
    let mut authed_client = etcd_client::Client::connect(
        vec![cluster.addrs()["server0"].to_string()],
        Some(ConnectOptions::new().with_user("root", "123")),
    )
    .await?;
    authed_client.put("foo", "bar", None).await?;
    authed_client.auth_disable().await?;
    authed_client.put("foo", "bar", None).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_auth_revision() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    client.put(PutRequest::new("foo", "bar")).await?;
    let user_add_resp = auth_client.user_add("root", "123", None).await?;
    let auth_rev = user_add_resp.header().unwrap().revision();
    assert_eq!(auth_rev, 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_auth_non_authorized_rpcs() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    let result = client.put(PutRequest::new("foo", "bar")).await;
    assert!(result.is_ok());
    enable_auth(&mut auth_client).await?;
    let result = client.put(PutRequest::new("foo", "bar")).await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_kv_authorization() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    set_user(&mut auth_client, "u1", "123", "r1", b"foo", &[]).await?;
    set_user(&mut auth_client, "u2", "123", "r2", b"foo", b"foy").await?;
    enable_auth(&mut auth_client).await?;

    let mut u1_client = etcd_client::Client::connect(
        vec![cluster.addrs()["server0"].to_string()],
        Some(ConnectOptions::new().with_user("u1", "123")),
    )
    .await?;
    let mut u2_client = etcd_client::Client::connect(
        vec![cluster.addrs()["server0"].to_string()],
        Some(ConnectOptions::new().with_user("u2", "123")),
    )
    .await?;

    let result = u1_client.put("foo", "bar", None).await;
    assert!(result.is_ok());
    let result = u1_client.put("fop", "bar", None).await;
    assert!(result.is_err());

    let result = u2_client
        .get("foo", Some(GetOptions::new().with_range("fox")))
        .await;
    assert!(result.is_ok());
    let result = u2_client
        .get("foo", Some(GetOptions::new().with_range("foz")))
        .await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_role_delete() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    set_user(&mut auth_client, "u", "123", "r", b"foo", &[]).await?;
    let user = auth_client.user_get("u").await?;
    assert_eq!(user.roles().len(), 1);
    auth_client.role_delete("r").await?;
    let user = auth_client.user_get("u").await?;
    assert_eq!(user.roles().len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_no_root_user_do_admin_ops() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    set_user(&mut auth_client, "u", "123", "r", &[], &[]).await?;
    enable_auth(&mut auth_client).await?;
    let mut user_client = etcd_client::Client::connect(
        vec![cluster.addrs()["server0"].to_string()],
        Some(ConnectOptions::new().with_user("u", "123")),
    )
    .await?;
    let mut root_client = etcd_client::Client::connect(
        vec![cluster.addrs()["server0"].to_string()],
        Some(ConnectOptions::new().with_user("root", "123")),
    )
    .await?;

    let result = user_client.user_add("u2", "123", None).await;
    assert!(result.is_err());
    let result = root_client.user_add("u2", "123", None).await;
    assert!(result.is_ok());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_auth_wrong_password() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;
    let mut auth_client = client.auth_client();

    enable_auth(&mut auth_client).await?;

    let result = etcd_client::Client::connect(
        vec![cluster.addrs()["server0"].to_string()],
        Some(ConnectOptions::new().with_user("root", "456")),
    )
    .await;
    assert!(result.is_err());

    let result = etcd_client::Client::connect(
        vec![cluster.addrs()["server0"].to_string()],
        Some(ConnectOptions::new().with_user("root", "123")),
    )
    .await;
    assert!(result.is_ok());

    Ok(())
}

async fn set_user(
    client: &mut AuthClient,
    name: &str,
    password: &str,
    role: &str,
    key: &[u8],
    range_end: &[u8],
) -> Result<(), Box<dyn Error>> {
    client.user_add(name, password, None).await?;
    client.role_add(role).await?;
    client.user_grant_role(name, role).await?;
    if !key.is_empty() {
        client
            .role_grant_permission(
                role,
                Permission::new(PermissionType::Readwrite, key).with_range_end(range_end),
            )
            .await?;
    }
    Ok(())
}

async fn enable_auth(client: &mut AuthClient) -> Result<(), Box<dyn Error>> {
    set_user(client, "root", "123", "root", &[], &[]).await?;
    client.auth_enable().await?;
    Ok(())
}
