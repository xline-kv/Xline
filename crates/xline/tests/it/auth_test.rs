use std::{error::Error, iter, path::PathBuf};

use test_macros::abort_on_panic;
use utils::config::{
    AuthConfig, ClusterConfig, CompactConfig, LogConfig, MetricsConfig, StorageConfig, TlsConfig,
    TraceConfig, XlineServerConfig,
};
use xline_test_utils::{
    enable_auth, set_user, types::kv::RangeOptions, Client, ClientOptions, Cluster,
};

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_auth_empty_user_get() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;

    enable_auth(client).await?;
    let res = client.kv_client().range("foo", None).await;
    assert!(res.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_auth_empty_user_put() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;

    enable_auth(client).await?;
    let res = client.kv_client().put("foo", "bar", None).await;
    assert!(res.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_auth_token_with_disable() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;

    enable_auth(client).await?;
    let authed_client = Client::connect(
        vec![cluster.get_client_url(0)],
        ClientOptions::default().with_user("root", "123"),
    )
    .await?;
    let kv_client = authed_client.kv_client();
    kv_client.put("foo", "bar", None).await?;
    authed_client.auth_client().auth_disable().await?;
    kv_client.put("foo", "bar", None).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_auth_revision() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;
    let auth_client = client.auth_client();

    client.kv_client().put("foo", "bar", None).await?;

    let user_add_resp = auth_client.user_add("root", "123", false).await?;
    let auth_rev = user_add_resp.header.unwrap().revision;
    assert_eq!(auth_rev, 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_auth_non_authorized_rpcs() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;
    let kv_client = client.kv_client();

    let result = kv_client.put("foo", "bar", None).await;
    assert!(result.is_ok());
    enable_auth(client).await?;
    let result = kv_client.put("foo", "bar", None).await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_kv_authorization() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;

    set_user(client, "u1", "123", "r1", b"foo", &[]).await?;
    set_user(client, "u2", "123", "r2", b"foo", b"foy").await?;
    enable_auth(client).await?;

    let u1_client = Client::connect(
        vec![cluster.get_client_url(0)],
        ClientOptions::default().with_user("u1", "123"),
    )
    .await?
    .kv_client();
    let u2_client = Client::connect(
        vec![cluster.get_client_url(0)],
        ClientOptions::default().with_user("u2", "123"),
    )
    .await?
    .kv_client();

    let result = u1_client.put("foo", "bar", None).await;
    assert!(result.is_ok());
    let result = u1_client.put("fop", "bar", None).await;
    assert!(result.is_err());

    let result = u2_client
        .range("foo", Some(RangeOptions::default().with_range_end("fox")))
        .await;
    assert!(result.is_ok());
    let result = u2_client
        .range("foo", Some(RangeOptions::default().with_range_end("foz")))
        .await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_role_delete() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;
    let auth_client = client.auth_client();
    set_user(client, "u", "123", "r", b"foo", &[]).await?;
    let user = auth_client.user_get("u").await?;
    assert_eq!(user.roles.len(), 1);
    auth_client.role_delete("r").await?;
    let user = auth_client.user_get("u").await?;
    assert_eq!(user.roles.len(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_no_root_user_do_admin_ops() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;

    set_user(client, "u", "123", "r", &[], &[]).await?;
    enable_auth(client).await?;
    let user_client = Client::connect(
        vec![cluster.get_client_url(0)],
        ClientOptions::default().with_user("u", "123"),
    )
    .await?
    .auth_client();
    let root_client = Client::connect(
        vec![cluster.get_client_url(0)],
        ClientOptions::default().with_user("root", "123"),
    )
    .await?
    .auth_client();

    let result = user_client.user_add("u2", "123", false).await;
    assert!(
        result.is_err(),
        "normal user should not allow to add user when auth is enabled: {result:?}"
    );
    let result = root_client.user_add("u2", "123", false).await;
    assert!(result.is_ok(), "root user failed to add user: {result:?}");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[abort_on_panic]
async fn test_auth_wrong_password() -> Result<(), Box<dyn Error>> {
    let mut cluster = Cluster::new_with_configs(configs_with_auth(3)).await;
    cluster.start().await;
    let client = cluster.client().await;

    enable_auth(client).await?;

    let result = Client::connect(
        vec![cluster.get_client_url(0)],
        ClientOptions::default().with_user("root", "456"),
    )
    .await;
    assert!(result.is_err());

    let result = Client::connect(
        vec![cluster.get_client_url(0)],
        ClientOptions::default().with_user("root", "123"),
    )
    .await;
    assert!(result.is_ok());

    Ok(())
}

fn configs_with_auth(size: usize) -> Vec<XlineServerConfig> {
    iter::repeat_with(|| {
        (
            Some(PathBuf::from("../../fixtures/public.pem")),
            Some(PathBuf::from("../../fixtures/private.pem")),
        )
    })
    .map(|(auth_public_key, auth_private_key)| {
        XlineServerConfig::new(
            ClusterConfig::default(),
            StorageConfig::default(),
            LogConfig::default(),
            TraceConfig::default(),
            AuthConfig::new(auth_public_key, auth_private_key),
            CompactConfig::default(),
            TlsConfig::default(),
            MetricsConfig::default(),
        )
    })
    .take(size)
    .collect()
}
